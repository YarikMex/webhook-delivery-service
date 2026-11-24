package dev.webhookdelivery;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class DeliveryIntegrationTest {
    private static final String EVENT = """
            {"type":"order.shipped","data":{"orderId":"A-1042"}}
            """;
    private final HttpClient client = HttpClient.newHttpClient();
    private final ObjectMapper json = new ObjectMapper();
    private final CopyOnWriteArrayList<ObservedRequest> received = new CopyOnWriteArrayList<>();
    private final AtomicInteger firstStatus = new AtomicInteger(503);
    private final AtomicInteger laterStatus = new AtomicInteger(200);
    private final String schema = "webhook_test_" + UUID.randomUUID().toString().replace("-", "");
    private JdbcConnectionDetails database;
    private HttpServer receiver;

    @BeforeEach
    void startReceiver() throws IOException {
        receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        receiver.createContext("/webhooks", this::receive);
        receiver.start();
    }

    @AfterEach
    void cleanUp() throws SQLException {
        if (receiver != null) {
            receiver.stop(0);
        }
        client.shutdownNow();
        if (database != null) {
            try (var connection = DriverManager.getConnection(database.getJdbcUrl(), database.getUsername(), database.getPassword());
                 var statement = connection.createStatement()) {
                statement.executeUpdate("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
            }
        }
    }

    @Test
    void resumesScheduledRetryAfterRestart() throws Exception {
        String location;
        try (var sender = startSender(3600000, 1500)) {
            location = submitEvent(sender);
            sender.getBean(DeliveryWorker.class).deliverNext();

            JsonNode pending = getDelivery(sender, location);
            assertThat(pending.path("status").asText()).isEqualTo("PENDING");
            assertThat(pending.path("nextAttemptAt").isTextual()).isTrue();
            assertThat(pending.path("attempts")).hasSize(1);
            assertThat(pending.at("/attempts/0/httpStatus").asInt()).isEqualTo(503);
        }

        try (var restarted = startSender(100, 1500)) {
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                JsonNode delivered = getDelivery(restarted, location);
                assertThat(delivered.path("status").asText()).isEqualTo("DELIVERED");
                assertThat(delivered.path("attempts")).hasSize(2);
                assertThat(delivered.at("/attempts/0/httpStatus").asInt()).isEqualTo(503);
                assertThat(delivered.at("/attempts/1/httpStatus").asInt()).isEqualTo(200);
            });

            String id = location.substring("/deliveries/".length());
            assertThat(received).containsExactly(
                    new ObservedRequest(id, "order.shipped", "{\"orderId\":\"A-1042\"}"),
                    new ObservedRequest(id, "order.shipped", "{\"orderId\":\"A-1042\"}"));
        }
    }

    @Test
    void recordsPermanentFailureAndDoesNotRetryIt() throws Exception {
        firstStatus.set(400);
        try (var sender = startSender(3600000, 100)) {
            String location = submitEvent(sender);

            var worker = sender.getBean(DeliveryWorker.class);
            worker.deliverNext();
            worker.deliverNext();

            JsonNode failed = getDelivery(sender, location);
            assertThat(failed.path("status").asText()).isEqualTo("FAILED");
            assertThat(failed.path("nextAttemptAt").isNull()).isTrue();
            assertThat(failed.path("attempts")).hasSize(1);
            assertThat(failed.at("/attempts/0/httpStatus").asInt()).isEqualTo(400);
            assertThat(received).hasSize(1);
        }
    }

    @Test
    void expiresLeaseAndRejectsStaleWorkerResult() throws Exception {
        try (var sender = startSender(3600000, 100)) {
            String location = submitEvent(sender);

            var queue = sender.getBean(DeliveryQueue.class);
            var expiredClaim = queue.claim().orElseThrow();
            assertThat(queue.claim()).isEmpty();
            sender.getBean(JdbcClient.class)
                    .sql("UPDATE delivery SET lease_until = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id = :id")
                    .param("id", expiredClaim.id())
                    .update();

            var recoveredClaim = queue.claim().orElseThrow();
            assertThat(recoveredClaim.id()).isEqualTo(expiredClaim.id());
            assertThat(recoveredClaim.leaseToken()).isNotEqualTo(expiredClaim.leaseToken());
            queue.finish(expiredClaim, DeliveryQueue.Status.FAILED, DeliveryQueue.Outcome.HTTP_ERROR, 400, 100);
            assertThat(getDelivery(sender, location).path("attempts")).isEmpty();
            queue.finish(recoveredClaim, DeliveryQueue.Status.DELIVERED, DeliveryQueue.Outcome.DELIVERED, 200, 100);
            assertThat(getDelivery(sender, location).at("/attempts/0/httpStatus").asInt()).isEqualTo(200);
        }
    }

    @Test
    void stopsAfterThreeRetryableFailures() throws Exception {
        laterStatus.set(503);
        try (var sender = startSender(100, 100)) {
            String location = submitEvent(sender);

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                JsonNode failed = getDelivery(sender, location);
                assertThat(failed.path("status").asText()).isEqualTo("FAILED");
                assertThat(failed.path("attempts")).hasSize(3);
                assertThat(failed.path("nextAttemptAt").isNull()).isTrue();
                assertThat(received).hasSize(3);
            });
        }
    }

    @Test
    void rejectsInvalidEventAndReportsUnknownDelivery() throws Exception {
        try (var sender = startSender(3600000, 100)) {
            assertThat(sendEvent(sender, "{\"type\":\"invalid event\",\"data\":{}}").statusCode()).isEqualTo(400);
            assertThat(sendEvent(sender, "{\"type\":\"order.shipped\"}").statusCode()).isEqualTo(400);
            assertThat(get(sender, "/deliveries/" + UUID.randomUUID()).statusCode()).isEqualTo(404);
            assertThat(sender.getBean(DeliveryQueue.class).claim()).isEmpty();
        }
    }

    private ServletWebServerApplicationContext startSender(long pollMs, long retryMs) {
        var sender = (ServletWebServerApplicationContext) new SpringApplicationBuilder(WebhookDeliveryApplication.class)
                .run("--server.port=0", "--spring.main.banner-mode=off",
                        "--webhook.receiver-url=http://127.0.0.1:" + receiver.getAddress().getPort() + "/webhooks",
                        "--webhook.poll-interval-ms=" + pollMs, "--webhook.retry-delay-ms=" + retryMs,
                        "--spring.flyway.default-schema=" + schema, "--spring.datasource.hikari.schema=" + schema);
        database = sender.getBean(JdbcConnectionDetails.class);
        return sender;
    }

    private String submitEvent(ServletWebServerApplicationContext sender) throws Exception {
        var response = sendEvent(sender, EVENT);
        assertThat(response.statusCode()).isEqualTo(202);
        String id = json.readTree(response.body()).path("id").asText();
        String location = response.headers().firstValue("Location").orElseThrow();
        assertThat(location).isEqualTo("/deliveries/" + id);
        return location;
    }

    private HttpResponse<String> sendEvent(ServletWebServerApplicationContext sender, String data) throws Exception {
        return client.send(request(sender, "/events").header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(data)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(ServletWebServerApplicationContext sender, String path) throws Exception {
        return client.send(request(sender, path).build(), HttpResponse.BodyHandlers.ofString());
    }

    private JsonNode getDelivery(ServletWebServerApplicationContext sender, String path) throws Exception {
        var response = get(sender, path);
        assertThat(response.statusCode()).isEqualTo(200);
        return json.readTree(response.body());
    }

    private HttpRequest.Builder request(ServletWebServerApplicationContext sender, String path) {
        return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + sender.getWebServer().getPort() + path))
                .timeout(Duration.ofSeconds(4));
    }

    private void receive(HttpExchange exchange) throws IOException {
        try (exchange) {
            var headers = exchange.getRequestHeaders();
            String body = json.readTree(exchange.getRequestBody()).toString();
            var request = new ObservedRequest(headers.getFirst("X-Webhook-Delivery-Id"),
                    headers.getFirst("X-Webhook-Event"), body);
            received.add(request);
            exchange.sendResponseHeaders(received.size() == 1 ? firstStatus.get() : laterStatus.get(), -1);
        }
    }

    private record ObservedRequest(String id, String eventType, String payload) {}
}
