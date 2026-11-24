package dev.webhookdelivery;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import static dev.webhookdelivery.DeliveryQueue.Outcome;
import static dev.webhookdelivery.DeliveryQueue.Status;

@Component
class DeliveryWorker {
    private static final int MAX_ATTEMPTS = 3;
    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
    private final DeliveryQueue queue;
    private final URI receiver;
    private final long retryDelayMs;

    DeliveryWorker(DeliveryQueue queue, @Value("${webhook.receiver-url}") URI receiver,
                   @Value("${webhook.retry-delay-ms}") long retryDelayMs) {
        if (!("http".equalsIgnoreCase(receiver.getScheme()) || "https".equalsIgnoreCase(receiver.getScheme()))
                || receiver.getHost() == null || receiver.getUserInfo() != null || receiver.getFragment() != null) {
            throw new IllegalArgumentException("The receiver must be an HTTP or HTTPS URL without credentials or a fragment.");
        }
        if (retryDelayMs <= 0) {
            throw new IllegalArgumentException("The retry delay must be positive.");
        }
        this.queue = queue;
        this.receiver = receiver;
        this.retryDelayMs = retryDelayMs;
    }

    @Scheduled(initialDelayString = "${webhook.poll-interval-ms:250}",
            fixedDelayString = "${webhook.poll-interval-ms:250}")
    void deliverNext() {
        queue.claim().ifPresent(this::deliver);
    }

    @PreDestroy
    void stop() {
        client.shutdownNow();
    }

    private void deliver(DeliveryQueue.Claim claim) {
        HttpRequest request = HttpRequest.newBuilder(receiver)
                .timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/json")
                .header("X-Webhook-Delivery-Id", claim.id().toString())
                .header("X-Webhook-Event", claim.eventType())
                .POST(HttpRequest.BodyPublishers.ofString(claim.payload()))
                .build();
        try {
            var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
            response.body().close();
            int code = response.statusCode();
            if (code >= 200 && code < 300) {
                queue.finish(claim, Status.DELIVERED, Outcome.DELIVERED, code, retryDelayMs);
            } else {
                boolean retryable = code == 408 || code == 429 || (code >= 500 && code <= 599);
                queue.finish(claim, failureStatus(claim, retryable), Outcome.HTTP_ERROR, code, retryDelayMs);
            }
        } catch (IOException exception) {
            queue.finish(claim, failureStatus(claim, true), Outcome.NETWORK_ERROR, null, retryDelayMs);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private Status failureStatus(DeliveryQueue.Claim claim, boolean retryable) {
        return retryable && claim.attemptNumber() < MAX_ATTEMPTS ? Status.PENDING : Status.FAILED;
    }
}
