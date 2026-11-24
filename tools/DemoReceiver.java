import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.sun.net.httpserver.HttpServer;

class DemoReceiver {
    public static void main(String[] args) throws Exception {
        int port = args.length == 0 ? 8081 : Integer.parseInt(args[0]);
        Set<String> received = ConcurrentHashMap.newKeySet();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.createContext("/webhooks", exchange -> {
            try (exchange) {
                String id = exchange.getRequestHeaders().getFirst("X-Webhook-Delivery-Id");
                if (!"POST".equals(exchange.getRequestMethod()) || id == null) {
                    exchange.sendResponseHeaders(400, -1);
                    return;
                }

                exchange.getRequestBody().readAllBytes();
                int code = received.add(id) ? 503 : 200;
                System.out.printf("Delivery %s returned HTTP %d%n", id, code);
                exchange.sendResponseHeaders(code, -1);
            }
        });
        server.start();
        System.out.printf("Demo receiver listening on http://127.0.0.1:%d/webhooks%n", port);
    }
}
