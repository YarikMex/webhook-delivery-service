# Webhook Delivery Service

I built this service to find out what happens to a webhook when the receiver goes down and the sender restarts before it can retry. The API saves the event to PostgreSQL, and a background worker delivers it to a configured receiver.

## Try it locally

You need Java 21, Maven 3.9+, Docker Compose, and PowerShell for the API examples. Run these commands from the repository root in separate terminals.

1. Start PostgreSQL and keep it running.

   ```sh
   docker compose up -d --wait
   ```

2. Start the demo receiver and leave it running. It returns HTTP 503 on the first request for each delivery ID and 200 on subsequent requests.

   ```sh
   java tools/DemoReceiver.java
   ```

3. Start the service with a 30-second retry delay so you have time to restart it before the next attempt.

   ```sh
   mvn spring-boot:run "-Dspring-boot.run.arguments=--webhook.retry-delay-ms=30000"
   ```

4. Submit an event. The response is `202 Accepted` and its `Location` header points to the delivery status.

   ```powershell
   $api = "http://127.0.0.1:8080"
   $event = @{ type = "order.shipped"; data = @{ orderId = "A-1042" } } | ConvertTo-Json
   $delivery = Invoke-RestMethod "$api/events" -Method Post -ContentType application/json -Body $event
   $url = "$api/deliveries/$($delivery.id)"
   Invoke-RestMethod $url | ConvertTo-Json -Depth 5
   ```

5. When the receiver logs `503`, check the delivery. Its status should be `PENDING` with the first attempt recorded. Stop only the Spring Boot service with Ctrl+C, then start it again with the same command. Once the scheduled retry runs, the status endpoint shows `DELIVERED` and the attempt history shows `503`, then `200`.

### Configuration

| Environment variable | Default |
| --- | --- |
| `WEBHOOK_RECEIVER_URL` | `http://127.0.0.1:8081/webhooks` |
| `WEBHOOK_RETRY_DELAY_MS` | `5000` |
| `WEBHOOK_DB_URL` | `jdbc:postgresql://127.0.0.1:5432/webhook` |
| `WEBHOOK_DB_USER` | `webhook` |
| `WEBHOOK_DB_PASSWORD` | `webhook-local-only` |

If port 5432 is in use, set `WEBHOOK_DB_PORT` for Compose and use that port in `WEBHOOK_DB_URL`. The receiver URL is set by the operator. The API listens on loopback for local development.

If port 8081 is in use, start the receiver with a different port, for example `java tools/DemoReceiver.java 18081`, and set `WEBHOOK_RECEIVER_URL` to `http://127.0.0.1:18081/webhooks` in the service terminal before starting it.

## How delivery works

- The API writes the payload to PostgreSQL before returning `202`. The worker sends the JSON data as the request body and includes the event type and stable delivery ID in headers.
- Workers claim due deliveries with `FOR UPDATE SKIP LOCKED` and a 30-second lease. The lease lets another worker recover unfinished work after a crash. A stale worker cannot overwrite a newer claim.
- Each recorded attempt and its resulting delivery status are saved in one transaction. Network failures and HTTP 408, 429, and 5xx can retry, with up to three recorded attempts. Other non-2xx responses move the delivery to `FAILED`.
- Retries can send the same delivery more than once. For example, the receiver can handle a request before the sender records its result. The stable `X-Webhook-Delivery-Id` gives receivers a key to deduplicate it.

## Tests

With PostgreSQL running, execute `mvn verify`. The integration tests use a real HTTP receiver and PostgreSQL. They exercise retry after restart, permanent failure, and recovery of an expired lease. GitHub Actions runs the same suite.

## Next steps

Exponential backoff with jitter, HMAC signatures, and a way to inspect and replay failed deliveries.
