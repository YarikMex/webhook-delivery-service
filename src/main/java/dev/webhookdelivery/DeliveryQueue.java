package dev.webhookdelivery;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

@Repository
class DeliveryQueue {
    private final JdbcClient jdbc;

    DeliveryQueue(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    UUID enqueue(String type, String payload) {
        UUID id = UUID.randomUUID();
        jdbc.sql("""
                INSERT INTO delivery (id, event_type, payload, status, next_attempt_at)
                VALUES (:id, :type, CAST(:payload AS jsonb), 'PENDING', CURRENT_TIMESTAMP)
                """)
                .param("id", id)
                .param("type", type)
                .param("payload", payload)
                .update();
        return id;
    }

    Optional<Claim> claim() {
        return jdbc.sql("""
                WITH candidate AS (
                    SELECT id FROM delivery
                    WHERE status = 'PENDING' AND next_attempt_at <= CURRENT_TIMESTAMP
                        AND (lease_until IS NULL OR lease_until <= CURRENT_TIMESTAMP)
                    ORDER BY next_attempt_at, id
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE delivery AS d
                SET lease_token = :token, lease_until = CURRENT_TIMESTAMP + INTERVAL '30 seconds'
                FROM candidate
                WHERE d.id = candidate.id
                RETURNING d.id, d.event_type, d.payload::text AS payload, d.lease_token,
                    (SELECT COUNT(*)::integer + 1 FROM delivery_attempt AS a
                        WHERE a.delivery_id = d.id) AS attempt_number
                """)
                .param("token", UUID.randomUUID())
                .query(Claim.class)
                .optional();
    }

    @Transactional
    public void finish(Claim claim, Status status, Outcome outcome, Integer httpStatus, long retryDelayMs) {
        int updated = jdbc.sql("""
                UPDATE delivery SET status = :status, lease_token = NULL, lease_until = NULL,
                    next_attempt_at = CASE WHEN :status = 'PENDING'
                        THEN CURRENT_TIMESTAMP + :delayMs * INTERVAL '1 millisecond' END
                WHERE id = :id AND lease_token = :token
                """)
                .param("status", status.name())
                .param("delayMs", retryDelayMs)
                .param("id", claim.id())
                .param("token", claim.leaseToken())
                .update();

        if (updated == 1) {
            jdbc.sql("""
                    INSERT INTO delivery_attempt (delivery_id, attempt_number, http_status, outcome)
                    VALUES (:id, :number, :httpStatus, :outcome)
                    """)
                    .param("id", claim.id())
                    .param("number", claim.attemptNumber())
                    .param("httpStatus", httpStatus, java.sql.Types.INTEGER)
                    .param("outcome", outcome.name())
                    .update();
        }
    }

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public Optional<Delivery> find(UUID id) {
        return jdbc.sql("""
                SELECT id, event_type, status, next_attempt_at FROM delivery WHERE id = :id
                """)
                .param("id", id)
                .query(DeliveryState.class)
                .optional()
                .map(state -> new Delivery(state.id(), state.eventType(), state.status(), state.nextAttemptAt(),
                        jdbc.sql("""
                                SELECT attempt_number, finished_at, http_status, outcome
                                FROM delivery_attempt WHERE delivery_id = :id ORDER BY attempt_number
                                """)
                                .param("id", id)
                                .query(Attempt.class)
                                .list()));
    }

    enum Status { PENDING, DELIVERED, FAILED }
    enum Outcome { DELIVERED, HTTP_ERROR, NETWORK_ERROR }

    record Claim(UUID id, String eventType, String payload, UUID leaseToken, int attemptNumber) {}
    record DeliveryState(UUID id, String eventType, String status, OffsetDateTime nextAttemptAt) {}
    record Attempt(int attemptNumber, OffsetDateTime finishedAt, Integer httpStatus, String outcome) {}
    record Delivery(UUID id, String eventType, String status, OffsetDateTime nextAttemptAt, List<Attempt> attempts) {}
}
