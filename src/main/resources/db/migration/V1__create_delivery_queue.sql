CREATE TABLE delivery (
    id uuid PRIMARY KEY,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('PENDING', 'DELIVERED', 'FAILED')),
    next_attempt_at timestamptz,
    lease_token uuid,
    lease_until timestamptz,
    CHECK ((status = 'PENDING') = (next_attempt_at IS NOT NULL)),
    CHECK ((lease_token IS NULL) = (lease_until IS NULL)),
    CHECK (status = 'PENDING' OR lease_token IS NULL)
);

CREATE INDEX delivery_pending_idx ON delivery (next_attempt_at, id)
    WHERE status = 'PENDING';

CREATE TABLE delivery_attempt (
    delivery_id uuid NOT NULL REFERENCES delivery (id),
    attempt_number integer NOT NULL CHECK (attempt_number BETWEEN 1 AND 3),
    finished_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    http_status integer CHECK (http_status BETWEEN 100 AND 599),
    outcome text NOT NULL CHECK (outcome IN ('DELIVERED', 'HTTP_ERROR', 'NETWORK_ERROR')),
    PRIMARY KEY (delivery_id, attempt_number)
);
