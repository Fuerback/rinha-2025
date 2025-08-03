CREATE TABLE payments (
    correlation_id UUID PRIMARY KEY,
    amount INTEGER NOT NULL,
    payment_processor SMALLINT NOT NULL,
    requested_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_payments_requested_at ON payments (requested_at);
CREATE INDEX idx_payments_requested_at_processor ON payments (requested_at, payment_processor);