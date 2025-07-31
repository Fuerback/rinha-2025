CREATE TABLE payments (
    correlation_id UUID PRIMARY KEY,
    amount INTEGER NOT NULL,
    payment_processor TEXT,
    requested_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_payments_requested_at ON payments (requested_at);