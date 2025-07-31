package domain

import (
	"time"

	"github.com/shopspring/decimal"
)

type Payment struct {
	ID               string
	CorrelationID    string
	Amount           decimal.Decimal
	PaymentProcessor PaymentProcessor
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type PaymentEvent struct {
	CorrelationID string
	Amount        decimal.Decimal
}

type PaymentSummary struct {
	Default  PaymentSummaryDetail
	Fallback PaymentSummaryDetail
}

type PaymentSummaryDetail struct {
	TotalRequests int
	TotalAmount   decimal.Decimal
}

type PaymentProcessor string

const (
	PaymentProcessorDefault  PaymentProcessor = "default"
	PaymentProcessorFallback PaymentProcessor = "fallback"
)

func NewPayment(correlationID string, amount decimal.Decimal, paymentProcessor PaymentProcessor) *Payment {
	return &Payment{
		CorrelationID:    correlationID,
		Amount:           amount,
		PaymentProcessor: paymentProcessor,
	}
}
