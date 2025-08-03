package domain

import (
	"time"

	"github.com/shopspring/decimal"
)

type Payment struct {
	CorrelationID    string
	Amount           decimal.Decimal
	PaymentProcessor PaymentProcessor
	RequestedAt      time.Time
}

type PaymentEvent struct {
	CorrelationID string
	Amount        decimal.Decimal
	RequestedAt   time.Time
	RetryCount    uint
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

var PaymentProcessorMap = map[PaymentProcessor]int{
	PaymentProcessorDefault:  0,
	PaymentProcessorFallback: 1,
}

var PaymentProcessorTranslationMap = map[int]PaymentProcessor{
	0: PaymentProcessorDefault,
	1: PaymentProcessorFallback,
}

func NewPayment(correlationID string, amount decimal.Decimal, requestedAt time.Time, paymentProcessor PaymentProcessor) *Payment {
	return &Payment{
		CorrelationID:    correlationID,
		Amount:           amount,
		PaymentProcessor: paymentProcessor,
		RequestedAt:      requestedAt,
	}
}
