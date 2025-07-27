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
	Status           PaymentStatus
	CreatedAt        time.Time
	UpdatedAt        time.Time
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

type PaymentStatus string

const (
	PaymentStatusPending PaymentStatus = "pending"
	PaymentStatusSuccess PaymentStatus = "success"
	PaymentStatusFailed  PaymentStatus = "failed"
)

func NewPayment(correlationID string, amount decimal.Decimal) *Payment {
	return &Payment{
		CorrelationID: correlationID,
		Amount:        amount,
		Status:        PaymentStatusPending,
	}
}

func (p *Payment) SetStatus(status PaymentStatus) {
	p.Status = status
}

func (p *Payment) SetPaymentProcessor(processor PaymentProcessor) {
	p.PaymentProcessor = processor
}
