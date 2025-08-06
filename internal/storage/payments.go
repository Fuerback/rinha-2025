package storage

import (
	"time"

	"github.com/Fuerback/rinha-2025/internal/model"
	"github.com/shopspring/decimal"
)

type PaymentStore interface {
	CreatePayment(payment *model.Payment) error
	GetPaymentSummary(from time.Time, to time.Time) (model.PaymentSummary, error)
	GetHealthCheck() (model.HealthCheck, error)
	UpdateHealthCheck(preferredProcessor, minResponseTime int) error
}

func decimalToInt64(d decimal.Decimal) int64 {
	scale := decimal.NewFromInt(10).Pow(decimal.NewFromInt(int64(2)))
	scaled := d.Mul(scale)
	return scaled.IntPart()
}

func int64ToDecimal(i int64) decimal.Decimal {
	scale := decimal.NewFromInt(10).Pow(decimal.NewFromInt(int64(2)))
	return decimal.NewFromInt(i).Div(scale)
}
