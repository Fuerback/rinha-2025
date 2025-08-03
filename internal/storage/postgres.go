package storage

import (
	"database/sql"
	"errors"
	"time"

	"github.com/Fuerback/rinha-2025/internal/model"
	"github.com/lib/pq"
)

var ErrUniqueViolation = errors.New("unique violation")

type PaymentPostgresStorage struct {
	db *sql.DB
}

func NewPaymentPostgresStorage(db *sql.DB) (*PaymentPostgresStorage, error) {
	return &PaymentPostgresStorage{
		db: db,
	}, nil
}

func (s *PaymentPostgresStorage) CreatePayment(payment *model.Payment) error {
	_, err := s.db.Exec("INSERT INTO payments (correlation_id, amount, payment_processor, requested_at) VALUES ($1, $2, $3, $4)", payment.CorrelationID, decimalToInt64(payment.Amount), model.PaymentProcessorMap[payment.PaymentProcessor], payment.RequestedAt)
	if err != nil {
		if pgErr, ok := err.(*pq.Error); ok {
			if pgErr.Code.Name() == "unique_violation" {
				return ErrUniqueViolation
			}
		}
		return err
	}
	return nil
}

type paymentSummaryModel struct {
	PaymentProcessor int
	TotalAmount      int64
	PaymentCount     int
}

func (s *PaymentPostgresStorage) GetPaymentSummary(from time.Time, to time.Time) (model.PaymentSummary, error) {
	rows, err := s.db.Query(`SELECT 
    payment_processor,
    SUM(amount) as total_amount,
    COUNT(*) as payment_count
FROM payments 
WHERE requested_at BETWEEN $1 AND $2
GROUP BY payment_processor
ORDER BY payment_processor;`, from, to)
	if err != nil {
		return model.PaymentSummary{}, err
	}
	defer rows.Close()

	var payments []paymentSummaryModel
	for rows.Next() {
		payment := paymentSummaryModel{}
		if err := rows.Scan(&payment.PaymentProcessor, &payment.TotalAmount, &payment.PaymentCount); err != nil {
			return model.PaymentSummary{}, err
		}
		payments = append(payments, payment)
	}

	var summary model.PaymentSummary
	for _, payment := range payments {
		if payment.PaymentProcessor == 0 {
			summary.Default.TotalAmount = int64ToDecimal(payment.TotalAmount)
			summary.Default.TotalRequests = payment.PaymentCount
		} else {
			summary.Fallback.TotalAmount = int64ToDecimal(payment.TotalAmount)
			summary.Fallback.TotalRequests = payment.PaymentCount
		}
	}

	return summary, nil
}
