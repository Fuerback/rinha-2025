package storage

import (
	"database/sql"
	"errors"
	"time"

	"github.com/Fuerback/rinha-2025/internal/domain"
	"github.com/lib/pq"
	"github.com/shopspring/decimal"
)

var ErrUniqueViolation = errors.New("unique violation")

type PaymentStore struct {
	db *sql.DB
}

func NewPaymentStore(db *sql.DB) *PaymentStore {
	return &PaymentStore{
		db: db,
	}
}

func (s *PaymentStore) CreatePayment(payment *domain.Payment) error {
	_, err := s.db.Exec("INSERT INTO payments (correlation_id, amount, status, created_at, updated_at) VALUES ($1, $2, $3, $4, $5)", payment.CorrelationID, decimalToInt64(payment.Amount), payment.Status, time.Now(), time.Now())
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

func (s *PaymentStore) FindPendingPayments() ([]domain.Payment, error) {
	rows, err := s.db.Query("SELECT * FROM payments WHERE status = $1", domain.PaymentStatusPending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var payments []domain.Payment
	for rows.Next() {
		payment := domain.Payment{}
		if err := rows.Scan(&payment.ID, &payment.CorrelationID, &payment.Amount, &payment.PaymentProcessor, &payment.Status, &payment.CreatedAt, &payment.UpdatedAt); err != nil {
			return nil, err
		}
		payment.Amount = int64ToDecimal(payment.Amount.IntPart())
		payments = append(payments, payment)
	}
	return payments, nil
}

func (s *PaymentStore) UpdatePaymentStatus(correlationID string, status domain.PaymentStatus, paymentProcessor domain.PaymentProcessor) error {
	_, err := s.db.Exec("UPDATE payments SET status = $1, payment_processor = $2, updated_at = $3 WHERE correlation_id = $4", status, paymentProcessor, time.Now(), correlationID)
	return err
}

func (s *PaymentStore) GetPaymentSummary(from time.Time, to time.Time) (domain.PaymentSummary, error) {
	rows, err := s.db.Query("SELECT * FROM payments WHERE created_at >= $1 AND created_at <= $2 AND status = $3", from, to, domain.PaymentStatusSuccess)
	if err != nil {
		return domain.PaymentSummary{}, err
	}
	defer rows.Close()

	var payments []domain.Payment
	for rows.Next() {
		payment := domain.Payment{}
		if err := rows.Scan(&payment.ID, &payment.CorrelationID, &payment.Amount, &payment.PaymentProcessor, &payment.Status, &payment.CreatedAt, &payment.UpdatedAt); err != nil {
			return domain.PaymentSummary{}, err
		}
		payment.Amount = int64ToDecimal(payment.Amount.IntPart())
		payments = append(payments, payment)
	}

	var summary domain.PaymentSummary
	for _, payment := range payments {
		if payment.PaymentProcessor == domain.PaymentProcessorDefault {
			summary.Default.TotalRequests++
			summary.Default.TotalAmount = summary.Default.TotalAmount.Add(payment.Amount)
		} else {
			summary.Fallback.TotalRequests++
			summary.Fallback.TotalAmount = summary.Fallback.TotalAmount.Add(payment.Amount)
		}
	}
	return summary, nil
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
