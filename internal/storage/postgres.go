package storage

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/Fuerback/rinha-2025/internal/model"
	"github.com/lib/pq"
)

var ErrUniqueViolation = errors.New("unique violation")

type PaymentPostgresStorage struct {
	db                    *sql.DB
	insertPaymentStmt     *sql.Stmt
	getPaymentSummaryStmt *sql.Stmt
	getHealthCheckStmt    *sql.Stmt
	updateHealthCheckStmt *sql.Stmt
}

func NewPaymentPostgresStorage(db *sql.DB) (*PaymentPostgresStorage, error) {
	storage := &PaymentPostgresStorage{db: db}

	// Prepare statements for better performance
	var err error
	storage.insertPaymentStmt, err = db.Prepare("INSERT INTO payments (correlation_id, amount, payment_processor, requested_at) VALUES ($1, $2, $3, $4)")
	if err != nil {
		return nil, err
	}

	storage.getPaymentSummaryStmt, err = db.Prepare(`SELECT 
    payment_processor,
    COALESCE(SUM(amount), 0) as total_amount,
    COUNT(*) as payment_count
FROM payments 
WHERE requested_at BETWEEN $1 AND $2
GROUP BY payment_processor
ORDER BY payment_processor`)
	if err != nil {
		return nil, err
	}

	storage.getHealthCheckStmt, err = db.Prepare("SELECT * FROM health_check limit 1")
	if err != nil {
		return nil, err
	}

	storage.updateHealthCheckStmt, err = db.Prepare("UPDATE health_check SET preferred_processor = $1, last_checked_at = $2")
	if err != nil {
		return nil, err
	}

	return storage, nil
}

func (s *PaymentPostgresStorage) CreatePayment(payment *model.Payment) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := s.insertPaymentStmt.ExecContext(ctx,
		payment.CorrelationID,
		decimalToInt64(payment.Amount),
		model.PaymentProcessorMap[payment.PaymentProcessor],
		payment.RequestedAt,
	)
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := s.getPaymentSummaryStmt.QueryContext(ctx, from, to)
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

	if err := rows.Err(); err != nil {
		return model.PaymentSummary{}, err
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

func (s *PaymentPostgresStorage) GetHealthCheck() (model.HealthCheck, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var healthCheck model.HealthCheck
	err := s.getHealthCheckStmt.QueryRowContext(ctx).Scan(&healthCheck.PreferredProcessor, &healthCheck.LastCheckedAt)
	if err != nil {
		return model.HealthCheck{}, err
	}

	return healthCheck, nil
}

func (s *PaymentPostgresStorage) UpdateHealthCheck(preferredProcessor int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := s.updateHealthCheckStmt.ExecContext(ctx, preferredProcessor, time.Now())
	if err != nil {
		return err
	}
	return nil
}

func (s *PaymentPostgresStorage) Close() error {
	if s.insertPaymentStmt != nil {
		s.insertPaymentStmt.Close()
	}
	if s.getPaymentSummaryStmt != nil {
		s.getPaymentSummaryStmt.Close()
	}
	return s.db.Close()
}
