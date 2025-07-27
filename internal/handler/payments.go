package handler

import (
	"fmt"
	"net/http"
	"time"

	"github.com/Fuerback/rinha-2025/internal/domain"
	"github.com/Fuerback/rinha-2025/internal/event"
	"github.com/Fuerback/rinha-2025/internal/storage"
	"github.com/labstack/echo/v4"
	"github.com/shopspring/decimal"
)

type PaymentRequest struct {
	CorrelationID string          `json:"correlation_id"`
	Amount        decimal.Decimal `json:"amount"`
}

type PaymentResponse struct {
	Default  PaymentSummaryResponse `json:"default"`
	Fallback PaymentSummaryResponse `json:"fallback"`
}

type PaymentSummaryResponse struct {
	TotalRequests int             `json:"total_requests"`
	TotalAmount   decimal.Decimal `json:"total_amount"`
}

func CreatePaymentHandler(store *storage.PaymentStore) echo.HandlerFunc {
	return func(c echo.Context) error {
		var req PaymentRequest
		if err := c.Bind(&req); err != nil {
			c.Logger().Error("failed to bind request", "error", err)
			return c.JSON(http.StatusBadRequest, fmt.Errorf("invalid request: %w", err))
		}

		payment := domain.NewPayment(req.CorrelationID, req.Amount)

		err := store.CreatePayment(payment)
		if err != nil {
			if err == storage.ErrUniqueViolation {
				return c.JSON(http.StatusAccepted, "Payment already exists")
			}
			c.Logger().Error("failed to create payment", "error", err)
			return c.JSON(http.StatusInternalServerError, fmt.Errorf("failed to create payment: %w", err))
		}

		event.RabbitMQClient.SendPaymentEvent(domain.PaymentEvent{
			CorrelationID: payment.CorrelationID,
			Amount:        payment.Amount,
		})

		return c.JSON(http.StatusCreated, "Payment created")
	}
}

func PaymentSummaryHandler(store *storage.PaymentStore) echo.HandlerFunc {
	return func(c echo.Context) error {
		fromStr := c.QueryParam("from")
		toStr := c.QueryParam("to")

		from, err := time.Parse(time.RFC3339, fromStr)
		if err != nil {
			c.Logger().Error("failed to parse start date", "error", err)
			return c.JSON(http.StatusBadRequest, fmt.Errorf("invalid value for start date: %w", err))
		}

		to, err := time.Parse(time.RFC3339, toStr)
		if err != nil {
			c.Logger().Error("failed to parse end date", "error", err)
			return c.JSON(http.StatusBadRequest, fmt.Errorf("invalid value for end date: %w", err))
		}

		summary, err := store.GetPaymentSummary(from, to)
		if err != nil {
			c.Logger().Error("failed to get payment summary", "error", err)
			return c.JSON(http.StatusInternalServerError, fmt.Errorf("failed to get payment summary: %w", err))
		}

		response := PaymentResponse{
			Default: PaymentSummaryResponse{
				TotalRequests: summary.Default.TotalRequests,
				TotalAmount:   summary.Default.TotalAmount,
			},
			Fallback: PaymentSummaryResponse{
				TotalRequests: summary.Fallback.TotalRequests,
				TotalAmount:   summary.Fallback.TotalAmount,
			},
		}

		return c.JSON(http.StatusOK, response)
	}
}
