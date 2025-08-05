package handler

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/Fuerback/rinha-2025/internal/model"
	"github.com/Fuerback/rinha-2025/internal/storage"
	"github.com/Fuerback/rinha-2025/internal/worker"
	"github.com/gofiber/fiber/v3"
	"github.com/shopspring/decimal"
)

type PaymentRequest struct {
	CorrelationID string          `json:"correlationId" validate:"required"`
	Amount        decimal.Decimal `json:"amount" validate:"required,gt=0"`
}

type PaymentResponse struct {
	Default  PaymentSummaryResponse `json:"default"`
	Fallback PaymentSummaryResponse `json:"fallback"`
}

type PaymentSummaryResponse struct {
	TotalRequests int             `json:"totalRequests"`
	TotalAmount   decimal.Decimal `json:"totalAmount"`
}

func CreatePaymentHandler(paymentWorker *worker.PaymentProcessorWorker) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req PaymentRequest
		if err := c.Bind().Body(&req); err != nil {
			log.Printf("failed to bind request: %s", err)
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{
				"error": "failed to bind request",
			})
		}

		if req.CorrelationID == "" {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{
				"error": "correlationId is required",
			})
		}

		if req.Amount.IsZero() || req.Amount.IsNegative() {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{
				"error": "amount must be greater than zero",
			})
		}

		go func() {
			paymentWorker.AddMessage(&model.PaymentEvent{
				CorrelationID: req.CorrelationID,
				Amount:        req.Amount,
				RequestedAt:   time.Now(),
			})
		}()

		return c.Status(http.StatusCreated).JSON(fiber.Map{
			"message": "Payment created",
		})
	}
}

func PaymentSummaryHandler(store storage.PaymentStore) fiber.Handler {
	return func(c fiber.Ctx) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		fromStr := c.Query("from")
		toStr := c.Query("to")

		if fromStr == "" || toStr == "" {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{
				"error": "from and to query parameters are required",
			})
		}

		from, err := time.Parse(time.RFC3339, fromStr)
		if err != nil {
			log.Printf("failed to parse start date: %s", err)
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{
				"error": "failed to parse start date, use RFC3339 format",
			})
		}

		to, err := time.Parse(time.RFC3339, toStr)
		if err != nil {
			log.Printf("failed to parse end date: %s", err)
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{
				"error": "failed to parse end date, use RFC3339 format",
			})
		}

		if to.Before(from) {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{
				"error": "to date must be after from date",
			})
		}

		summary, err := store.GetPaymentSummary(from, to)
		if err != nil {
			select {
			case <-ctx.Done():
				return c.Status(http.StatusRequestTimeout).JSON(fiber.Map{
					"error": "request timeout",
				})
			default:
				log.Printf("failed to get payment summary: %s", err)
				return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
					"error": "failed to get payment summary",
				})
			}
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

		return c.Status(http.StatusOK).JSON(response)
	}
}
