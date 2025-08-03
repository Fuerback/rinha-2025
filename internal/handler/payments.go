package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/Fuerback/rinha-2025/internal/domain"
	"github.com/Fuerback/rinha-2025/internal/storage"
	"github.com/gofiber/fiber/v3"
	"github.com/nats-io/nats.go"
	"github.com/shopspring/decimal"
)

type PaymentRequest struct {
	CorrelationID string          `json:"correlationId"`
	Amount        decimal.Decimal `json:"amount"`
}

type PaymentResponse struct {
	Default  PaymentSummaryResponse `json:"default"`
	Fallback PaymentSummaryResponse `json:"fallback"`
}

type PaymentSummaryResponse struct {
	TotalRequests int             `json:"totalRequests"`
	TotalAmount   decimal.Decimal `json:"totalAmount"`
}

func CreatePaymentHandler(store *storage.PaymentStore, nc *nats.Conn) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req PaymentRequest
		if err := c.Bind().Body(&req); err != nil {
			log.Printf("failed to bind request: %s", err)
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{
				"error": "failed to bind request",
			})
		}

		paymentEvent := domain.PaymentEvent{
			CorrelationID: req.CorrelationID,
			Amount:        req.Amount,
			RequestedAt:   time.Now(),
		}

		// convert paymentEvent to json
		jsonPaymentEvent, err := json.Marshal(paymentEvent)
		if err != nil {
			log.Printf("failed to marshal payment event: %s", err)
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
				"error": "failed to marshal payment event",
			})
		}

		err = nc.Publish("payment", jsonPaymentEvent)
		if err != nil {
			log.Printf("failed to publish payment event: %s", err)
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
				"error": "failed to publish payment event",
			})
		}

		return c.Status(http.StatusCreated).JSON(fiber.Map{
			"message": "Payment created",
		})
	}
}

func PaymentSummaryHandler(store *storage.PaymentStore) fiber.Handler {
	return func(c fiber.Ctx) error {
		fromStr := c.Query("from")
		toStr := c.Query("to")

		from, err := time.Parse(time.RFC3339, fromStr)
		if err != nil {
			log.Printf("failed to parse start date: %s", err)
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{
				"error": "failed to parse start date",
			})
		}

		to, err := time.Parse(time.RFC3339, toStr)
		if err != nil {
			log.Printf("failed to parse end date: %s", err)
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{
				"error": "failed to parse end date",
			})
		}

		summary, err := store.GetPaymentSummary(from, to)
		if err != nil {
			log.Printf("failed to get payment summary: %s", err)
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
				"error": "failed to get payment summary",
			})
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
