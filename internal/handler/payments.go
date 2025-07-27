package handler

import (
	"fmt"
	"net/http"
	"time"

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
	TotalRequests int `json:"total_requests"`
	TotalAmount   int `json:"total_amount"`
}

func CreatePaymentHandler() echo.HandlerFunc {
	return func(c echo.Context) error {
		var req PaymentRequest
		if err := c.Bind(&req); err != nil {
			return err
		}

		fmt.Println(req)

		return c.JSON(http.StatusCreated, "Payment created")
	}
}

func PaymentSummaryHandler() echo.HandlerFunc {
	return func(c echo.Context) error {
		fromStr := c.QueryParam("from")
		toStr := c.QueryParam("to")

		from, err := time.Parse(time.RFC3339, fromStr)
		if err != nil {
			return c.JSON(http.StatusBadRequest, "invalid value for start date")
		}

		to, err := time.Parse(time.RFC3339, toStr)
		if err != nil {
			return c.JSON(http.StatusBadRequest, "invalid value for end date")
		}

		fmt.Println(from, to)

		return c.JSON(http.StatusOK, PaymentResponse{
			Default: PaymentSummaryResponse{
				TotalRequests: 2,
				TotalAmount:   100,
			},
			Fallback: PaymentSummaryResponse{
				TotalRequests: 1,
				TotalAmount:   50,
			},
		})
	}
}
