package main

import (
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"

	"github.com/Fuerback/rinha-2025/internal/handler"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	_ "github.com/lib/pq"
)

func main() {
	// Echo instance
	e := echo.New()

	// Middleware
	e.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
		Format: `{"time":"${time_rfc3339_nano}","id":"${id}",` +
			`"host":"${host}","method":"${method}","uri":"${uri}",` +
			`"status":${status},"error":"${error}","latency":${latency},"latency_human":"${latency_human}"` +
			`,"bytes_in":${bytes_in},"bytes_out":${bytes_out}}` + "\n",
	}))
	e.Use(middleware.Recover())

	// Initialize database connection
	DB_URL := os.Getenv("DATABASE_URL")

	db, err := sql.Open("postgres", DB_URL)
	if err != nil {
		e.Logger.Fatal("failed to connect to database", "error", err)
	}
	defer db.Close()

	// Routes
	e.POST("/payments", handler.CreatePaymentHandler())
	e.GET("/payments-summary", handler.PaymentSummaryHandler())

	// Start server
	if err := e.Start(":9999"); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("failed to start server", "error", err)
	}
}
