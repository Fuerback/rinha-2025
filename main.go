package main

import (
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"

	"github.com/Fuerback/rinha-2025/internal/event"
	"github.com/Fuerback/rinha-2025/internal/handler"
	"github.com/Fuerback/rinha-2025/internal/storage"
	"github.com/Fuerback/rinha-2025/internal/worker"
	"github.com/joho/godotenv"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	_ "github.com/lib/pq"
)

func main() {
	// Echo instance
	e := echo.New()

	_ = godotenv.Load()

	// Middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	// Initialize rabbitmq connection
	event.NewRabbitMQConnection()
	defer event.RabbitMQClient.Close()

	// Initialize database connection
	DB_URL := os.Getenv("DATABASE_URL")
	if DB_URL == "" {
		e.Logger.Fatal("DATABASE_URL is not set")
	}

	db, err := sql.Open("postgres", DB_URL)
	if err != nil {
		e.Logger.Fatal("failed to connect to database", "error", err)
	}
	defer db.Close()

	store := storage.NewPaymentStore(db)

	// Routes
	e.POST("/payments", handler.CreatePaymentHandler(store))
	e.GET("/payments-summary", handler.PaymentSummaryHandler(store))

	// Payment Processor
	go worker.PaymentProcessor(store)

	// Start server
	if err := e.Start(":9999"); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("failed to start server", "error", err)
	}
}
