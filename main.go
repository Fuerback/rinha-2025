package main

import (
	"database/sql"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/Fuerback/rinha-2025/internal/event"
	"github.com/Fuerback/rinha-2025/internal/handler"
	"github.com/Fuerback/rinha-2025/internal/storage"
	"github.com/Fuerback/rinha-2025/internal/worker"
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/logger"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func main() {
	app := fiber.New()

	app.Use(logger.New())

	_ = godotenv.Load()

	event.NewRabbitMQConnection()
	defer event.RabbitMQClient.Close()

	DB_URL := os.Getenv("DATABASE_URL")
	if DB_URL == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	db, err := sql.Open("postgres", DB_URL)
	if err != nil {
		log.Fatal("failed to connect to database", "error", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(200)
	db.SetMaxIdleConns(30)
	db.SetConnMaxLifetime(time.Hour * 2)
	db.SetConnMaxIdleTime(time.Minute * 5)

	if err := db.Ping(); err != nil {
		log.Fatal("failed to ping database", "error", err)
	}

	paymentStorage := storage.NewPaymentStorage(db)

	worker := worker.NewPaymentProcessor(paymentStorage)
	worker.Init()

	app.Post("/payments", handler.CreatePaymentHandler(paymentStorage, worker))
	app.Get("/payments-summary", handler.PaymentSummaryHandler(paymentStorage))

	// Payment Processor
	//go worker.PaymentProcessor(paymentStorage)

	if err := app.Listen(":9999"); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("failed to start server", "error", err)
	}
}
