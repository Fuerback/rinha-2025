package main

import (
	"database/sql"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/Fuerback/rinha-2025/internal/handler"
	"github.com/Fuerback/rinha-2025/internal/storage"
	"github.com/Fuerback/rinha-2025/internal/worker"
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/logger"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
)

func main() {
	app := fiber.New()

	app.Use(logger.New())

	_ = godotenv.Load()

	// event.NewRabbitMQConnection()
	// defer event.RabbitMQClient.Close()

	DB_URL := os.Getenv("DATABASE_URL")
	if DB_URL == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	db, err := sql.Open("postgres", DB_URL)
	if err != nil {
		log.Fatal("failed to connect to database", "error", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(60)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(time.Hour * 2)
	db.SetConnMaxIdleTime(time.Minute * 5)

	if err := db.Ping(); err != nil {
		log.Fatal("failed to ping database", "error", err)
	}

	paymentStorage := storage.NewPaymentStorage(db)

	nc, err := nats.Connect(os.Getenv("NATS_URL"))
	if err != nil {
		log.Fatal("failed to connect to nats", "error", err)
	}
	defer nc.Drain()

	workers := os.Getenv("WORKERS")
	if workers == "" {
		workers = "3"
	}
	workerCount, err := strconv.Atoi(workers)
	if err != nil {
		log.Fatal("failed to parse workers", "error", err)
	}

	go worker.PaymentProcessor(paymentStorage, nc, workerCount)

	app.Post("/payments", handler.CreatePaymentHandler(paymentStorage, nc))
	app.Get("/payments-summary", handler.PaymentSummaryHandler(paymentStorage))

	if err := app.Listen(":9999"); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("failed to start server", "error", err)
	}
}
