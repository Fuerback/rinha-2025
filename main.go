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

	maxWorkers := os.Getenv("MAX_WORKERS")
	if maxWorkers == "" {
		log.Fatal("MAX_WORKERS is not set")
	}

	maxWorkersNum, err := strconv.Atoi(maxWorkers)
	if err != nil {
		log.Fatal("failed to convert MAX_WORKERS to int", "error", err)
	}

	// Get max queue size from environment or use default
	maxQueue := os.Getenv("MAX_QUEUE")
	if maxQueue == "" {
		maxQueue = "1000" // Default queue size
	}

	maxQueueNum, err := strconv.Atoi(maxQueue)
	if err != nil {
		log.Fatal("failed to convert MAX_QUEUE to int", "error", err)
	}

	paymentStorage := storage.NewPaymentStorage(db)

	// Initialize JobQueue before starting dispatcher
	worker.InitJobQueue(maxQueueNum)
	log.Printf("Initialized JobQueue with buffer size: %d", maxQueueNum)

	app.Post("/payments", handler.CreatePaymentHandler(paymentStorage))
	app.Get("/payments-summary", handler.PaymentSummaryHandler(paymentStorage))

	dispatcher := worker.NewDispatcher(maxWorkersNum, paymentStorage)
	dispatcher.Run()
	log.Printf("Started dispatcher with %d workers", maxWorkersNum)

	// Payment Processor
	//go worker.PaymentProcessor(paymentStorage)

	if err := app.Listen(":9999"); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("failed to start server", "error", err)
	}
}
