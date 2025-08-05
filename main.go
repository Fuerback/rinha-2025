package main

import (
	"database/sql"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/Fuerback/rinha-2025/internal/handler"
	"github.com/Fuerback/rinha-2025/internal/storage"
	"github.com/Fuerback/rinha-2025/internal/worker"
	"github.com/gofiber/fiber/v3"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func main() {
	app := fiber.New()

	_ = godotenv.Load()

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

	paymentStorage, err := storage.NewPaymentPostgresStorage(db)
	if err != nil {
		log.Fatal("failed to connect to database", "error", err)
	}

	paymentWorker := worker.NewPaymentProcessorWorker(paymentStorage)
	go paymentWorker.Start()

	app.Post("/payments", handler.CreatePaymentHandler(paymentWorker))
	app.Get("/payments-summary", handler.PaymentSummaryHandler(paymentStorage))

	if err := app.Listen(":8080"); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("failed to start server", "error", err)
		paymentWorker.Stop()
	}
}
