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
	"github.com/gofiber/fiber/v3/middleware/logger"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
)

func main() {
	app := fiber.New()

	app.Use(logger.New())

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

	nc, err := nats.Connect(os.Getenv("NATS_URL"),
		nats.MaxReconnects(10),
		nats.ReconnectWait(2*time.Second),
		nats.ReconnectJitter(500*time.Millisecond, 2*time.Second),
		nats.Timeout(5*time.Second),
		nats.PingInterval(30*time.Second),
		nats.MaxPingsOutstanding(3),
		nats.ReconnectBufSize(8*1024*1024), // 8MB buffer
		nats.SyncQueueLen(1024),            // Larger subscription channel
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("Reconnected to NATS server: %v", nc.ConnectedUrl())
		}),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Printf("NATS disconnected: %v", err)
		}),
	)
	if err != nil {
		log.Fatal("failed to connect to nats", "error", err)
	}
	defer nc.Drain()

	paymentWorker := worker.NewPaymentProcessorWorker(paymentStorage, nc)
	go paymentWorker.Start()

	app.Post("/payments", handler.CreatePaymentHandler(nc))
	app.Get("/payments-summary", handler.PaymentSummaryHandler(paymentStorage))

	if err := app.Listen(":8080"); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("failed to start server", "error", err)
	}
}
