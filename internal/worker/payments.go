package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Fuerback/rinha-2025/internal/model"
	"github.com/Fuerback/rinha-2025/internal/storage"
	"github.com/nats-io/nats.go"
)

type requestBody struct {
	CorrelationID string    `json:"correlationId"`
	Amount        string    `json:"amount"`
	RequestedAt   time.Time `json:"requestedAt"`
}

type PaymentProcessorWorker struct {
	store                storage.PaymentStore
	nc                   *nats.Conn
	client               *http.Client
	defaultProcessorURL  string
	fallbackProcessorURL string
	workerCount          int
	jobChan              chan *nats.Msg
	retryChan            chan *nats.Msg
	ctx                  context.Context
	cancel               context.CancelFunc
	wg                   sync.WaitGroup
}

func NewPaymentProcessorWorker(store storage.PaymentStore, nc *nats.Conn) *PaymentProcessorWorker {
	clientTimeoutStr := os.Getenv("CLIENT_TIMEOUT")
	if clientTimeoutStr == "" {
		clientTimeoutStr = "500"
	}
	clientTimeout, err := strconv.Atoi(clientTimeoutStr)
	if err != nil {
		log.Fatal("failed to parse client timeout", "error", err)
	}

	workers := os.Getenv("WORKERS")
	if workers == "" {
		workers = "3"
	}
	workerCount, err := strconv.Atoi(workers)
	if err != nil {
		log.Fatal("failed to parse workers", "error", err)
	}

	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 50,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		DisableKeepAlives:   false,
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &PaymentProcessorWorker{
		store:                store,
		nc:                   nc,
		defaultProcessorURL:  os.Getenv("PROCESSOR_DEFAULT_URL"),
		fallbackProcessorURL: os.Getenv("PROCESSOR_FALLBACK_URL"),
		workerCount:          workerCount,
		jobChan:              make(chan *nats.Msg, 10000),
		retryChan:            make(chan *nats.Msg, 10000),
		ctx:                  ctx,
		cancel:               cancel,
		client: &http.Client{
			Timeout:   time.Duration(clientTimeout) * time.Millisecond,
			Transport: transport,
		},
	}
}

func (w *PaymentProcessorWorker) Start() {
	// Start worker goroutines
	for i := 0; i < w.workerCount; i++ {
		w.wg.Add(1)
		go w.worker()
	}

	// Start retry worker
	w.wg.Add(1)
	go w.retryWorker()

	// Subscribe to main payment queue
	sub, err := w.nc.QueueSubscribe("payment", "payment-queue", func(msg *nats.Msg) {
		select {
		case w.jobChan <- msg:
		case <-w.ctx.Done():
			return
		default:
			// Channel full, log and ack message to prevent redelivery
			log.Printf("Worker queue full, dropping message")
			msg.Ack()
		}
	})
	if err != nil {
		log.Fatal("failed to subscribe to payment queue", "error", err)
	}
	defer sub.Unsubscribe()

	// Subscribe to retry queue
	retrySub, err := w.nc.QueueSubscribe("payment-retry", "payment-retry-queue", func(msg *nats.Msg) {
		select {
		case w.retryChan <- msg:
		case <-w.ctx.Done():
			return
		default:
			log.Printf("Retry queue full, dropping message")
			msg.Ack()
		}
	})
	if err != nil {
		log.Fatal("failed to subscribe to payment retry queue", "error", err)
	}
	defer retrySub.Unsubscribe()

	// Wait for context cancellation
	<-w.ctx.Done()

	// Close channels and wait for workers to finish
	close(w.jobChan)
	close(w.retryChan)
	w.wg.Wait()
}

func (w *PaymentProcessorWorker) worker() {
	defer w.wg.Done()

	for {
		select {
		case msg, ok := <-w.jobChan:
			if !ok {
				return
			}
			w.processMessage(msg, false)
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *PaymentProcessorWorker) retryWorker() {
	defer w.wg.Done()

	for {
		select {
		case msg, ok := <-w.retryChan:
			if !ok {
				return
			}
			// Add delay for retry
			time.Sleep(500 * time.Millisecond)
			w.processMessage(msg, true)
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *PaymentProcessorWorker) processMessage(msg *nats.Msg, isRetry bool) {
	var paymentEvent model.PaymentEvent
	err := json.Unmarshal(msg.Data, &paymentEvent)
	if err != nil {
		log.Printf("Error reading payment event: %s", err)
		msg.Ack()
		return
	}

	body := requestBody{
		CorrelationID: paymentEvent.CorrelationID,
		Amount:        paymentEvent.Amount.String(),
		RequestedAt:   paymentEvent.RequestedAt,
	}

	paymentProcessor := model.PaymentProcessorDefault
	err = w.SendToProcessor(w.defaultProcessorURL, body)
	if err != nil {
		log.Printf("Error processing payment with default processor: %s", err)
		paymentProcessor = model.PaymentProcessorFallback
		err = w.SendToProcessor(w.fallbackProcessorURL, body)
		if err != nil {
			log.Printf("Error processing payment with fallback processor: %s", err)
			if !isRetry {
				w.retryPayment(msg.Data)
			}
			msg.Ack()
			return
		}
	}

	log.Printf("Payment processed successfully with %s processor", paymentProcessor)

	err = w.store.CreatePayment(model.NewPayment(paymentEvent.CorrelationID, paymentEvent.Amount, paymentEvent.RequestedAt, paymentProcessor))
	if err != nil {
		if err == storage.ErrUniqueViolation {
			log.Println("Payment already exists")
		} else {
			log.Printf("failed to create payment: %s", err)
		}
	}

	msg.Ack()
}

func (w *PaymentProcessorWorker) SendToProcessor(url string, body requestBody) error {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", url+"/payments", bytes.NewBuffer(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %s", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to process payment: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		if resp.StatusCode == http.StatusUnprocessableEntity {
			return nil
		}
		return fmt.Errorf("processor returned non-OK status: %d", resp.StatusCode)
	}

	return nil
}

func (w *PaymentProcessorWorker) retryPayment(paymentEvent []byte) {
	err := w.nc.Publish("payment-retry", paymentEvent)
	if err != nil {
		log.Printf("failed to publish payment retry event: %s", err)
	}
}

func (w *PaymentProcessorWorker) Stop() {
	w.cancel()
}
