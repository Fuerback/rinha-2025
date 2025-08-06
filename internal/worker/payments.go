package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Fuerback/rinha-2025/internal/model"
	"github.com/Fuerback/rinha-2025/internal/storage"
)

type requestBody struct {
	CorrelationID string    `json:"correlationId"`
	Amount        string    `json:"amount"`
	RequestedAt   time.Time `json:"requestedAt"`
}

type PaymentProcessorWorker struct {
	store                storage.PaymentStore
	client               *http.Client
	defaultProcessorURL  string
	fallbackProcessorURL string
	workerCount          int
	jobChan              chan *model.PaymentEvent
	retryChan            chan *model.PaymentEvent
	ctx                  context.Context
	cancel               context.CancelFunc
	wg                   sync.WaitGroup
}

var errExternalClientTimeout = errors.New("external client timeout")

func NewPaymentProcessorWorker(store storage.PaymentStore) *PaymentProcessorWorker {
	// clientTimeoutStr := os.Getenv("CLIENT_TIMEOUT")
	// if clientTimeoutStr == "" {
	// 	clientTimeoutStr = "500"
	// }
	// clientTimeout, err := strconv.Atoi(clientTimeoutStr)
	// if err != nil {
	// 	log.Fatal("failed to parse client timeout", "error", err)
	// }

	workers := os.Getenv("WORKERS")
	if workers == "" {
		workers = "3"
	}
	workerCount, err := strconv.Atoi(workers)
	if err != nil {
		log.Fatal("failed to parse workers", "error", err)
	}

	transport := &http.Transport{
		MaxIdleConns:        2000, // Increase for high concurrency
		MaxIdleConnsPerHost: 2000, // Increase for high concurrency
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,  // Lower timeout for faster failover
			KeepAlive: 60 * time.Second, // Longer keepalive for connection reuse
		}).DialContext,
		// Optional: tune TLSHandshakeTimeout, ExpectContinueTimeout, etc.
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   5 * time.Second, // Lower timeout for faster error returns
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &PaymentProcessorWorker{
		store:                store,
		defaultProcessorURL:  os.Getenv("PROCESSOR_DEFAULT_URL"),
		fallbackProcessorURL: os.Getenv("PROCESSOR_FALLBACK_URL"),
		workerCount:          workerCount,
		jobChan:              make(chan *model.PaymentEvent, 10000),
		retryChan:            make(chan *model.PaymentEvent, 10000),
		ctx:                  ctx,
		cancel:               cancel,
		client:               client,
	}
}

func (w *PaymentProcessorWorker) AddMessage(msg *model.PaymentEvent) {
	select {
	case w.jobChan <- msg:
	case <-w.ctx.Done():
		return
	default:
		// Channel full, log and ack message to prevent redelivery
		log.Printf("Worker queue full, dropping message")
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
			w.processMessage(msg)
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
			time.Sleep(1000 * time.Millisecond)
			w.processMessage(msg)
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *PaymentProcessorWorker) processMessage(msg *model.PaymentEvent) {
	body := requestBody{
		CorrelationID: msg.CorrelationID,
		Amount:        msg.Amount.String(),
		RequestedAt:   msg.RequestedAt,
	}

	paymentProcessor := model.PaymentProcessorDefault
	err := w.SendToProcessor(w.defaultProcessorURL, msg, body)
	if err != nil {
		if errors.Is(err, errExternalClientTimeout) && msg.PreferredProcessor == "" {
			msg.PreferredProcessor = model.PaymentProcessorDefault
			w.retryPayment(msg)
			return
		}
		log.Printf("Error processing payment with default processor: %s", err)
		paymentProcessor = model.PaymentProcessorFallback
		err = w.SendToProcessor(w.fallbackProcessorURL, msg, body)
		if err != nil {
			if errors.Is(err, errExternalClientTimeout) && msg.PreferredProcessor == "" {
				msg.PreferredProcessor = model.PaymentProcessorFallback
				w.retryPayment(msg)
				return
			}
			log.Printf("Error processing payment with fallback processor: %s", err)
			w.retryPayment(msg)
			return
		}
	}

	if msg.PreferredProcessor != "" {
		paymentProcessor = msg.PreferredProcessor
	}

	err = w.store.CreatePayment(model.NewPayment(msg.CorrelationID, msg.Amount, msg.RequestedAt, paymentProcessor))
	if err != nil {
		if err == storage.ErrUniqueViolation {
			log.Println("Payment already exists")
		} else {
			log.Printf("failed to create payment: %s", err)
		}
	}
}

func (w *PaymentProcessorWorker) SendToProcessor(url string, msg *model.PaymentEvent, body requestBody) error {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	if msg.PreferredProcessor != "" {
		resp, err := w.client.Get(url + "/payments/" + body.CorrelationID)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			log.Printf("Payment already processed: %s", body.CorrelationID)
			return nil
		} else if resp.StatusCode == http.StatusNotFound {
			log.Printf("Payment not found: %s", body.CorrelationID)
			msg.PreferredProcessor = ""
		} else {
			log.Printf("Using preferred processor for correlationId: %s, preferredProcessor: %s", body.CorrelationID, msg.PreferredProcessor)
			if msg.PreferredProcessor == model.PaymentProcessorDefault {
				url = w.defaultProcessorURL
			} else {
				url = w.fallbackProcessorURL
			}
		}
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
		if errors.Is(err, context.DeadlineExceeded) {
			return errExternalClientTimeout
		} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return errExternalClientTimeout
		}
		return fmt.Errorf("failed to process payment: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		if resp.StatusCode == http.StatusUnprocessableEntity {
			log.Printf("processor returned unprocessable entity status: %d, correlationId: %s", resp.StatusCode, body.CorrelationID)
			return nil
		}
		return fmt.Errorf("processor returned non-OK status: %d", resp.StatusCode)
	}

	return nil
}

func (w *PaymentProcessorWorker) retryPayment(msg *model.PaymentEvent) {
	select {
	case w.retryChan <- msg:
	case <-w.ctx.Done():
		return
	default:
		// Channel full, log and ack message to prevent redelivery
		log.Printf("Worker queue full, dropping message")
	}
}

func (w *PaymentProcessorWorker) Stop() {
	w.cancel()
}
