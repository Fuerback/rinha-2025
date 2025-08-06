package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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

var errDeadlineExceeded = errors.New("deadline exceeded")

func NewPaymentProcessorWorker(store storage.PaymentStore) *PaymentProcessorWorker {
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
		defaultProcessorURL:  os.Getenv("PROCESSOR_DEFAULT_URL"),
		fallbackProcessorURL: os.Getenv("PROCESSOR_FALLBACK_URL"),
		workerCount:          workerCount,
		jobChan:              make(chan *model.PaymentEvent, 10000),
		retryChan:            make(chan *model.PaymentEvent, 10000),
		ctx:                  ctx,
		cancel:               cancel,
		client: &http.Client{
			Timeout:   time.Duration(clientTimeout) * time.Millisecond,
			Transport: transport,
		},
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

	paymentProcessorResp, err := w.store.GetHealthCheck()
	if err != nil {
		log.Printf("Error getting health check: %s", err)
		w.retryPayment(msg)
		return
	}

	switch paymentProcessorResp.PreferredProcessor {
	case 0:
		err := w.SendToProcessor(w.defaultProcessorURL, body, msg)
		if err != nil {
			if errors.Is(err, errDeadlineExceeded) {
				msg.PreferredProcessor = model.PaymentProcessorDefault
			}
			log.Printf("Error processing payment with default processor: %s", err)
			w.retryPayment(msg)
			return
		}
	case 1:
		err := w.SendToProcessor(w.fallbackProcessorURL, body, msg)
		if err != nil {
			if errors.Is(err, errDeadlineExceeded) {
				msg.PreferredProcessor = model.PaymentProcessorFallback
			}
			log.Printf("Error processing payment with fallback processor: %s", err)
			w.retryPayment(msg)
			return
		}
	default:
		w.retryPayment(msg)
	}

	err = w.store.CreatePayment(model.NewPayment(msg.CorrelationID, msg.Amount, msg.RequestedAt, model.PaymentProcessorTranslationMap[paymentProcessorResp.PreferredProcessor]))
	if err != nil {
		if err == storage.ErrUniqueViolation {
			log.Println("Payment already exists")
		} else {
			log.Printf("failed to create payment: %s", err)
		}
	}
}

func (w *PaymentProcessorWorker) SendToProcessor(url string, body requestBody, msg *model.PaymentEvent) error {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	if msg.PreferredProcessor != "" {
		if msg.PreferredProcessor == model.PaymentProcessorDefault {
			url = w.defaultProcessorURL
		} else {
			url = w.fallbackProcessorURL
		}
	}

	req, err := http.NewRequest("POST", url+"/payments", bytes.NewBuffer(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %s", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return errDeadlineExceeded
		}
		return fmt.Errorf("failed to process payment: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnprocessableEntity {
		return nil
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
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
