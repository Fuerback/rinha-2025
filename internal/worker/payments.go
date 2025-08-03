package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
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
	store  storage.PaymentStore
	nc     *nats.Conn
	client *http.Client
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

	return &PaymentProcessorWorker{
		store: store,
		nc:    nc,
		client: &http.Client{
			Timeout: time.Duration(clientTimeout) * time.Millisecond,
		},
	}
}

func (w *PaymentProcessorWorker) Start() {
	forever := make(chan bool)

	workers := os.Getenv("WORKERS")
	if workers == "" {
		workers = "3"
	}
	workerCount, err := strconv.Atoi(workers)
	if err != nil {
		log.Fatal("failed to parse workers", "error", err)
	}

	defaultProcessorURL := os.Getenv("PROCESSOR_DEFAULT_URL")
	fallbackProcessorURL := os.Getenv("PROCESSOR_FALLBACK_URL")

	for i := range workerCount {
		go func(id int) {
			w.nc.QueueSubscribe("payment", "payment-queue", func(msg *nats.Msg) {
				var paymentEvent model.PaymentEvent
				err := json.Unmarshal(msg.Data, &paymentEvent)
				if err != nil {
					log.Printf("Error reading payment event (please check the JSON format): %s", err)
					return
				}

				body := requestBody{
					CorrelationID: paymentEvent.CorrelationID,
					Amount:        paymentEvent.Amount.String(),
					RequestedAt:   paymentEvent.RequestedAt,
				}

				paymentProcessor := model.PaymentProcessorDefault
				err = w.SendToProcessor(defaultProcessorURL, body)
				if err != nil {
					log.Printf("Error processing payment with default processor: %s", err)
					paymentProcessor = model.PaymentProcessorFallback
					err = w.SendToProcessor(fallbackProcessorURL, body)
					if err != nil {
						log.Printf("Error processing payment with fallback processor: %s", err)
						w.retryPayment(msg.Data, err)
						return
					}
				}

				log.Printf("Payment processed successfully with %s processor\n", paymentProcessor)

				err = w.store.CreatePayment(model.NewPayment(paymentEvent.CorrelationID, paymentEvent.Amount, paymentEvent.RequestedAt, paymentProcessor))
				if err != nil {
					if err == storage.ErrUniqueViolation {
						log.Println("Payment already exists")
						return
					}
					log.Printf("failed to create payment: %s", err)
					return
				}
			})
		}(i)
	}

	<-forever
}

func (w *PaymentProcessorWorker) SendToProcessor(url string, body requestBody) error {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url+"/payments", bytes.NewBuffer(jsonBody))
	if err != nil {
		return err
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

func (w *PaymentProcessorWorker) retryPayment(paymentEvent []byte, err error) error {
	log.Default().Printf("enqueue to retry payment: %v", err)

	err = w.nc.Publish("payment", paymentEvent)
	if err != nil {
		log.Printf("failed to publish payment event: %s", err)
		return fmt.Errorf("failed to publish payment event: %s", err)
	}

	return nil
}
