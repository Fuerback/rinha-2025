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

	"github.com/Fuerback/rinha-2025/internal/domain"
	"github.com/Fuerback/rinha-2025/internal/storage"
	"github.com/nats-io/nats.go"
)

type requestBody struct {
	CorrelationID string    `json:"correlationId"`
	Amount        string    `json:"amount"`
	RequestedAt   time.Time `json:"requestedAt"`
}

func PaymentProcessor(store *storage.PaymentStore, nc *nats.Conn, workerCount int) {
	forever := make(chan bool)

	clientTimeoutStr := os.Getenv("CLIENT_TIMEOUT")
	if clientTimeoutStr == "" {
		clientTimeoutStr = "500"
	}
	clientTimeout, err := strconv.Atoi(clientTimeoutStr)
	if err != nil {
		log.Fatal("failed to parse client timeout", "error", err)
	}

	defaultProcessorURL := os.Getenv("PROCESSOR_DEFAULT_URL")
	fallbackProcessorURL := os.Getenv("PROCESSOR_FALLBACK_URL")

	for i := range workerCount {
		go func(id int) {
			nc.QueueSubscribe("payment", "payment-queue", func(msg *nats.Msg) {
				var paymentEvent domain.PaymentEvent
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

				paymentProcessor := domain.PaymentProcessorDefault
				err = tryProcessor(defaultProcessorURL, body, time.Duration(clientTimeout)*time.Millisecond)
				if err != nil {
					log.Printf("Error processing payment with default processor: %s", err)
					paymentProcessor = domain.PaymentProcessorFallback
					err = tryProcessor(fallbackProcessorURL, body, time.Duration(clientTimeout)*time.Millisecond)
					if err != nil {
						log.Printf("Error processing payment with fallback processor: %s", err)
						addPaymentToRetry(paymentEvent, nc, err)
						return
					}
				}

				log.Printf("Payment processed successfully with %s processor\n", paymentProcessor)

				err = store.CreatePayment(domain.NewPayment(paymentEvent.CorrelationID, paymentEvent.Amount, paymentEvent.RequestedAt, paymentProcessor))
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

func tryProcessor(url string, body requestBody, timeout time.Duration) error {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url+"/payments", bytes.NewBuffer(jsonBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout: timeout,
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnprocessableEntity {
		return nil
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("processor returned non-OK status: %d", resp.StatusCode)
	}

	return nil
}

func addPaymentToRetry(paymentEvent domain.PaymentEvent, nc *nats.Conn, err error) error {
	log.Default().Printf("enqueue to retry payment with id %s numRetry %d: %v", paymentEvent.CorrelationID, paymentEvent.RetryCount, err)

	// convert paymentEvent to json
	jsonPaymentEvent, err := json.Marshal(paymentEvent)
	if err != nil {
		log.Printf("failed to marshal payment event: %s", err)
		return fmt.Errorf("failed to marshal payment event: %s", err)
	}

	err = nc.Publish("payment", jsonPaymentEvent)
	if err != nil {
		log.Printf("failed to publish payment event: %s", err)
		return fmt.Errorf("failed to publish payment event: %s", err)
	}

	// TODO: no retry delay?
	//time.Sleep(20 * time.Millisecond)

	// err = event.RabbitMQClient.SendPaymentEventRetry(paymentEvent)
	// if err != nil {
	// 	return err
	// }

	return nil
}
