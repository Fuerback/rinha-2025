package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Fuerback/rinha-2025/internal/domain"
	"github.com/Fuerback/rinha-2025/internal/event"
	"github.com/Fuerback/rinha-2025/internal/storage"
)

type requestBody struct {
	CorrelationID string    `json:"correlationId"`
	Amount        string    `json:"amount"`
	RequestedAt   time.Time `json:"requestedAt"`
}

func PaymentProcessor(store *storage.PaymentStore) {
	msgs, err := event.RabbitMQClient.ConsumePaymentEvent()

	if err != nil {
		log.Fatalf("Failed to consume RabbitMQ queue: %s", err)
		return
	}

	forever := make(chan bool)

	const workerCount = 15

	for i := range workerCount {
		go func(id int) {
			for d := range msgs {
				var paymentEvent domain.PaymentEvent
				err := json.Unmarshal(d.Body, &paymentEvent)
				if err != nil {
					log.Printf("Error reading payment event (please check the JSON format): %s", err)
					continue
				}

				fmt.Printf("Received a payment event: Correlation ID = %s, Amount = %s\n", paymentEvent.CorrelationID, paymentEvent.Amount)

				body := requestBody{
					CorrelationID: paymentEvent.CorrelationID,
					Amount:        paymentEvent.Amount.String(),
					RequestedAt:   time.Now(),
				}

				paymentProcessor := domain.PaymentProcessorDefault
				err = tryProcessor(os.Getenv("PROCESSOR_DEFAULT_URL"), body, 200*time.Millisecond)
				if err != nil {
					fmt.Printf("Error processing payment with default processor: %s", err)
					paymentProcessor = domain.PaymentProcessorFallback
					err = tryProcessor(os.Getenv("PROCESSOR_FALLBACK_URL"), body, 200*time.Millisecond)
					if err != nil {
						fmt.Printf("Error processing payment with fallback processor: %s", err)
						// should add the payment to the queue again to be reprocessed, but it should have a retry logic and a retry delay
						// maybe create a channel to store the failed payments and retry them later in another thread
						// failedPaymentsChannel <- paymentEvent
						continue
					}
				}

				fmt.Printf("Payment processed successfully with %s processor\n", paymentProcessor)

				err = store.CreatePayment(domain.NewPayment(paymentEvent.CorrelationID, paymentEvent.Amount, paymentProcessor))
				if err != nil {
					if err == storage.ErrUniqueViolation {
						fmt.Println("Payment already exists")
						continue
					}
					fmt.Printf("failed to create payment: %s", err)
					continue
				}
			}
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

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("processor returned non-OK status: %d", resp.StatusCode)
	}

	return nil
}
