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
	"github.com/Fuerback/rinha-2025/internal/storage"
	"github.com/nats-io/nats.go"
)

type requestBody struct {
	CorrelationID string    `json:"correlationId"`
	Amount        string    `json:"amount"`
	RequestedAt   time.Time `json:"requestedAt"`
}

func PaymentProcessor(store *storage.PaymentStore, nc *nats.Conn) {
	// msgs, err := event.RabbitMQClient.ConsumePaymentEvent()
	// if err != nil {
	// 	log.Fatalf("Failed to consume RabbitMQ queue: %s", err)
	// 	return
	// }

	// msgsRetry, err := event.RabbitMQClient.ConsumePaymentEventRetry()
	// if err != nil {
	// 	log.Fatalf("Failed to consume RabbitMQ retry queue: %s", err)
	// 	return
	// }

	forever := make(chan bool)

	const workerCount = 10

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
				err = tryProcessor(os.Getenv("PROCESSOR_DEFAULT_URL"), body, 200*time.Millisecond)
				if err != nil {
					log.Printf("Error processing payment with default processor: %s", err)
					paymentProcessor = domain.PaymentProcessorFallback
					err = tryProcessor(os.Getenv("PROCESSOR_FALLBACK_URL"), body, 200*time.Millisecond)
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

			// for d := range msgs {
			// 	var paymentEvent domain.PaymentEvent
			// 	err := json.Unmarshal(d.Body, &paymentEvent)
			// 	if err != nil {
			// 		log.Printf("Error reading payment event (please check the JSON format): %s", err)
			// 		continue
			// 	}

			// 	log.Printf("Received a payment event: Correlation ID = %s, Amount = %s\n", paymentEvent.CorrelationID, paymentEvent.Amount)

			// 	body := requestBody{
			// 		CorrelationID: paymentEvent.CorrelationID,
			// 		Amount:        paymentEvent.Amount.String(),
			// 		RequestedAt:   paymentEvent.RequestedAt,
			// 	}

			// 	paymentProcessor := domain.PaymentProcessorDefault
			// 	err = tryProcessor(os.Getenv("PROCESSOR_DEFAULT_URL"), body, 200*time.Millisecond)
			// 	if err != nil {
			// 		log.Printf("Error processing payment with default processor: %s", err)
			// 		paymentProcessor = domain.PaymentProcessorFallback
			// 		err = tryProcessor(os.Getenv("PROCESSOR_FALLBACK_URL"), body, 200*time.Millisecond)
			// 		if err != nil {
			// 			log.Printf("Error processing payment with fallback processor: %s", err)
			// 			addPaymentToRetry(paymentEvent, err)
			// 			continue
			// 		}
			// 	}

			// 	log.Printf("Payment processed successfully with %s processor\n", paymentProcessor)

			// 	err = store.CreatePayment(domain.NewPayment(paymentEvent.CorrelationID, paymentEvent.Amount, paymentEvent.RequestedAt, paymentProcessor))
			// 	if err != nil {
			// 		if err == storage.ErrUniqueViolation {
			// 			log.Println("Payment already exists")
			// 			continue
			// 		}
			// 		log.Printf("failed to create payment: %s", err)
			// 		continue
			// 	}
			// }
		}(i)
	}

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
				err = tryProcessor(os.Getenv("PROCESSOR_DEFAULT_URL"), body, 200*time.Millisecond)
				if err != nil {
					log.Printf("Error processing payment with default processor: %s", err)
					paymentProcessor = domain.PaymentProcessorFallback
					err = tryProcessor(os.Getenv("PROCESSOR_FALLBACK_URL"), body, 200*time.Millisecond)
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

	// for i := range workerCount {
	// 	go func(id int) {
	// 		for d := range msgsRetry {
	// 			var paymentEvent domain.PaymentEvent
	// 			err := json.Unmarshal(d.Body, &paymentEvent)
	// 			if err != nil {
	// 				log.Printf("Error reading payment event (please check the JSON format): %s", err)
	// 				continue
	// 			}

	// 			log.Printf("Received a payment event: Correlation ID = %s, Amount = %s\n", paymentEvent.CorrelationID, paymentEvent.Amount)

	// 			body := requestBody{
	// 				CorrelationID: paymentEvent.CorrelationID,
	// 				Amount:        paymentEvent.Amount.String(),
	// 				RequestedAt:   paymentEvent.RequestedAt,
	// 			}

	// 			paymentProcessor := domain.PaymentProcessorDefault
	// 			err = tryProcessor(os.Getenv("PROCESSOR_DEFAULT_URL"), body, 200*time.Millisecond)
	// 			if err != nil {
	// 				log.Printf("Error processing payment with default processor: %s", err)
	// 				paymentProcessor = domain.PaymentProcessorFallback
	// 				err = tryProcessor(os.Getenv("PROCESSOR_FALLBACK_URL"), body, 200*time.Millisecond)
	// 				if err != nil {
	// 					log.Printf("Error processing payment with fallback processor: %s", err)
	// 					addPaymentToRetry(paymentEvent, err)
	// 					continue
	// 				}
	// 			}

	// 			log.Printf("Payment processed successfully with %s processor\n", paymentProcessor)

	// 			err = store.CreatePayment(domain.NewPayment(paymentEvent.CorrelationID, paymentEvent.Amount, paymentEvent.RequestedAt, paymentProcessor))
	// 			if err != nil {
	// 				if err == storage.ErrUniqueViolation {
	// 					log.Println("Payment already exists")
	// 					continue
	// 				}
	// 				log.Printf("failed to create payment: %s", err)
	// 				continue
	// 			}
	// 		}
	// 	}(i)
	// }

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
	// if 10 > paymentEvent.RetryCount {
	// 	paymentEvent.RetryCount++

	// 	log.Default().Printf("enqueue to retry payment with id %s numRetry %d: %v", paymentEvent.CorrelationID, paymentEvent.RetryCount, err)

	// 	// TODO: no retry delay?
	// 	//time.Sleep(20 * time.Millisecond)

	// 	err = event.RabbitMQClient.SendPaymentEventRetry(paymentEvent)
	// 	if err != nil {
	// 		return err
	// 	}
	// } else {
	// 	log.Default().Printf("payment with id %s numRetry %d: %v", paymentEvent.CorrelationID, paymentEvent.RetryCount, err)
	// }

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
