package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"

	"github.com/Fuerback/rinha-2025/internal/domain"
	"github.com/Fuerback/rinha-2025/internal/event"
	"github.com/Fuerback/rinha-2025/internal/storage"
)

func PaymentProcessor(store *storage.PaymentStore) {
	msgs, err := event.RabbitMQClient.ConsumePaymentEvent()

	if err != nil {
		log.Fatalf("Failed to consume RabbitMQ queue: %s", err)
		return
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			var paymentEvent domain.PaymentEvent
			err := json.Unmarshal(d.Body, &paymentEvent)
			if err != nil {
				log.Printf("Error reading payment event (please check the JSON format): %s", err)
				continue
			}

			fmt.Printf("Received a payment event: Correlation ID = %s, Amount = %s\n", paymentEvent.CorrelationID, paymentEvent.Amount)

			randomNumber := rand.Intn(1000)
			paymentProcessor := domain.PaymentProcessorDefault
			if randomNumber%2 == 0 {
				paymentProcessor = domain.PaymentProcessorFallback
			}

			store.UpdatePaymentStatus(paymentEvent.CorrelationID, domain.PaymentStatusSuccess, paymentProcessor)
		}
	}()

	<-forever
}
