package event

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/Fuerback/rinha-2025/internal/domain"
	amqp "github.com/rabbitmq/amqp091-go"
)

const PaymentEventQueue = "payment_event"

func (r *RabbitMQ) SendPaymentEvent(paymentEvent domain.PaymentEvent) error {
	body, err := json.Marshal(paymentEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	err = r.Channel.Publish(
		"",                // exchange
		PaymentEventQueue, // routing key (queue name)
		false,             // mandatory
		false,             // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})

	if err != nil {
		return fmt.Errorf("failed to publish message: %v", err)
	}

	log.Printf("Payment event has been sent to RabbitMQ queue: %s", paymentEvent)

	return nil
}

func (r *RabbitMQ) ConsumePaymentEvent() (<-chan amqp.Delivery, error) {
	msgs, err := r.Channel.Consume(
		PaymentEventQueue, // queue name
		"",                // consumer
		true,              // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register a consumer: %v", err)
	}

	return msgs, nil
}
