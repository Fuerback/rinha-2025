package event

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/Fuerback/rinha-2025/internal/domain"
	amqp "github.com/rabbitmq/amqp091-go"
)

var RabbitMQClient *RabbitMQ

type RabbitMQ struct {
	Conn    *amqp.Connection
	Channel *amqp.Channel
}

const PaymentEventQueue = "payment_event"
const PaymentEventRetryQueue = "payment_event_retry"

func NewRabbitMQConnection() {
	conn, err := amqp.Dial(os.Getenv("RABBITMQ_CONNECTION_URL"))
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a RabbitMQ channel: %s", err)
	}

	_, err = ch.QueueDeclare(
		PaymentEventQueue, // queue name
		true,              // durable
		false,             // delete when unused
		false,             // exclusive
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %s", err)
	}

	_, err = ch.QueueDeclare(
		PaymentEventRetryQueue, // queue name
		true,                   // durable
		false,                  // delete when unused
		false,                  // exclusive
		false,                  // no-wait
		nil,                    // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a retry queue: %s", err)
	}

	RabbitMQClient = &RabbitMQ{
		Conn:    conn,
		Channel: ch,
	}
}

func (r *RabbitMQ) Close() {
	r.Conn.Close()
	r.Channel.Close()
}

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

	log.Printf("Payment event has been sent to RabbitMQ queue: %+v", paymentEvent)

	return nil
}

func (r *RabbitMQ) SendPaymentEventRetry(paymentEvent domain.PaymentEvent) error {
	body, err := json.Marshal(paymentEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	err = r.Channel.Publish(
		"",                     // exchange
		PaymentEventRetryQueue, // routing key (queue name)
		false,                  // mandatory
		false,                  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})

	if err != nil {
		return fmt.Errorf("failed to publish message: %v", err)
	}

	log.Printf("Payment event has been sent to RabbitMQ queue: %+v", paymentEvent)

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

func (r *RabbitMQ) ConsumePaymentEventRetry() (<-chan amqp.Delivery, error) {
	msgs, err := r.Channel.Consume(
		PaymentEventRetryQueue, // queue name
		"",                     // consumer
		true,                   // auto-ack
		false,                  // exclusive
		false,                  // no-local
		false,                  // no-wait
		nil,                    // args
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register a consumer: %v", err)
	}

	return msgs, nil
}
