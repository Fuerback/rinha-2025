package event

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/Fuerback/rinha-2025/internal/domain"
	"github.com/redis/go-redis/v9"
)

type RedisPaymentEvent struct {
	client *redis.Client
}

const PaymentRedisEventChannel = "payment_event"
const PaymentRedisEventRetryChannel = "payment_event_retry"

func NewRedisConnection() *RedisPaymentEvent {
	opt, err := redis.ParseURL(os.Getenv("REDIS_CONNECTION_URL"))
	if err != nil {
		panic(err)
	}

	return &RedisPaymentEvent{
		client: redis.NewClient(opt),
	}
}

func (r *RedisPaymentEvent) SendPaymentEvent(paymentEvent domain.PaymentEvent, channel string) error {
	body, err := json.Marshal(paymentEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	err = r.client.Publish(context.Background(), channel, string(body)).Err()
	if err != nil {
		return err
	}

	return nil
}

func (r *RedisPaymentEvent) Subscribe(channel string) <-chan *redis.Message {
	pubsub := r.client.Subscribe(context.Background(), channel)
	return pubsub.Channel()
}
