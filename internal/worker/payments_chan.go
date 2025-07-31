package worker

import (
	"errors"
	"log"
	"os"
	"time"

	"github.com/Fuerback/rinha-2025/internal/domain"
	"github.com/Fuerback/rinha-2025/internal/event"
	"github.com/Fuerback/rinha-2025/internal/storage"
)

type PaymentProcessorChan struct {
	store       *storage.PaymentStore
	processorCh chan domain.PaymentEvent
	maxRetries  uint
	retryDelay  time.Duration
}

func NewPaymentProcessor(store *storage.PaymentStore) *PaymentProcessorChan {
	return &PaymentProcessorChan{
		store:       store,
		processorCh: make(chan domain.PaymentEvent, 20000),
		maxRetries:  3,
		retryDelay:  100 * time.Millisecond,
	}
}

func (p *PaymentProcessorChan) AddPayment(paymentEvent domain.PaymentEvent) error {
	select {
	case p.processorCh <- paymentEvent:
		return nil
	default:
		return errors.New("fail to add payment event to processor channel")
	}
}

func (p *PaymentProcessorChan) Init() {
	for range 15 {
		go p.startProcessor()
	}
}

func (p *PaymentProcessorChan) startProcessor() {
	for paymentEvent := range p.processorCh {
		body := requestBody{
			CorrelationID: paymentEvent.CorrelationID,
			Amount:        paymentEvent.Amount.String(),
			RequestedAt:   paymentEvent.RequestedAt,
		}

		paymentProcessor := domain.PaymentProcessorDefault
		err := tryProcessor(os.Getenv("PROCESSOR_DEFAULT_URL"), body, 200*time.Millisecond)
		if err != nil {
			log.Printf("Error processing payment with default processor: %s", err)
			paymentProcessor = domain.PaymentProcessorFallback
			err = tryProcessor(os.Getenv("PROCESSOR_FALLBACK_URL"), body, 200*time.Millisecond)
			if err != nil {
				log.Printf("Error processing payment with fallback processor: %s", err)
				err = p.addPaymentToRetry(paymentEvent, err)
				if err != nil {
					log.Printf("Error adding payment to retry: %s", err)
					continue
				}
			}
		}

		log.Printf("Payment processed successfully with %s processor\n", paymentProcessor)

		err = p.store.CreatePayment(domain.NewPayment(paymentEvent.CorrelationID, paymentEvent.Amount, paymentEvent.RequestedAt, paymentProcessor))
		if err != nil {
			if err == storage.ErrUniqueViolation {
				log.Println("Payment already exists")
				continue
			}
			log.Printf("failed to create payment: %s", err)
			continue
		}
	}
}

func (p *PaymentProcessorChan) addPaymentToRetry(paymentEvent domain.PaymentEvent, err error) error {
	if p.maxRetries > paymentEvent.RetryCount {
		paymentEvent.RetryCount++

		log.Default().Printf("enqueue to retry payment with id %s numRetry %d: %v", paymentEvent.CorrelationID, paymentEvent.RetryCount, err)

		time.Sleep(p.retryDelay)

		p.processorCh <- paymentEvent
	} else {
		err = event.RabbitMQClient.SendPaymentEvent(paymentEvent)
		if err != nil {
			return err
		}
	}

	return nil
}
