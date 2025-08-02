package worker

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	"github.com/Fuerback/rinha-2025/internal/domain"
	"github.com/Fuerback/rinha-2025/internal/storage"
)

type PaymentProcessorChan struct {
	store            *storage.PaymentStore
	processorCh      chan domain.PaymentEvent
	processorRetryCh chan domain.PaymentEvent
	maxRetries       uint
	retryDelay       time.Duration
}

func NewPaymentProcessor(store *storage.PaymentStore) *PaymentProcessorChan {
	return &PaymentProcessorChan{
		store:            store,
		processorCh:      make(chan domain.PaymentEvent, 20000),
		processorRetryCh: make(chan domain.PaymentEvent, 20000),
		maxRetries:       5,
		retryDelay:       50 * time.Millisecond,
	}
}

func (p *PaymentProcessorChan) AddPayment(paymentEvent domain.PaymentEvent) error {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	select {
	case p.processorCh <- paymentEvent:
		return nil
	case <-ctx.Done():
		return errors.New("payment queue full - try again later")
	}
}

func (p *PaymentProcessorChan) Init() {
	for range 20 {
		go p.startProcessor()
	}
	for range 20 {
		go p.startRetryProcessor()
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
				go func() {
					//time.Sleep(p.retryDelay)
					p.processorRetryCh <- paymentEvent
				}()
				// err = p.addPaymentToRetry(paymentEvent, err)
				// if err != nil {
				// 	log.Printf("Error adding payment to retry: %s", err)
				// 	continue
				// }
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

func (p *PaymentProcessorChan) startRetryProcessor() {
	for paymentEvent := range p.processorRetryCh {
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
				go func() {
					time.Sleep(p.retryDelay)
					p.processorRetryCh <- paymentEvent
				}()
				// err = p.addPaymentToRetry(paymentEvent, err)
				// if err != nil {
				// 	log.Printf("Error adding payment to retry: %s", err)
				// 	continue
				// }
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

		p.processorRetryCh <- paymentEvent
	} else {
		log.Default().Printf("payment with id %s numRetry %d: %v", paymentEvent.CorrelationID, paymentEvent.RetryCount, err)
		return nil
	}

	return nil
}
