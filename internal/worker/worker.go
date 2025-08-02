package worker

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/Fuerback/rinha-2025/internal/domain"
	"github.com/Fuerback/rinha-2025/internal/storage"
)

var (
	MaxWorker = os.Getenv("MAX_WORKERS")
	MaxQueue  = os.Getenv("MAX_QUEUE")
)

// Job represents the job to be run
type Job struct {
	Payment domain.PaymentEvent
}

// A buffered channel that we can send work requests on.
var JobQueue chan Job

// InitJobQueue initializes the JobQueue with proper buffer size
func InitJobQueue(maxQueue int) {
	JobQueue = make(chan Job, maxQueue)
}

// Worker represents the worker that executes the job
type Worker struct {
	WorkerPool chan chan Job
	JobChannel chan Job
	quit       chan bool
	store      *storage.PaymentStore
}

func NewWorker(workerPool chan chan Job, store *storage.PaymentStore) Worker {
	return Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		quit:       make(chan bool),
		store:      store,
	}
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w Worker) Start() {
	go func() {
		for {
			// register the current worker into the worker queue.
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				// we have received a work request.
				err := w.sendToPaymentProcessor(job.Payment)
				if err != nil {
					// Determine if this is a retryable error and calculate backoff
					if w.shouldRetry(job.Payment, err) {
						retryCount := job.Payment.RetryCount + 1
						log.Printf("Payment processing failed, retrying (attempt %d/10): %v", retryCount, err)

						// Create retry job with incremented retry count
						retryJob := Job{Payment: domain.PaymentEvent{
							CorrelationID: job.Payment.CorrelationID,
							Amount:        job.Payment.Amount,
							RequestedAt:   job.Payment.RequestedAt,
							RetryCount:    retryCount,
						}}

						// Calculate exponential backoff with jitter
						backoffDelay := w.calculateBackoff(int(retryCount), err)

						// Add delay before retry to avoid overwhelming the system
						go func(retryJob Job, delay time.Duration) {
							time.Sleep(delay)
							select {
							case JobQueue <- retryJob:
								log.Printf("Queued retry for payment %s after %v delay", retryJob.Payment.CorrelationID, delay)
							default:
								log.Printf("Failed to queue retry for payment %s: queue full", retryJob.Payment.CorrelationID)
							}
						}(retryJob, backoffDelay)
					} else {
						log.Printf("Payment processing failed permanently: %v", err)
					}
				}

			case <-w.quit:
				// we have received a signal to stop
				return
			}
		}
	}()
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

func (w Worker) sendToPaymentProcessor(paymentEvent domain.PaymentEvent) error {
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
			return fmt.Errorf("error processing payment with fallback processor: %s", err)
		}
	}

	log.Printf("Payment processed successfully with %s processor\n", paymentProcessor)

	err = w.store.CreatePayment(domain.NewPayment(paymentEvent.CorrelationID, paymentEvent.Amount, paymentEvent.RequestedAt, paymentProcessor))
	if err != nil {
		if err == storage.ErrUniqueViolation {
			log.Println("Payment already exists")
			return nil
		}
		log.Printf("failed to create payment: %s", err)
		return nil
	}

	return nil
}

func (w Worker) shouldRetry(paymentEvent domain.PaymentEvent, err error) bool {
	// Implement logic to determine if the error is retryable
	// For example, you can check the error type or the retry count
	return paymentEvent.RetryCount < 10
}

func (w Worker) calculateBackoff(retryCount int, err error) time.Duration {
	// Implement exponential backoff with jitter
	// For example, you can use the following formula:
	// backoffDelay = (2 ^ retryCount) * 100ms + (random(0, 100ms))
	backoffDelay := time.Duration((2 << uint(retryCount)) * 50 * time.Millisecond)
	jitter := time.Duration(rand.Intn(50)) * time.Millisecond
	return backoffDelay + jitter
}

// ------

type Dispatcher struct {
	// A pool of workers channels that are registered with the dispatcher
	WorkerPool chan chan Job
	store      *storage.PaymentStore
	pool       chan chan Job
}

func NewDispatcher(maxWorkers int, store *storage.PaymentStore) *Dispatcher {
	pool := make(chan chan Job, maxWorkers)
	return &Dispatcher{WorkerPool: pool, store: store, pool: pool}
}

func (d *Dispatcher) Run() {
	// starting n number of workers
	for i := 0; i < 20; i++ { // TODO: make this configurable
		worker := NewWorker(d.pool, d.store) // TODO: is it should be d.WorkerPool or d.pool?
		worker.Start()
	}

	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	for job := range JobQueue {
		// a job request has been received
		go func(job Job) {
			// try to obtain a worker job channel that is available.
			// this will block until a worker is idle
			jobChannel := <-d.WorkerPool

			// dispatch the job to the worker job channel
			jobChannel <- job
		}(job)
	}
}
