package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/Fuerback/rinha-2025/internal/storage"
)

type HealthCheckWorker struct {
	store                storage.PaymentStore
	client               *http.Client
	defaultProcessorURL  string
	fallbackProcessorURL string
	ctx                  context.Context
	cancel               context.CancelFunc
	wg                   sync.WaitGroup
}

type healthCheckResponse struct {
	Failing         bool `json:"failing"`
	MinResponseTime int  `json:"minResponseTime"`
}

var errHealthCheckFailedTooManyRequests = errors.New("health check failed with too many requests")

func NewHealthCheckWorker(store storage.PaymentStore) *HealthCheckWorker {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 50,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		DisableKeepAlives:   false,
	}

	return &HealthCheckWorker{
		store:                store,
		client:               &http.Client{Timeout: 5 * time.Second, Transport: transport},
		defaultProcessorURL:  os.Getenv("PROCESSOR_DEFAULT_URL"),
		fallbackProcessorURL: os.Getenv("PROCESSOR_FALLBACK_URL"),
	}
}

func (h *HealthCheckWorker) Start() {
	h.ctx, h.cancel = context.WithCancel(context.Background())
	h.wg.Add(1)
	go h.runHealthCheckLoop()
}

func (h *HealthCheckWorker) Stop() {
	if h.cancel != nil {
		h.cancel()
	}
	h.wg.Wait()
}

func (h *HealthCheckWorker) runHealthCheckLoop() {
	defer h.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.performHealthCheck()
		}
	}
}

func (h *HealthCheckWorker) performHealthCheck() {
	// Get current health check status
	currentHealthCheckResp, err := h.store.GetHealthCheck()
	if err != nil {
		slog.Error("Failed to get current health check", "error", err)
		return
	}

	// Check if LastCheckedAt was 5 seconds ago or before
	now := time.Now()
	if now.Sub(currentHealthCheckResp.LastCheckedAt) <= 5*time.Second {
		return
	}

	// Check health of both processors
	defaultHealthCheckResp, errDefault := h.checkProcessorHealth(h.defaultProcessorURL)
	fallbackHealthCheckResp, errFallback := h.checkProcessorHealth(h.fallbackProcessorURL)

	if errDefault != nil && errFallback != nil {
		//slog.Error("Failed to check processor health", "errorDefault", errDefault, "errorFallback", errFallback)
		return
	}

	isDefaultHealthy := !defaultHealthCheckResp.Failing
	isFallbackHealthy := !fallbackHealthCheckResp.Failing

	// Determine the best processor
	var bestProcessor int
	if isDefaultHealthy && isFallbackHealthy {
		if defaultHealthCheckResp.MinResponseTime < fallbackHealthCheckResp.MinResponseTime {
			bestProcessor = 0
		} else {
			bestProcessor = 1
		}
	} else if isDefaultHealthy {
		// Only default is healthy
		bestProcessor = 0
	} else if isFallbackHealthy {
		// Only fallback is healthy
		bestProcessor = 1
	} else {
		// Neither is healthy, keep current preference
		slog.Warn("Neither processor is healthy")
		bestProcessor = -1
	}

	if bestProcessor != currentHealthCheckResp.PreferredProcessor {
		// Update health check with the best processor
		err = h.store.UpdateHealthCheck(bestProcessor)
		if err != nil {
			slog.Error("Failed to update health check", "error", err, "bestProcessor", bestProcessor)
			return
		}
	}
}

func (h *HealthCheckWorker) checkProcessorHealth(processorURL string) (healthCheckResponse, error) {
	healthURL := processorURL + "/payments/service-health"
	req, err := http.NewRequest("GET", healthURL, nil)
	if err != nil {
		return healthCheckResponse{}, fmt.Errorf("failed to create health check request: %s", err)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return healthCheckResponse{}, fmt.Errorf("health check request failed: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		var healthCheckResp healthCheckResponse
		if err := json.NewDecoder(resp.Body).Decode(&healthCheckResp); err != nil {
			return healthCheckResponse{}, fmt.Errorf("failed to decode health check response: %s", err)
		}

		return healthCheckResp, nil
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		return healthCheckResponse{}, errHealthCheckFailedTooManyRequests
	}

	return healthCheckResponse{
		Failing:         true,
		MinResponseTime: 0,
	}, nil
}
