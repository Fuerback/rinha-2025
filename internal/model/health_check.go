package model

import "time"

type HealthCheck struct {
	PreferredProcessor int
	LastCheckedAt      time.Time
}
