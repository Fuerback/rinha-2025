package model

import "time"

type HealthCheck struct {
	PreferredProcessor int
	MinResponseTime    int
	LastCheckedAt      time.Time
}
