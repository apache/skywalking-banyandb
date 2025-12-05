// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.  See the License for the specific
// language governing permissions and limitations under the License.

// Package sidecar provides sidecar mode functionality for FODC, including service discovery and health endpoints.
package sidecar

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

// Constants for default values.
const (
	DefaultHealthPort = 17914
	EnvHealthPort     = "FODC_HEALTH_PORT"
	StatusStarting    = "STARTING"
	StatusServing     = "SERVING"
	StatusNotServing  = "NOT_SERVING"
)

// HealthStatus represents the health status of the sidecar.
type HealthStatus struct { //nolint:govet // fieldalignment: field order optimized for readability
	BanyanDB  *BanyanDBHealth        `json:"banyandb,omitempty"`
	Metrics   *MetricsHealth         `json:"metrics,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	Uptime    time.Duration          `json:"uptime"`
	Status    string                 `json:"status"`
	Version   string                 `json:"version,omitempty"`
}

// BanyanDBHealth represents the health of the monitored BanyanDB instance.
type BanyanDBHealth struct {
	LastCheck time.Time `json:"last_check,omitempty"`
	Error     string    `json:"error,omitempty"`
	Connected bool      `json:"connected"`
}

// MetricsHealth represents the health of metrics collection.
type MetricsHealth struct {
	LastSnapshotTime time.Time `json:"last_snapshot_time,omitempty"`
	TotalSnapshots   int       `json:"total_snapshots"`
	Errors           int       `json:"errors"`
}

// HealthServer provides HTTP health endpoints for the sidecar.
type HealthServer struct { //nolint:govet // fieldalignment: field order optimized for readability
	server    *http.Server
	status    *HealthStatus
	mu        sync.RWMutex
	startTime time.Time
	version   string
	port      int
}

// NewHealthServer creates a new health server.
func NewHealthServer(port int, version string) *HealthServer {
	if port == 0 {
		port = DefaultHealthPort
		if portStr := os.Getenv(EnvHealthPort); portStr != "" {
			if p, err := strconv.Atoi(portStr); err == nil && p > 0 {
				port = p
			}
		}
	}

	hs := &HealthServer{
		port:      port,
		startTime: time.Now(),
		version:   version,
		status: &HealthStatus{
			Status:    StatusStarting,
			Timestamp: time.Now(),
			Version:   version,
			Metadata:  make(map[string]interface{}),
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", hs.handleHealth)
	mux.HandleFunc("/healthz", hs.handleHealth)
	mux.HandleFunc("/ready", hs.handleReady)
	mux.HandleFunc("/live", hs.handleLiveness)

	hs.server = &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return hs
}

// Start starts the health server.
func (hs *HealthServer) Start() error {
	hs.mu.Lock()
	hs.status.Status = StatusServing
	hs.status.Timestamp = time.Now()
	hs.mu.Unlock()

	return hs.server.ListenAndServe()
}

// Stop stops the health server.
func (hs *HealthServer) Stop() error {
	return hs.server.Close()
}

// UpdateBanyanDBHealth updates the BanyanDB health status.
func (hs *HealthServer) UpdateBanyanDBHealth(connected bool, err error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.status.BanyanDB = &BanyanDBHealth{
		Connected: connected,
		LastCheck: time.Now(),
	}
	if err != nil {
		hs.status.BanyanDB.Error = err.Error()
	}
}

// UpdateMetricsHealth updates the metrics collection health status.
func (hs *HealthServer) UpdateMetricsHealth(totalSnapshots, errors int, lastSnapshotTime time.Time) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.status.Metrics = &MetricsHealth{
		TotalSnapshots:   totalSnapshots,
		LastSnapshotTime: lastSnapshotTime,
		Errors:           errors,
	}
}

// SetMetadata sets metadata for the health status.
func (hs *HealthServer) SetMetadata(key string, value interface{}) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	if hs.status.Metadata == nil {
		hs.status.Metadata = make(map[string]interface{})
	}
	hs.status.Metadata[key] = value
}

func (hs *HealthServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	hs.mu.RLock()
	status := *hs.status
	status.Uptime = time.Since(hs.startTime)
	status.Timestamp = time.Now()
	hs.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")

	httpStatus := http.StatusOK
	if status.BanyanDB != nil && !status.BanyanDB.Connected {
		httpStatus = http.StatusServiceUnavailable
	}

	w.WriteHeader(httpStatus)
	if err := json.NewEncoder(w).Encode(status); err != nil {
		// If JSON encoding fails, write a simple error response
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf("Error encoding health status: %v", err)))
	}
}

func (hs *HealthServer) handleReady(w http.ResponseWriter, _ *http.Request) {
	hs.mu.RLock()
	ready := hs.status.Status == StatusServing
	if hs.status.BanyanDB != nil {
		ready = ready && hs.status.BanyanDB.Connected
	}
	hs.mu.RUnlock()

	if ready {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("NOT READY"))
	}
}

func (hs *HealthServer) handleLiveness(w http.ResponseWriter, _ *http.Request) {
	hs.mu.RLock()
	alive := hs.status.Status != StatusNotServing
	hs.mu.RUnlock()

	if alive {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ALIVE"))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("NOT ALIVE"))
	}
}
