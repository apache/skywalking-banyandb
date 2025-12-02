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

package detector

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

type DeathRattleType string

const (
	DeathRattleTypeFile      DeathRattleType = "file"
	DeathRattleTypeSignal    DeathRattleType = "signal"
	DeathRattleTypeHealth    DeathRattleType = "health_check"
	DeathRattleTypeContainer DeathRattleType = "container"
)

type DeathRattleEvent struct {
	Type      DeathRattleType
	Message   string
	Timestamp time.Time
	Severity  string
}

type DeathRattleDetector struct {
	filePath      string
	containerName string
	healthURL     string
	healthInterval time.Duration
	client        *http.Client
}

func NewDeathRattleDetector(filePath, containerName, healthURL string, healthInterval time.Duration) *DeathRattleDetector {
	return &DeathRattleDetector{
		filePath:       filePath,
		containerName: containerName,
		healthURL:      healthURL,
		healthInterval: healthInterval,
		client: &http.Client{
			Timeout: 3 * time.Second,
		},
	}
}

func (d *DeathRattleDetector) Start(ctx context.Context, outChan chan<- DeathRattleEvent) error {
	// Start file watcher
	go d.watchFile(ctx, outChan)

	// Start signal watcher
	go d.watchSignals(ctx, outChan)

	// Start health check watcher
	go d.watchHealthCheck(ctx, outChan)

	// Start container status watcher (if docker is available)
	// Note: This requires docker CLI to be available
	// go d.watchContainerStatus(ctx, outChan)

	<-ctx.Done()
	return nil
}

func (d *DeathRattleDetector) watchFile(ctx context.Context, outChan chan<- DeathRattleEvent) {
	// Watch the death rattle file path
	dir := filepath.Dir(d.filePath)
	if dir == "" || dir == "." {
		dir = "/tmp"
	}

	// Create directory if it doesn't exist
	if err := os.MkdirAll(dir, 0755); err != nil {
		return
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Check if death rattle file exists
			if _, err := os.Stat(d.filePath); err == nil {
				// File exists - read its contents
				content, err := os.ReadFile(d.filePath)
				message := fmt.Sprintf("Death rattle file detected at %s", d.filePath)
				if err == nil && len(content) > 0 {
					message = fmt.Sprintf("%s: %s", message, string(content))
				}

				outChan <- DeathRattleEvent{
					Type:      DeathRattleTypeFile,
					Message:   message,
					Timestamp: time.Now(),
					Severity:  "critical",
				}

				// Optionally remove the file after reading
				_ = os.Remove(d.filePath)
			}

			// Also check common death rattle locations
			commonPaths := []string{
				"/tmp/container-failing",
				"/dev/shm/death-rattle",
				"/tmp/banyandb-failing",
			}
			for _, path := range commonPaths {
				if _, err := os.Stat(path); err == nil {
					outChan <- DeathRattleEvent{
						Type:      DeathRattleTypeFile,
						Message:   fmt.Sprintf("Death rattle file detected at %s", path),
						Timestamp: time.Now(),
						Severity:  "critical",
					}
				}
			}
		}
	}
}

func (d *DeathRattleDetector) watchSignals(ctx context.Context, outChan chan<- DeathRattleEvent) {
	// Note: Signal watching is handled by the main process
	// This function monitors for signal-related death rattles via other means
	// For example, checking if the container's main process has received signals
	
	// Check for signal-related files or indicators
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Check for signal-related indicators in the container
			// This could be extended to check Docker events or process status
			// For now, we rely on file-based and health check detection
		}
	}
}

func (d *DeathRattleDetector) watchHealthCheck(ctx context.Context, outChan chan<- DeathRattleEvent) {
	ticker := time.NewTicker(d.healthInterval)
	defer ticker.Stop()

	failureCount := 0
	const maxFailures = 3

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			req, err := http.NewRequestWithContext(ctx, "GET", d.healthURL, nil)
			if err != nil {
				failureCount++
				if failureCount >= maxFailures {
					outChan <- DeathRattleEvent{
						Type:      DeathRattleTypeHealth,
						Message:   fmt.Sprintf("Health check failed %d times: %v", failureCount, err),
						Timestamp: time.Now(),
						Severity:  "critical",
					}
					failureCount = 0 // Reset after alerting
				}
				continue
			}

			resp, err := d.client.Do(req)
			if err != nil {
				failureCount++
				if failureCount >= maxFailures {
					outChan <- DeathRattleEvent{
						Type:      DeathRattleTypeHealth,
						Message:   fmt.Sprintf("Health check failed %d times: %v", failureCount, err),
						Timestamp: time.Now(),
						Severity:  "critical",
					}
					failureCount = 0
				}
				continue
			}
			resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				failureCount++
				if failureCount >= maxFailures {
					outChan <- DeathRattleEvent{
						Type:      DeathRattleTypeHealth,
						Message:   fmt.Sprintf("Health check returned non-200 status: %d (failed %d times)", resp.StatusCode, failureCount),
						Timestamp: time.Now(),
						Severity:  "critical",
					}
					failureCount = 0
				}
				continue
			}

			// Health check passed, reset failure count
			if failureCount > 0 {
				failureCount = 0
			}
		}
	}
}

func (d *DeathRattleDetector) watchContainerStatus(ctx context.Context, outChan chan<- DeathRattleEvent) {
	// Check container status using Docker API or filesystem
	// This is a simplified version - in production, you might use Docker API
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Check if container PID file exists (common pattern)
			pidFile := fmt.Sprintf("/proc/1/status")
			if _, err := os.Stat(pidFile); err != nil {
				// Try to read container status from /proc
				if _, err := os.ReadFile(pidFile); err != nil {
					outChan <- DeathRattleEvent{
						Type:      DeathRattleTypeContainer,
						Message:   fmt.Sprintf("Cannot access container process status: %v", err),
						Timestamp: time.Now(),
						Severity:  "warning",
					}
				}
			}

			// Check for OOM killer events
			oomPath := "/sys/fs/cgroup/memory/memory.oom_control"
			if _, err := os.Stat(oomPath); err == nil {
				// Check if OOM kill happened
				oomKillPath := "/sys/fs/cgroup/memory/memory.oom_kill"
				if data, err := os.ReadFile(oomKillPath); err == nil {
					if len(data) > 0 && string(data) != "0\n" {
						outChan <- DeathRattleEvent{
							Type:      DeathRattleTypeContainer,
							Message:   fmt.Sprintf("OOM kill detected: %s", string(data)),
							Timestamp: time.Now(),
							Severity:  "critical",
						}
					}
				}
			}
		}
	}
}

// TriggerDeathRattle creates a death rattle file for testing
func TriggerDeathRattle(filePath, message string) error {
	if message == "" {
		message = fmt.Sprintf("Death rattle triggered at %s", time.Now().Format(time.RFC3339))
	}
	return os.WriteFile(filePath, []byte(message), 0644)
}

