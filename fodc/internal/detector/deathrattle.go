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
	"strconv"
	"strings"
	"sync"
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
	filePath       string
	containerName  string
	healthURL      string
	healthInterval time.Duration
	client         *http.Client
	wg             sync.WaitGroup
	errChan        chan error
}

func NewDeathRattleDetector(filePath, containerName, healthURL string, healthInterval time.Duration) *DeathRattleDetector {
	return &DeathRattleDetector{
		filePath:       filePath,
		containerName:  containerName,
		healthURL:      healthURL,
		healthInterval: healthInterval,
		client: &http.Client{
			Timeout: 3 * time.Second,
		},
		errChan: make(chan error, 4), // Buffer for 4 goroutines
	}
}

func (d *DeathRattleDetector) Start(ctx context.Context, outChan chan<- DeathRattleEvent) error {
	// Start file watcher with error handling
	if err := d.startFileWatcher(ctx, outChan); err != nil {
		return fmt.Errorf("failed to start file watcher: %w", err)
	}

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				d.errChan <- fmt.Errorf("watchSignals panicked: %v", r)
			}
		}()
		d.watchSignals(ctx, outChan)
	}()

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				d.errChan <- fmt.Errorf("watchHealthCheck panicked: %v", r)
			}
		}()
		d.watchHealthCheck(ctx, outChan)
	}()

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				d.errChan <- fmt.Errorf("watchContainerStatus panicked: %v", r)
			}
		}()
		d.watchContainerStatus(ctx, outChan)
	}()

	// Monitor for goroutine errors
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-d.errChan:
				if err != nil {
					outChan <- DeathRattleEvent{
						Type:      DeathRattleTypeSignal,
						Message:   fmt.Sprintf("Monitor error: %v", err),
						Timestamp: time.Now(),
						Severity:  "warning",
					}
				}
			}
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()

	// Wait for all goroutines to finish cleanup
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()

	// Wait with timeout to avoid hanging indefinitely
	select {
	case <-done:
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout waiting for goroutines to shutdown: %w", ctx.Err())
	}
}

// initializes the file watcher and returns any startup errors
func (d *DeathRattleDetector) startFileWatcher(ctx context.Context, outChan chan<- DeathRattleEvent) error {
	dir := filepath.Dir(d.filePath)
	if dir == "" || dir == "." {
		dir = "/tmp"
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				d.errChan <- fmt.Errorf("watchFile panicked: %v", r)
			}
		}()
		d.watchFile(ctx, outChan)
	}()

	return nil
}

func (d *DeathRattleDetector) watchFile(ctx context.Context, outChan chan<- DeathRattleEvent) {

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
	// Monitor for signal-related death rattles by checking process status
	// Since this runs as a sidecar, we can't directly catch signals sent to the main process
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// Track previous signal counts to detect changes
	prevSelfSigPnd := uint64(0)
	prevSelfShdPnd := uint64(0)
	prevPid1SigPnd := uint64(0)
	prevPid1ShdPnd := uint64(0)

	// Common signal-related file paths
	signalFiles := []string{
		"/tmp/sigterm-received",
		"/tmp/sigkill-received",
		"/dev/shm/signal-received",
		"/tmp/termination-signal",
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, path := range signalFiles {
				if _, err := os.Stat(path); err == nil {
					content, _ := os.ReadFile(path)
					message := fmt.Sprintf("Signal-related file detected at %s", path)
					if len(content) > 0 {
						message = fmt.Sprintf("%s: %s", message, strings.TrimSpace(string(content)))
					}
					outChan <- DeathRattleEvent{
						Type:      DeathRattleTypeSignal,
						Message:   message,
						Timestamp: time.Now(),
						Severity:  "critical",
					}
					_ = os.Remove(path)
				}
			}

			// Check /proc/self/status for pending signals
			if sigPnd, shdPnd, err := d.parseSignalStatus("/proc/self/status"); err == nil {
				if (sigPnd > prevSelfSigPnd || shdPnd > prevSelfShdPnd) && (prevSelfSigPnd > 0 || prevSelfShdPnd > 0) {
					outChan <- DeathRattleEvent{
						Type:      DeathRattleTypeSignal,
						Message:   fmt.Sprintf("Pending signals detected in self process: SigPnd=%d, ShdPnd=%d", sigPnd, shdPnd),
						Timestamp: time.Now(),
						Severity:  "warning",
					}
				}
				prevSelfSigPnd = sigPnd
				prevSelfShdPnd = shdPnd
			}

			// Check /proc/1/status for main container process signals
			if sigPnd, shdPnd, err := d.parseSignalStatus("/proc/1/status"); err == nil {
				if (sigPnd > prevPid1SigPnd || shdPnd > prevPid1ShdPnd) && (prevPid1SigPnd > 0 || prevPid1ShdPnd > 0) {
					outChan <- DeathRattleEvent{
						Type:      DeathRattleTypeSignal,
						Message:   fmt.Sprintf("Pending signals detected in main process (PID 1): SigPnd=%d, ShdPnd=%d", sigPnd, shdPnd),
						Timestamp: time.Now(),
						Severity:  "critical",
					}
				}
				// Check if process state indicates termination
				if state, err := d.getProcessState("/proc/1/status"); err == nil {
					if state == "Z" { // Zombie state
						outChan <- DeathRattleEvent{
							Type:      DeathRattleTypeSignal,
							Message:   "Main process (PID 1) is in zombie state - likely terminated",
							Timestamp: time.Now(),
							Severity:  "critical",
						}
					}
				}
				prevPid1SigPnd = sigPnd
				prevPid1ShdPnd = shdPnd
			}
		}
	}
}

// extracts SigPnd and ShdPnd values from /proc/pid/status
func (d *DeathRattleDetector) parseSignalStatus(statusPath string) (sigPnd, shdPnd uint64, err error) {
	data, err := os.ReadFile(statusPath)
	if err != nil {
		return 0, 0, err
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "SigPnd:":
			if val, err := strconv.ParseUint(fields[1], 16, 64); err == nil {
				sigPnd = val
			}
		case "ShdPnd:":
			if val, err := strconv.ParseUint(fields[1], 16, 64); err == nil {
				shdPnd = val
			}
		}
	}
	return sigPnd, shdPnd, nil
}

// extracts the process state from /proc/pid/status
func (d *DeathRattleDetector) getProcessState(statusPath string) (string, error) {
	data, err := os.ReadFile(statusPath)
	if err != nil {
		return "", err
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "State:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				return fields[1], nil
			}
		}
	}
	return "", fmt.Errorf("state not found")
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
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Track previous OOM kill count to detect new kills
	var prevOOMKillCount uint64
	var prevProcessState string
	firstCheck := true

	// Use ContainerMonitor if container name is provided
	var containerMonitor *ContainerMonitor
	if d.containerName != "" {
		containerMonitor = NewContainerMonitor(d.containerName)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Try Docker-based monitoring first if container name is available
			if containerMonitor != nil {
				healthy, err := containerMonitor.CheckContainerHealth(ctx)
				if err != nil {
					outChan <- DeathRattleEvent{
						Type:      DeathRattleTypeContainer,
						Message:   fmt.Sprintf("Container health check failed: %v", err),
						Timestamp: time.Now(),
						Severity:  "critical",
					}
				} else if !healthy {
					outChan <- DeathRattleEvent{
						Type:      DeathRattleTypeContainer,
						Message:   "Container is not healthy or not running",
						Timestamp: time.Now(),
						Severity:  "critical",
					}
				}

				// Check for OOM kill using ContainerMonitor
				oomKilled, msg, err := containerMonitor.CheckOOMKill(ctx)
				if err == nil && oomKilled {
					outChan <- DeathRattleEvent{
						Type:      DeathRattleTypeContainer,
						Message:   fmt.Sprintf("OOM kill detected: %s", msg),
						Timestamp: time.Now(),
						Severity:  "critical",
					}
				}
			}

			// Fallback to filesystem-based checks
			d.checkProcessStatus(ctx, outChan, &prevProcessState, firstCheck)
			d.checkOOMKillFilesystem(ctx, outChan, &prevOOMKillCount, firstCheck)

			firstCheck = false
		}
	}
}

// checkProcessStatus checks the main process status via /proc filesystem
func (d *DeathRattleDetector) checkProcessStatus(ctx context.Context, outChan chan<- DeathRattleEvent, prevState *string, firstCheck bool) {
	pidFile := "/proc/1/status"
	data, err := os.ReadFile(pidFile)
	if err != nil {
		// Only alert if this is not the first check (to avoid false positives on startup)
		if !firstCheck {
			outChan <- DeathRattleEvent{
				Type:      DeathRattleTypeContainer,
				Message:   fmt.Sprintf("Cannot access main process status (PID 1): %v", err),
				Timestamp: time.Now(),
				Severity:  "critical",
			}
		}
		return
	}

	// Extract process state
	state := d.getProcessStateFromData(string(data))
	if state == "" {
		return
	}

	// Check for zombie state or other problematic states
	if state == "Z" {
		outChan <- DeathRattleEvent{
			Type:      DeathRattleTypeContainer,
			Message:   "Main process (PID 1) is in zombie state - container may be terminating",
			Timestamp: time.Now(),
			Severity:  "critical",
		}
	} else if state == "T" && *prevState != "T" {
		// Process stopped (traced or stopped by signal)
		outChan <- DeathRattleEvent{
			Type:      DeathRattleTypeContainer,
			Message:   "Main process (PID 1) is stopped",
			Timestamp: time.Now(),
			Severity:  "warning",
		}
	}

	*prevState = state
}

// checkOOMKillFilesystem checks for OOM kills using cgroup filesystem
func (d *DeathRattleDetector) checkOOMKillFilesystem(ctx context.Context, outChan chan<- DeathRattleEvent, prevCount *uint64, firstCheck bool) {
	// Try cgroup v1 path
	oomKillPath := "/sys/fs/cgroup/memory/memory.oom_kill"
	if data, err := os.ReadFile(oomKillPath); err == nil {
		count, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
		if err == nil {
			if count > *prevCount && !firstCheck {
				outChan <- DeathRattleEvent{
					Type:      DeathRattleTypeContainer,
					Message:   fmt.Sprintf("OOM kill detected via cgroup v1 (total kills: %d)", count),
					Timestamp: time.Now(),
					Severity:  "critical",
				}
			}
			*prevCount = count
			return
		}
	}

	// Try cgroup v2 path
	oomKillPathV2 := "/sys/fs/cgroup/memory.events"
	if data, err := os.ReadFile(oomKillPathV2); err == nil {
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "oom_kill ") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					count, err := strconv.ParseUint(fields[1], 10, 64)
					if err == nil {
						if count > *prevCount && !firstCheck {
							outChan <- DeathRattleEvent{
								Type:      DeathRattleTypeContainer,
								Message:   fmt.Sprintf("OOM kill detected via cgroup v2 (total kills: %d)", count),
								Timestamp: time.Now(),
								Severity:  "critical",
							}
						}
						*prevCount = count
					}
					return
				}
			}
		}
	}

	// Check memory.oom_control for additional info
	oomControlPath := "/sys/fs/cgroup/memory/memory.oom_control"
	if data, err := os.ReadFile(oomControlPath); err == nil {
		// Look for oom_kill_disable flag
		if strings.Contains(string(data), "oom_kill_disable 1") {
			// OOM killer is disabled, which might indicate issues
			if !firstCheck {
				outChan <- DeathRattleEvent{
					Type:      DeathRattleTypeContainer,
					Message:   "OOM killer is disabled in cgroup - memory issues may not be handled properly",
					Timestamp: time.Now(),
					Severity:  "warning",
				}
			}
		}
	}
}

// getProcessStateFromData extracts process state from /proc/pid/status data
func (d *DeathRattleDetector) getProcessStateFromData(data string) string {
	lines := strings.Split(data, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "State:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				return fields[1]
			}
		}
	}
	return ""
}
