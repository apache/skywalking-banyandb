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

// checks the main process status via /proc filesystem
func (d *DeathRattleDetector) checkProcessStatus(ctx context.Context, outChan chan<- DeathRattleEvent, prevState *string, firstCheck bool) {
	// Check context cancellation before proceeding
	select {
	case <-ctx.Done():
		return
	default:
	}

	pidFile := "/proc/1/status"
	data, err := os.ReadFile(pidFile)
	if err != nil {
		// avoid false positives on startup
		if !firstCheck {
			// Check if process directory exists to distinguish between process gone vs permission error
			if _, statErr := os.Stat("/proc/1"); statErr != nil {
				outChan <- DeathRattleEvent{
					Type:      DeathRattleTypeContainer,
					Message:   "Main process (PID 1) no longer exists - container may have terminated",
					Timestamp: time.Now(),
					Severity:  "critical",
				}
			} else {
				outChan <- DeathRattleEvent{
					Type:      DeathRattleTypeContainer,
					Message:   fmt.Sprintf("Cannot access main process status (PID 1): %v", err),
					Timestamp: time.Now(),
					Severity:  "warning",
				}
			}
		}
		return
	}

	statusData := string(data)

	// Extract process state and additional information
	state := d.getProcessStateFromData(statusData)
	if state == "" {
		return
	}

	// Extract additional process information for better diagnostics
	ppid := d.extractFieldFromStatus(statusData, "PPid:")
	threads := d.extractFieldFromStatus(statusData, "Threads:")
	vmRSS := d.extractFieldFromStatus(statusData, "VmRSS:")

	// Check for zombie state or other problematic states
	if state == "Z" {
		message := "Main process (PID 1) is in zombie state - container may be terminating"
		if ppid != "" {
			message = fmt.Sprintf("%s (parent PID: %s)", message, ppid)
		}
		outChan <- DeathRattleEvent{
			Type:      DeathRattleTypeContainer,
			Message:   message,
			Timestamp: time.Now(),
			Severity:  "critical",
		}
	} else if state == "T" && *prevState != "T" {
		// Process stopped (traced or stopped by signal)
		message := "Main process (PID 1) is stopped (traced or stopped by signal)"
		if threads != "" {
			message = fmt.Sprintf("%s (threads: %s)", message, threads)
		}
		outChan <- DeathRattleEvent{
			Type:      DeathRattleTypeContainer,
			Message:   message,
			Timestamp: time.Now(),
			Severity:  "warning",
		}
	} else if state == "D" {
		// Uninterruptible sleep (usually I/O wait) - can indicate I/O problems
		if *prevState != "D" {
			message := "Main process (PID 1) is in uninterruptible sleep (D state) - possible I/O issue"
			if vmRSS != "" {
				message = fmt.Sprintf("%s (RSS: %s)", message, vmRSS)
			}
			outChan <- DeathRattleEvent{
				Type:      DeathRattleTypeContainer,
				Message:   message,
				Timestamp: time.Now(),
				Severity:  "warning",
			}
		}
	} else if state == "X" || state == "x" {
		// Dead process (shouldn't normally see this)
		outChan <- DeathRattleEvent{
			Type:      DeathRattleTypeContainer,
			Message:   "Main process (PID 1) is dead",
			Timestamp: time.Now(),
			Severity:  "critical",
		}
	}

	*prevState = state
}

// checks for OOM kills using cgroup filesystem
func (d *DeathRattleDetector) checkOOMKillFilesystem(ctx context.Context, outChan chan<- DeathRattleEvent, prevCount *uint64, firstCheck bool) {
	// Check context cancellation before proceeding
	select {
	case <-ctx.Done():
		return
	default:
	}

	// First, try to find the actual cgroup path dynamically
	cgroupPaths := d.findCgroupPaths()

	// Try cgroup v1 paths
	for _, basePath := range cgroupPaths.v1 {
		oomKillPath := filepath.Join(basePath, "memory.oom_kill")
		if count, err := d.readOOMKillCountV1(oomKillPath); err == nil {
			if count > *prevCount && !firstCheck {
				newKills := count - *prevCount
				outChan <- DeathRattleEvent{
					Type:      DeathRattleTypeContainer,
					Message:   fmt.Sprintf("OOM kill detected via cgroup v1 (new kills: %d, total: %d, path: %s)", newKills, count, basePath),
					Timestamp: time.Now(),
					Severity:  "critical",
				}
			}
			*prevCount = count

			// Also check memory pressure and oom_control
			d.checkMemoryPressureV1(outChan, basePath, firstCheck)
			return
		}
	}

	// Try cgroup v2 paths
	for _, basePath := range cgroupPaths.v2 {
		eventsPath := filepath.Join(basePath, "memory.events")
		if count, err := d.readOOMKillCountV2(eventsPath); err == nil {
			if count > *prevCount && !firstCheck {
				newKills := count - *prevCount
				outChan <- DeathRattleEvent{
					Type:      DeathRattleTypeContainer,
					Message:   fmt.Sprintf("OOM kill detected via cgroup v2 (new kills: %d, total: %d, path: %s)", newKills, count, basePath),
					Timestamp: time.Now(),
					Severity:  "critical",
				}
			}
			*prevCount = count

			// Also check memory pressure events
			d.checkMemoryPressureV2(outChan, basePath, firstCheck)
			return
		}
	}

	// Fallback to default paths if dynamic discovery failed
	if !firstCheck {
		d.checkOOMKillFallback(outChan, prevCount, firstCheck)
	}
}

// cgroupPaths holds discovered cgroup paths for v1 and v2
type cgroupPaths struct {
	v1 []string
	v2 []string
}

// findCgroupPaths discovers cgroup paths by reading /proc/self/cgroup
func (d *DeathRattleDetector) findCgroupPaths() cgroupPaths {
	paths := cgroupPaths{
		v1: []string{"/sys/fs/cgroup/memory"}, // Default fallback
		v2: []string{"/sys/fs/cgroup"},        // Default fallback
	}

	// Read /proc/self/cgroup to find actual cgroup paths
	cgroupData, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return paths
	}

	lines := strings.Split(string(cgroupData), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		fields := strings.Split(line, ":")
		if len(fields) < 3 {
			continue
		}

		subsystems := fields[1]
		cgroupPath := fields[2]

		// For cgroup v2, subsystems field is empty
		if subsystems == "" {
			// cgroup v2
			if cgroupPath != "/" {
				fullPath := filepath.Join("/sys/fs/cgroup", cgroupPath)
				paths.v2 = append(paths.v2, fullPath)
			} else {
				paths.v2 = append(paths.v2, "/sys/fs/cgroup")
			}
		} else if strings.Contains(subsystems, "memory") {
			// cgroup v1 with memory subsystem
			if cgroupPath != "/" {
				fullPath := filepath.Join("/sys/fs/cgroup/memory", cgroupPath)
				paths.v1 = append(paths.v1, fullPath)
			} else {
				paths.v1 = append(paths.v1, "/sys/fs/cgroup/memory")
			}
		}
	}

	return paths
}

// reads OOM kill count from cgroup v1 memory.oom_kill file
func (d *DeathRattleDetector) readOOMKillCountV1(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
}

// reads OOM kill count from cgroup v2 memory.events file
func (d *DeathRattleDetector) readOOMKillCountV2(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "oom_kill ") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				return strconv.ParseUint(fields[1], 10, 64)
			}
		}
	}
	return 0, fmt.Errorf("oom_kill field not found")
}

// checks memory pressure indicators in cgroup v1
func (d *DeathRattleDetector) checkMemoryPressureV1(outChan chan<- DeathRattleEvent, basePath string, firstCheck bool) {
	if firstCheck {
		return
	}

	// Check memory.oom_control for OOM killer status
	oomControlPath := filepath.Join(basePath, "memory.oom_control")
	if data, err := os.ReadFile(oomControlPath); err == nil {
		content := string(data)
		if strings.Contains(content, "oom_kill_disable 1") {
			outChan <- DeathRattleEvent{
				Type:      DeathRattleTypeContainer,
				Message:   fmt.Sprintf("OOM killer is disabled in cgroup v1 (path: %s) - memory issues may not be handled properly", basePath),
				Timestamp: time.Now(),
				Severity:  "warning",
			}
		}
	}

	// Check memory.usage_in_bytes for high memory usage
	usagePath := filepath.Join(basePath, "memory.usage_in_bytes")
	if usageData, err := os.ReadFile(usagePath); err == nil {
		usage, err := strconv.ParseUint(strings.TrimSpace(string(usageData)), 10, 64)
		if err == nil {
			limitPath := filepath.Join(basePath, "memory.limit_in_bytes")
			if limitData, err := os.ReadFile(limitPath); err == nil {
				limit, err := strconv.ParseUint(strings.TrimSpace(string(limitData)), 10, 64)
				if err == nil && limit > 0 {
					usagePercent := float64(usage) * 100 / float64(limit)
					if usagePercent > 90 {
						outChan <- DeathRattleEvent{
							Type:      DeathRattleTypeContainer,
							Message:   fmt.Sprintf("High memory usage detected: %.1f%% (usage: %d, limit: %d)", usagePercent, usage, limit),
							Timestamp: time.Now(),
							Severity:  "warning",
						}
					}
				}
			}
		}
	}
}

// checks memory pressure indicators in cgroup v2
func (d *DeathRattleDetector) checkMemoryPressureV2(outChan chan<- DeathRattleEvent, basePath string, firstCheck bool) {
	if firstCheck {
		return
	}

	// Check memory.events for pressure events
	eventsPath := filepath.Join(basePath, "memory.events")
	if data, err := os.ReadFile(eventsPath); err == nil {
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				eventName := fields[0]
				count, err := strconv.ParseUint(fields[1], 10, 64)
				if err == nil && count > 0 {
					switch eventName {
					case "high":
						outChan <- DeathRattleEvent{
							Type:      DeathRattleTypeContainer,
							Message:   fmt.Sprintf("Memory pressure high event detected (count: %d)", count),
							Timestamp: time.Now(),
							Severity:  "warning",
						}
					case "critical":
						outChan <- DeathRattleEvent{
							Type:      DeathRattleTypeContainer,
							Message:   fmt.Sprintf("Memory pressure critical event detected (count: %d)", count),
							Timestamp: time.Now(),
							Severity:  "critical",
						}
					}
				}
			}
		}
	}

	// Check memory.current and memory.max for usage percentage
	currentPath := filepath.Join(basePath, "memory.current")
	maxPath := filepath.Join(basePath, "memory.max")
	if currentData, err := os.ReadFile(currentPath); err == nil {
		current, err := strconv.ParseUint(strings.TrimSpace(string(currentData)), 10, 64)
		if err == nil {
			if maxData, err := os.ReadFile(maxPath); err == nil {
				maxStr := strings.TrimSpace(string(maxData))
				if maxStr == "max" {
					return // Unlimited memory
				}
				max, err := strconv.ParseUint(maxStr, 10, 64)
				if err == nil && max > 0 {
					usagePercent := float64(current) * 100 / float64(max)
					if usagePercent > 90 {
						outChan <- DeathRattleEvent{
							Type:      DeathRattleTypeContainer,
							Message:   fmt.Sprintf("High memory usage detected: %.1f%% (usage: %d, limit: %d)", usagePercent, current, max),
							Timestamp: time.Now(),
							Severity:  "warning",
						}
					}
				}
			}
		}
	}
}

// uses default paths when dynamic discovery fails
func (d *DeathRattleDetector) checkOOMKillFallback(outChan chan<- DeathRattleEvent, prevCount *uint64, firstCheck bool) {
	// Try default cgroup v1 path
	oomKillPath := "/sys/fs/cgroup/memory/memory.oom_kill"
	if count, err := d.readOOMKillCountV1(oomKillPath); err == nil {
		if count > *prevCount && !firstCheck {
			newKills := count - *prevCount
			outChan <- DeathRattleEvent{
				Type:      DeathRattleTypeContainer,
				Message:   fmt.Sprintf("OOM kill detected via cgroup v1 fallback (new kills: %d, total: %d)", newKills, count),
				Timestamp: time.Now(),
				Severity:  "critical",
			}
		}
		*prevCount = count
		return
	}

	// Try default cgroup v2 path
	eventsPath := "/sys/fs/cgroup/memory.events"
	if count, err := d.readOOMKillCountV2(eventsPath); err == nil {
		if count > *prevCount && !firstCheck {
			newKills := count - *prevCount
			outChan <- DeathRattleEvent{
				Type:      DeathRattleTypeContainer,
				Message:   fmt.Sprintf("OOM kill detected via cgroup v2 fallback (new kills: %d, total: %d)", newKills, count),
				Timestamp: time.Now(),
				Severity:  "critical",
			}
		}
		*prevCount = count
		return
	}
}

// extracts process state from /proc/pid/status data
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

// extracts a field value from /proc/pid/status data
func (d *DeathRattleDetector) extractFieldFromStatus(data, fieldName string) string {
	lines := strings.Split(data, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, fieldName) {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				return fields[1]
			}
		}
	}
	return ""
}
