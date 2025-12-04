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
	"os"
	"os/exec"
	"strings"
	"time"
)

// ContainerMonitor provides utilities for monitoring container status
type ContainerMonitor struct {
	containerName string
}

func NewContainerMonitor(containerName string) *ContainerMonitor {
	return &ContainerMonitor{
		containerName: containerName,
	}
}

// checks if the container is running using docker ps
func (c *ContainerMonitor) CheckContainerHealth(ctx context.Context) (bool, error) {
	cmd := exec.CommandContext(ctx, "docker", "ps", "--filter", fmt.Sprintf("name=%s", c.containerName), "--format", "{{.Status}}")
	output, err := cmd.Output()
	if err != nil {
		return false, err
	}

	status := strings.TrimSpace(string(output))
	if status == "" {
		return false, fmt.Errorf("container %s not found", c.containerName)
	}

	// Check if container is healthy/running
	if strings.Contains(status, "Up") || strings.Contains(status, "healthy") {
		return true, nil
	}

	return false, fmt.Errorf("container status: %s", status)
}

// gets the PID of the container's main process
func (c *ContainerMonitor) GetContainerPID(ctx context.Context) (int, error) {
	cmd := exec.CommandContext(ctx, "docker", "inspect", "-f", "{{.State.Pid}}", c.containerName)
	output, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	var pid int
	_, err = fmt.Sscanf(strings.TrimSpace(string(output)), "%d", &pid)
	if err != nil {
		return 0, err
	}

	return pid, nil
}

// checks if the container was killed by OOM killer
func (c *ContainerMonitor) CheckOOMKill(ctx context.Context) (bool, string, error) {
	pid, err := c.GetContainerPID(ctx)
	if err != nil {
		return false, "", err
	}

	// Check /proc/<pid>/status for OOM kill indicator
	statusFile := fmt.Sprintf("/proc/%d/status", pid)
	data, err := os.ReadFile(statusFile)
	if err != nil {
		return false, "", err
	}

	status := string(data)
	if strings.Contains(status, "oom_kill") {
		// Extract OOM kill count
		lines := strings.Split(status, "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "voluntary_ctxt_switches") {
				return true, line, nil
			}
		}
		return true, "OOM kill detected", nil
	}

	return false, "", nil
}

// WatchContainerStatus continuously monitors container status
func (c *ContainerMonitor) WatchContainerStatus(ctx context.Context, interval time.Duration, outChan chan<- DeathRattleEvent) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			healthy, err := c.CheckContainerHealth(ctx)
			if err != nil || !healthy {
				outChan <- DeathRattleEvent{
					Type:      DeathRattleTypeContainer,
					Message:   fmt.Sprintf("Container health check failed: %v", err),
					Timestamp: time.Now(),
					Severity:  "critical",
				}
			}

			oomKilled, msg, err := c.CheckOOMKill(ctx)
			if err == nil && oomKilled {
				outChan <- DeathRattleEvent{
					Type:      DeathRattleTypeContainer,
					Message:   fmt.Sprintf("OOM kill detected: %s", msg),
					Timestamp: time.Now(),
					Severity:  "critical",
				}
			}
		}
	}
}
