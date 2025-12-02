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

package sidecar

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"
)

const (
	// Default BanyanDB ports
	DefaultMetricsPort = 2121
	DefaultHTTPPort    = 17913

	// Environment variables for sidecar mode
	EnvBanyanDBHost       = "BANYANDB_HOST"
	EnvBanyanDBMetricsPort = "BANYANDB_METRICS_PORT"
	EnvBanyanDBHTTPPort   = "BANYANDB_HTTP_PORT"
	EnvPodName            = "POD_NAME"
	EnvPodNamespace       = "POD_NAMESPACE"
	EnvPodIP              = "POD_IP"
	EnvHostIP             = "HOST_IP"
)

// BanyanDBEndpoint represents discovered BanyanDB endpoints
type BanyanDBEndpoint struct {
	MetricsURL string
	HealthURL  string
	Host       string
	PodName    string
	PodIP      string
}

// DiscoverBanyanDB discovers the BanyanDB instance to monitor
// Priority:
// 1. Environment variables (BANYANDB_HOST, BANYANDB_METRICS_PORT, BANYANDB_HTTP_PORT)
// 2. Kubernetes service discovery (localhost in same pod)
// 3. Default localhost with standard ports
func DiscoverBanyanDB() (*BanyanDBEndpoint, error) {
	endpoint := &BanyanDBEndpoint{}

	// Get host from environment or use localhost
	host := os.Getenv(EnvBanyanDBHost)
	if host == "" {
		// In Kubernetes sidecar, use localhost (same pod network namespace)
		// In Docker, use the service name or localhost
		host = "localhost"
	}
	endpoint.Host = host

	// Get ports from environment or use defaults
	metricsPort := DefaultMetricsPort
	if portStr := os.Getenv(EnvBanyanDBMetricsPort); portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil {
			metricsPort = p
		}
	}

	httpPort := DefaultHTTPPort
	if portStr := os.Getenv(EnvBanyanDBHTTPPort); portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil {
			httpPort = p
		}
	}

	// Build URLs
	endpoint.MetricsURL = fmt.Sprintf("http://%s:%d/metrics", host, metricsPort)
	endpoint.HealthURL = fmt.Sprintf("http://%s:%d/api/healthz", host, httpPort)

	// Get pod information if available (for Kubernetes)
	endpoint.PodName = os.Getenv(EnvPodName)
	endpoint.PodIP = os.Getenv(EnvPodIP)

	return endpoint, nil
}

// VerifyEndpoint verifies that the discovered endpoint is accessible
func VerifyEndpoint(endpoint *BanyanDBEndpoint, timeout time.Duration) error {
	client := &http.Client{
		Timeout: timeout,
	}

	// Try health endpoint first (more reliable)
	resp, err := client.Get(endpoint.HealthURL)
	if err != nil {
		return fmt.Errorf("failed to connect to BanyanDB health endpoint at %s: %w", endpoint.HealthURL, err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("BanyanDB health endpoint returned non-200 status: %d", resp.StatusCode)
	}

	return nil
}

// IsKubernetes checks if running in Kubernetes environment
func IsKubernetes() bool {
	// Check for Kubernetes-specific environment variables
	return os.Getenv(EnvPodName) != "" || os.Getenv(EnvPodNamespace) != ""
}

// IsDocker checks if running in Docker environment
func IsDocker() bool {
	// Check for Docker-specific indicators
	if _, err := os.Stat("/.dockerenv"); err == nil {
		return true
	}
	// Check cgroup
	if data, err := os.ReadFile("/proc/self/cgroup"); err == nil {
		return contains(string(data), "docker")
	}
	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsMiddle(s, substr)))
}

func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// GetLocalIP gets the local IP address
func GetLocalIP() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "", err
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String(), nil
}

