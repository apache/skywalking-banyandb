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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package propertyrepair package provides utilities for property repair performance testing in BanyanDB.
package propertyrepair

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
)

// Constants for property repair performance testing.
const (
	DataSize     = 2048 // 2KB per property
	LiaisonAddr  = "localhost:17912"
	Concurrency  = 6
	GroupName    = "perf-test-group"
	PropertyName = "perf-test-property"
)

// PrometheusEndpoints defines the prometheus endpoints for data nodes.
var PrometheusEndpoints = []string{
	"http://localhost:2122/metrics", // data-node-1
	"http://localhost:2123/metrics", // data-node-2
	"http://localhost:2124/metrics", // data-node-3
}

// NodeMetrics represents the metrics for a data node.
type NodeMetrics struct {
	LastScrapeTime        time.Time
	NodeName              string
	ErrorMessage          string
	TotalPropagationCount int64
	RepairSuccessCount    int64
	IsHealthy             bool
}

// GenerateLargeData creates a string of specified size filled with random characters.
func GenerateLargeData(size int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	// Generate some random bytes
	randomBytes := make([]byte, 32)
	_, err := rand.Read(randomBytes)
	if err != nil {
		// Fallback to timestamp-based data
		baseData := fmt.Sprintf("timestamp-%d-", time.Now().UnixNano())
		repeats := size / len(baseData)
		if repeats == 0 {
			repeats = 1
		}
		return strings.Repeat(baseData, repeats)[:size]
	}

	// Create base string from random bytes
	var baseBuilder strings.Builder
	for _, b := range randomBytes {
		baseBuilder.WriteByte(charset[b%byte(len(charset))])
	}
	baseData := baseBuilder.String()

	// Repeat to reach desired size
	repeats := (size / len(baseData)) + 1
	result := strings.Repeat(baseData, repeats)

	if len(result) > size {
		return result[:size]
	}
	return result
}

// FormatDuration formats a duration to a human-readable string.
func FormatDuration(duration time.Duration) string {
	if duration < time.Second {
		return fmt.Sprintf("%dms", duration.Milliseconds())
	}
	if duration < time.Minute {
		return fmt.Sprintf("%.1fs", duration.Seconds())
	}
	return fmt.Sprintf("%.1fm", duration.Minutes())
}

// FormatThroughput calculates and formats throughput.
func FormatThroughput(count int64, duration time.Duration) string {
	if duration == 0 {
		return "N/A"
	}
	throughput := float64(count) / duration.Seconds()
	return fmt.Sprintf("%.1f/s", throughput)
}

// CreateGroup creates a property group with specified parameters.
func CreateGroup(ctx context.Context, groupClient databasev1.GroupRegistryServiceClient, replicaNum uint32) {
	fmt.Printf("Creating group %s with %d replicas...\n", GroupName, replicaNum)
	_, err := groupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
		Group: &commonv1.Group{
			Metadata: &commonv1.Metadata{
				Name: GroupName,
			},
			Catalog: commonv1.Catalog_CATALOG_PROPERTY,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum: 1,
				Replicas: replicaNum,
			},
		},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// UpdateGroupReplicas updates the replica number of an existing group.
func UpdateGroupReplicas(ctx context.Context, groupClient databasev1.GroupRegistryServiceClient, newReplicaNum uint32) {
	fmt.Printf("Updating group %s to %d replicas...\n", GroupName, newReplicaNum)
	_, err := groupClient.Update(ctx, &databasev1.GroupRegistryServiceUpdateRequest{
		Group: &commonv1.Group{
			Metadata: &commonv1.Metadata{
				Name: GroupName,
			},
			Catalog: commonv1.Catalog_CATALOG_PROPERTY,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum: 1,
				Replicas: newReplicaNum,
			},
		},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// CreatePropertySchema creates a property schema.
func CreatePropertySchema(ctx context.Context, propertyClient databasev1.PropertyRegistryServiceClient) {
	fmt.Printf("Creating property schema %s...\n", PropertyName)
	_, err := propertyClient.Create(ctx, &databasev1.PropertyRegistryServiceCreateRequest{
		Property: &databasev1.Property{
			Metadata: &commonv1.Metadata{
				Name:  PropertyName,
				Group: GroupName,
			},
			Tags: []*databasev1.TagSpec{
				{Name: "data", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// WriteProperties writes a batch of properties concurrently.
func WriteProperties(ctx context.Context, propertyServiceClient propertyv1.PropertyServiceClient,
	startIdx int, endIdx int,
) error {
	fmt.Printf("Starting to write %d-%d properties using %d goroutines...\n",
		startIdx, endIdx, Concurrency)

	startTime := time.Now()

	// Channel to generate property data
	dataChannel := make(chan int, 1000) // Buffer for property indices
	var wg sync.WaitGroup
	var totalProcessed int64

	// Start data producer goroutine
	go func() {
		defer close(dataChannel)
		for i := startIdx; i < endIdx; i++ {
			dataChannel <- i
		}
	}()

	// Start consumer goroutines
	for i := 0; i < Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer ginkgo.GinkgoRecover()
			defer wg.Done()
			var count int64

			for propertyIndex := range dataChannel {
				propertyID := fmt.Sprintf("property-%d", propertyIndex)
				largeData := GenerateLargeData(DataSize)
				timestamp := time.Now().Format(time.RFC3339Nano)

				_, writeErr := propertyServiceClient.Apply(ctx, &propertyv1.ApplyRequest{
					Property: &propertyv1.Property{
						Metadata: &commonv1.Metadata{
							Name:  PropertyName,
							Group: GroupName,
						},
						Id: propertyID,
						Tags: []*modelv1.Tag{
							{Key: "data", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: largeData}}}},
							{Key: "timestamp", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: timestamp}}}},
						},
					},
				})
				gomega.Expect(writeErr).NotTo(gomega.HaveOccurred())

				count++
				atomic.AddInt64(&totalProcessed, 1)

				if atomic.LoadInt64(&totalProcessed)%500 == 0 {
					elapsed := time.Since(startTime)
					totalCount := atomic.LoadInt64(&totalProcessed)
					fmt.Printf("total processed: %d, use: %v\n", totalCount, elapsed)
				}
			}

			fmt.Printf("Worker %d completed: processed %d properties total\n", workerID, count)
		}(i)
	}

	wg.Wait()
	endTime := time.Now()
	duration := endTime.Sub(startTime)
	fmt.Printf("Write completed: %d properties in %s (%s props/sec)\n",
		endIdx, FormatDuration(duration), FormatThroughput(int64(endIdx), duration))
	time.Sleep(10 * time.Second)
	return nil
}

// GetNodeMetrics fetches prometheus metrics from a single data node endpoint.
func GetNodeMetrics(endpoint string, nodeIndex int) *NodeMetrics {
	nodeName := fmt.Sprintf("data-node-%d", nodeIndex+1)
	metrics := &NodeMetrics{
		NodeName:       nodeName,
		LastScrapeTime: time.Now(),
		IsHealthy:      false,
	}

	// Set timeout for HTTP request
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Get(endpoint)
	if err != nil {
		metrics.ErrorMessage = fmt.Sprintf("Failed to connect to %s: %v", endpoint, err)
		return metrics
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		metrics.ErrorMessage = fmt.Sprintf("HTTP error %d from %s", resp.StatusCode, endpoint)
		return metrics
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		metrics.ErrorMessage = fmt.Sprintf("Failed to read response from %s: %v", endpoint, err)
		return metrics
	}

	// Parse metrics from prometheus data
	content := string(body)
	totalPropagationCount := parseTotalPropagationCount(content)
	repairSuccessCount := parseRepairSuccessCount(content)

	metrics.TotalPropagationCount = totalPropagationCount
	metrics.RepairSuccessCount = repairSuccessCount
	metrics.IsHealthy = true
	return metrics
}

// parseTotalPropagationCount parses the total_propagation_count from prometheus metrics text.
func parseTotalPropagationCount(content string) int64 {
	// Look for metric lines like: banyandb_property_repair_gossip_server_total_propagation_count{group="perf-test-group",original_node="data-node-1:17912"} 3
	re := regexp.MustCompile(`banyandb_property_repair_gossip_server_total_propagation_count\{[^}]+\}\s+(\d+(?:\.\d+)?)`)
	matches := re.FindAllStringSubmatch(content, -1)

	var totalCount int64
	for _, match := range matches {
		if len(match) >= 2 {
			value, err := strconv.ParseFloat(match[1], 64)
			if err != nil {
				continue
			}
			totalCount += int64(value)
		}
	}

	return totalCount
}

// parseRepairSuccessCount parses the repair_success_count from prometheus metrics text.
func parseRepairSuccessCount(content string) int64 {
	// Look for metric lines like: banyandb_property_scheduler_property_repair_success_count{group="perf-test-group",shard="0"} 100
	re := regexp.MustCompile(`banyandb_property_scheduler_property_repair_success_count\{[^}]+\}\s+(\d+(?:\.\d+)?)`)
	matches := re.FindAllStringSubmatch(content, -1)

	var totalCount int64
	for _, match := range matches {
		if len(match) >= 2 {
			value, err := strconv.ParseFloat(match[1], 64)
			if err != nil {
				continue
			}
			totalCount += int64(value)
		}
	}

	return totalCount
}

// GetAllNodeMetrics fetches metrics from all data nodes concurrently.
func GetAllNodeMetrics() []*NodeMetrics {
	var wg sync.WaitGroup
	metrics := make([]*NodeMetrics, len(PrometheusEndpoints))

	for i, endpoint := range PrometheusEndpoints {
		wg.Add(1)
		go func(index int, url string) {
			defer wg.Done()
			metrics[index] = GetNodeMetrics(url, index)
		}(i, endpoint)
	}

	wg.Wait()
	return metrics
}

// VerifyPropagationCountIncreased compares metrics before and after to verify total_propagation_count increased by exactly 1.
func VerifyPropagationCountIncreased(beforeMetrics, afterMetrics []*NodeMetrics) error {
	if len(beforeMetrics) != len(afterMetrics) {
		return fmt.Errorf("metrics array length mismatch: before=%d, after=%d", len(beforeMetrics), len(afterMetrics))
	}

	for i, after := range afterMetrics {
		before := beforeMetrics[i]

		if !after.IsHealthy {
			return fmt.Errorf("node %s is not healthy after update: %s", after.NodeName, after.ErrorMessage)
		}

		if !before.IsHealthy {
			return fmt.Errorf("node %s was not healthy before update: %s", before.NodeName, before.ErrorMessage)
		}

		expectedCount := before.TotalPropagationCount + 1
		if after.TotalPropagationCount != expectedCount {
			return fmt.Errorf("node %s propagation count mismatch: expected=%d, actual=%d (before=%d)",
				after.NodeName, expectedCount, after.TotalPropagationCount, before.TotalPropagationCount)
		}
	}

	return nil
}

// PrintMetricsComparison prints a comparison of metrics before and after.
func PrintMetricsComparison(beforeMetrics, afterMetrics []*NodeMetrics) {
	fmt.Println("=== Prometheus Metrics Comparison ===")
	fmt.Printf("%-12s | %-29s | %-29s | %-7s\n", "Node", "Propagation Count", "Repair Success Count", "Healthy")
	fmt.Printf("%-12s | %-9s %-9s %-9s | %-9s %-9s %-9s | %-7s\n", "", "Before", "After", "Delta", "Before", "After", "Delta", "")
	fmt.Println(strings.Repeat("-", 85))

	for i, after := range afterMetrics {
		if i < len(beforeMetrics) {
			before := beforeMetrics[i]
			propagationDelta := after.TotalPropagationCount - before.TotalPropagationCount
			repairDelta := after.RepairSuccessCount - before.RepairSuccessCount
			healthStatus := "✓"
			if !after.IsHealthy {
				healthStatus = "✗"
			}

			fmt.Printf("%-12s | %-9d %-9d %-9d | %-9d %-9d %-9d | %-7s\n",
				after.NodeName,
				before.TotalPropagationCount, after.TotalPropagationCount, propagationDelta,
				before.RepairSuccessCount, after.RepairSuccessCount, repairDelta,
				healthStatus)
		}
	}
	fmt.Println()
}

// ExecuteComposeCommand executes a docker-compose command, supporting both v1 and v2.
func ExecuteComposeCommand(args ...string) error {
	// Detect compose invoker
	invoker, detectErr := detectComposeInvoker()
	if detectErr != nil {
		return fmt.Errorf("failed to detect compose invoker: %w", detectErr)
	}

	// Build and execute command
	cmdArgs := append([]string{}, invoker...)
	cmdArgs = append(cmdArgs, args...)
	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...) // #nosec G204 -- invoker is from trusted detectComposeInvoker function
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if runErr := cmd.Run(); runErr != nil {
		return runErr
	}

	// wait all the container ready
	interval := time.Second * 3
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for compose containers ready timeout")
		default:
		}
		ids, err := listComposeContainerIDs()
		if err != nil {
			return fmt.Errorf("listing compose containers: %w", err)
		}
		if len(ids) == 0 {
			time.Sleep(interval)
			continue
		}

		allOK := true
		var details []string

		for _, id := range ids {
			name, health, state, err := inspectHealth(id)
			if err != nil {
				allOK = false
				details = append(details, fmt.Sprintf("%s: inspect error: %v", id, err))
				continue
			}

			ok := false
			switch {
			case health != "":
				ok = health == "healthy"
			default:
				ok = state == "running"
			}

			if !ok {
				allOK = false
			}
			if health == "" {
				details = append(details, fmt.Sprintf("%s (state=%s, no healthcheck)", name, state))
			} else {
				details = append(details, fmt.Sprintf("%s (health=%s, state=%s)", name, health, state))
			}
		}

		fmt.Println("Compose containers status:")
		for _, d := range details {
			fmt.Println("   -", d)
		}

		if allOK {
			return nil
		}

		time.Sleep(interval)
	}
}

func detectComposeInvoker() ([]string, error) {
	// Try v2: docker compose
	if _, err := exec.LookPath("docker"); err == nil {
		check := exec.Command("docker", "compose", "version")
		if out, checkErr := check.CombinedOutput(); checkErr == nil && strings.Contains(string(out), "Docker Compose") {
			return []string{"docker", "compose"}, nil
		}
	}
	// Try v1: docker-compose
	if _, err := exec.LookPath("docker-compose"); err == nil {
		return []string{"docker-compose"}, nil
	}
	return nil, errors.New("no docker compose found (neither 'docker compose' nor 'docker-compose')")
}

func listComposeContainerIDs() ([]string, error) {
	out, err := exec.Command("docker", "ps", "-q").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("docker ps -q: %w (%s)", err, string(out))
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	var ids []string
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l != "" {
			ids = append(ids, l)
		}
	}
	return ids, nil
}

func inspectHealth(containerID string) (name, health, state string, err error) {
	tpl := "{{.Name}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}{{end}}|{{.State.Status}}"
	cmd := exec.Command("docker", "inspect", "-f", tpl, containerID)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", "", "", fmt.Errorf("docker inspect %s: %w (%s)", containerID, err, string(out))
	}
	parts := strings.Split(strings.TrimSpace(string(out)), "|")
	if len(parts) != 3 {
		return "", "", "", errors.New("unexpected inspect output: " + string(out))
	}
	name = strings.TrimPrefix(parts[0], "/")
	health = parts[1]
	state = parts[2]
	return
}

// CreateLogDir creates a timestamped log directory for a test scenario.
func CreateLogDir(testScenario string) (string, error) {
	timestamp := time.Now().Format("20060102-150405")
	logDir := fmt.Sprintf("/tmp/banyandb-test-%s-%s", testScenario, timestamp)
	if createErr := os.MkdirAll(logDir, 0o755); createErr != nil {
		return "", fmt.Errorf("failed to create log directory: %w", createErr)
	}
	fmt.Printf("Created log directory: %s\n", logDir)
	return logDir, nil
}

// ExportDockerComposeLogs exports logs from all services in the docker compose stack.
func ExportDockerComposeLogs(composeFile, logDir string) error {
	fmt.Printf("Exporting docker compose logs to %s...\n", logDir)

	// Detect compose invoker
	invoker, detectErr := detectComposeInvoker()
	if detectErr != nil {
		return fmt.Errorf("failed to detect compose invoker: %w", detectErr)
	}

	// Get list of services dynamically from compose file
	fmt.Println("  Discovering services from compose file...")
	configArgs := append([]string{}, invoker...)
	configArgs = append(configArgs, "-f", composeFile, "config", "--services")
	configCmd := exec.Command(configArgs[0], configArgs[1:]...) // #nosec G204 -- invoker is from trusted detectComposeInvoker function

	// Separate stdout and stderr to filter out warnings
	var stdout, stderr bytes.Buffer
	configCmd.Stdout = &stdout
	configCmd.Stderr = &stderr

	if configErr := configCmd.Run(); configErr != nil {
		return fmt.Errorf("failed to get services list: %w (stderr: %s)", configErr, stderr.String())
	}

	servicesOutput := stdout.Bytes()

	// Parse services list and filter out warning lines
	serviceLines := strings.Split(strings.TrimSpace(string(servicesOutput)), "\n")
	var services []string
	for _, line := range serviceLines {
		serviceName := strings.TrimSpace(line)
		// Skip empty lines and WARN lines
		if serviceName != "" && !strings.HasPrefix(serviceName, "WARN[") {
			services = append(services, serviceName)
		}
	}

	if len(services) == 0 {
		return errors.New("no services found in compose file")
	}

	fmt.Printf("  Found %d services: %v\n", len(services), services)

	// Export logs for each service
	for _, service := range services {
		logFile := fmt.Sprintf("%s/%s.log", logDir, service)
		fmt.Printf("  Exporting logs for service %s to %s...\n", service, logFile)

		// Build command: docker compose -f <file> logs --no-color <service>
		logsArgs := append([]string{}, invoker...)
		logsArgs = append(logsArgs, "-f", composeFile, "logs", "--no-color", service)
		logsCmd := exec.Command(logsArgs[0], logsArgs[1:]...) // #nosec G204 -- invoker is from trusted detectComposeInvoker function

		// Capture output
		output, cmdErr := logsCmd.CombinedOutput()
		if cmdErr != nil {
			// Service might not exist or have no logs, continue with warning
			fmt.Printf("  Warning: failed to get logs for %s: %v\n", service, cmdErr)
			continue
		}

		// Write to file
		if writeErr := os.WriteFile(logFile, output, 0o600); writeErr != nil {
			return fmt.Errorf("failed to write log file for %s: %w", service, writeErr)
		}
		fmt.Printf("  Successfully exported %d bytes of logs for %s\n", len(output), service)
	}

	fmt.Printf("All logs exported to %s\n", logDir)
	return nil
}
