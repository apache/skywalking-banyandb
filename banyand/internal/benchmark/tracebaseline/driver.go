// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package tracebaseline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark/controller"
)

type scheduleDocument struct {
	DayStart    time.Time       `json:"dayStart"`
	Writes      []scheduleWrite `json:"writes"`
	DayDuration time.Duration   `json:"dayDuration"`
}

type scheduleWrite struct {
	Publication         time.Time         `json:"publication"`
	IndexSHA256         map[string]string `json:"indexSHA256"`
	PartID              string            `json:"partID"`
	CoreSHA256          string            `json:"coreSHA256"`
	CoreCompressedBytes uint64            `json:"coreCompressedBytes"`
	Rows                uint64            `json:"rows"`
}

// DriverOptions configures the external publisher process.
type DriverOptions struct {
	SocketPath    string
	SourceRoot    string
	DataRoot      string
	SchedulePath  string
	OutputPath    string
	Mode          string
	Acceleration  float64
	ControllerCPU int
}

// Drive publishes the immutable fixture from outside the measured cgroup and writes the completed run report.
func Drive(ctx context.Context, options DriverOptions) (RunReport, error) {
	if options.Acceleration <= 0 {
		return RunReport{}, fmt.Errorf("acceleration must be positive")
	}
	if options.Mode != ModeSerial && options.Mode != ModeThroughput {
		return RunReport{}, fmt.Errorf("mode must be serial or throughput")
	}
	if options.ControllerCPU >= 0 {
		if pinErr := controller.PinToCPUs([]int{options.ControllerCPU}); pinErr != nil {
			return RunReport{}, fmt.Errorf("cannot pin benchmark controller to CPU %d: %w", options.ControllerCPU, pinErr)
		}
	}
	scheduleData, readErr := os.ReadFile(options.SchedulePath)
	if readErr != nil {
		return RunReport{}, fmt.Errorf("cannot read benchmark schedule: %w", readErr)
	}
	var schedule scheduleDocument
	if decodeErr := json.Unmarshal(scheduleData, &schedule); decodeErr != nil {
		return RunReport{}, fmt.Errorf("cannot decode benchmark schedule: %w", decodeErr)
	}
	client := unixHTTPClient(options.SocketPath)
	if waitErr := waitForServer(ctx, client); waitErr != nil {
		return RunReport{}, fmt.Errorf("cannot reach measured data node: %w", waitErr)
	}
	if requestErr := postNoContent(ctx, client, "/measurement/start", nil); requestErr != nil {
		return RunReport{}, fmt.Errorf("cannot start primary measurement: %w", requestErr)
	}
	wallStart := time.Now()
	statusPoints := make([]StatusPoint, 0, len(schedule.Writes))
	var inputBytes, inputRows uint64
	for writeIdx := range schedule.Writes {
		write := &schedule.Writes[writeIdx]
		due := wallStart.Add(time.Duration(float64(write.Publication.Sub(schedule.DayStart)) / options.Acceleration))
		if options.Mode == ModeThroughput {
			if delay := time.Until(due); delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return RunReport{}, ctx.Err()
				case <-timer.C:
				}
			}
		}
		partID, parseErr := strconv.ParseUint(write.PartID, 16, 64)
		if parseErr != nil {
			return RunReport{}, fmt.Errorf("cannot parse scheduled part ID %q: %w", write.PartID, parseErr)
		}
		if publishErr := publishPart(options, write); publishErr != nil {
			return RunReport{}, publishErr
		}
		var status struct {
			OldestTimestamp int64  `json:"oldestTimestamp"`
			CoreBytes       uint64 `json:"coreBytes"`
			CoreParts       int    `json:"coreParts"`
			QueuedMerges    int    `json:"queuedMerges"`
			RunningMerges   int    `json:"runningMerges"`
			InFlightParts   int    `json:"inFlightParts"`
		}
		barrierStarted := time.Now()
		if postErr := postJSON(ctx, client, "/publish", PublishRequest{PartID: partID, LogicalNow: write.Publication}, &status); postErr != nil {
			return RunReport{}, postErr
		}
		barrierNanos := time.Since(barrierStarted).Nanoseconds()
		now := time.Now()
		lag := max(int64(0), now.Sub(due).Nanoseconds())
		statusPoints = append(statusPoints, StatusPoint{
			LogicalNow: write.Publication, WallTime: now, BarrierNanos: barrierNanos, LagNanos: lag, CoreBytes: status.CoreBytes, CoreParts: status.CoreParts,
			QueuedMerges: status.QueuedMerges, RunningMerges: status.RunningMerges, InFlightParts: status.InFlightParts,
			OldestPartAge: max(int64(0), write.Publication.UnixNano()-status.OldestTimestamp), PublishedParts: writeIdx + 1,
		})
		inputBytes += write.CoreCompressedBytes
		inputRows += write.Rows
	}
	dayEnd := schedule.DayStart.Add(schedule.DayDuration)
	if primaryErr := postNoContent(ctx, client, "/primary/end", PublishRequest{LogicalNow: dayEnd}); primaryErr != nil {
		return RunReport{}, fmt.Errorf("cannot finish primary phase: %w", primaryErr)
	}
	if cooldownErr := postNoContent(ctx, client, "/cooldown/run", PublishRequest{LogicalNow: dayEnd.Add(2 * time.Hour)}); cooldownErr != nil {
		return RunReport{}, fmt.Errorf("cannot run two-hour cooldown: %w", cooldownErr)
	}
	var report RunReport
	if reportErr := postJSON(ctx, client, "/report", nil, &report); reportErr != nil {
		return RunReport{}, fmt.Errorf("cannot collect measured run report: %w", reportErr)
	}
	controllerIdentity, _ := controller.CurrentResourceIdentity(os.Getpid())
	controllerHostname, _ := os.Hostname()
	report.Environment.ControllerPID = os.Getpid()
	report.Environment.ControllerCgroup = controllerHostname + ":" + controllerIdentity.Cgroup
	report.Status = statusPoints
	report.Published = len(schedule.Writes)
	report.Primary.InputBytes = inputBytes
	report.Primary.PublishedRows = inputRows
	reportData, marshalErr := json.MarshalIndent(report, "", "  ")
	if marshalErr != nil {
		return RunReport{}, fmt.Errorf("cannot encode completed run report: %w", marshalErr)
	}
	if writeErr := os.WriteFile(options.OutputPath, reportData, 0o600); writeErr != nil {
		return RunReport{}, fmt.Errorf("cannot write completed run report: %w", writeErr)
	}
	return report, nil
}

func publishPart(options DriverOptions, write *scheduleWrite) error {
	partID, parseErr := strconv.ParseUint(write.PartID, 16, 64)
	if parseErr != nil {
		return fmt.Errorf("cannot parse part ID %q for publication: %w", write.PartID, parseErr)
	}
	partName := fmt.Sprintf("%016x", partID)
	moves := []controller.Move{{
		Source: filepath.Join(options.SourceRoot, partName), Destination: filepath.Join(options.DataRoot, partName), SHA256: write.CoreSHA256,
	}}
	indexNames := make([]string, 0, len(write.IndexSHA256))
	for indexName := range write.IndexSHA256 {
		indexNames = append(indexNames, indexName)
	}
	sort.Strings(indexNames)
	for _, indexName := range indexNames {
		moves = append(moves, controller.Move{
			Source:      filepath.Join(options.SourceRoot, "sidx", indexName, partName),
			Destination: filepath.Join(options.DataRoot, "sidx", indexName, partName), SHA256: write.IndexSHA256[indexName],
		})
	}
	if _, publishErr := controller.AtomicPublish(moves); publishErr != nil {
		return fmt.Errorf("cannot atomically publish part %s: %w", partName, publishErr)
	}
	return nil
}

func unixHTTPClient(socketPath string) *http.Client {
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	}}
	return &http.Client{Transport: transport, Timeout: 15 * time.Minute}
}

func waitForServer(ctx context.Context, client *http.Client) error {
	for {
		request, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, "http://unix/health", nil)
		if requestErr == nil {
			response, responseErr := client.Do(request)
			if responseErr == nil {
				_ = response.Body.Close()
				if response.StatusCode == http.StatusNoContent {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for data-node benchmark server: %w", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func postNoContent(ctx context.Context, client *http.Client, path string, body any) error {
	var ignored any
	return postJSON(ctx, client, path, body, &ignored)
}

func postJSON(ctx context.Context, client *http.Client, path string, body, responseValue any) error {
	var requestBody io.Reader
	if body != nil {
		encoded, marshalErr := json.Marshal(body)
		if marshalErr != nil {
			return fmt.Errorf("cannot encode benchmark request %s: %w", path, marshalErr)
		}
		requestBody = bytes.NewReader(encoded)
	}
	request, requestErr := http.NewRequestWithContext(ctx, http.MethodPost, "http://unix"+path, requestBody)
	if requestErr != nil {
		return fmt.Errorf("cannot create benchmark request %s: %w", path, requestErr)
	}
	request.Header.Set("Content-Type", "application/json")
	response, responseErr := client.Do(request)
	if responseErr != nil {
		return fmt.Errorf("benchmark request %s failed: %w", path, responseErr)
	}
	defer response.Body.Close()
	responseData, readErr := io.ReadAll(response.Body)
	if readErr != nil {
		return fmt.Errorf("cannot read benchmark response %s: %w", path, readErr)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("benchmark request %s returned %s: %s", path, response.Status, responseData)
	}
	if len(responseData) > 0 && responseValue != nil {
		if decodeErr := json.Unmarshal(responseData, responseValue); decodeErr != nil {
			return fmt.Errorf("cannot decode benchmark response %s: %w", path, decodeErr)
		}
	}
	return nil
}
