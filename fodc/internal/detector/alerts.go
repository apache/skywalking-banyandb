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
	"fmt"
	"time"

	"github.com/apache/skywalking-banyandb/fodc/internal/metric"
	"github.com/apache/skywalking-banyandb/fodc/internal/poller"
)

type AlertManager struct {
	errorRateThreshold float64
	alerts             []Alert
}

type Alert struct {
	Message   string
	Timestamp time.Time
	Severity  string
}

func NewAlertManager(errorRateThreshold float64) *AlertManager {
	return &AlertManager{
		errorRateThreshold: errorRateThreshold,
		alerts:             []Alert{},
	}
}

// findMetricByLabel finds a metric by name and label value, summing all matching metrics
func findMetricByLabel(metrics []metric.RawMetric, name, labelName, labelValue string) (float64, bool) {
	var sum float64
	found := false
	for _, m := range metrics {
		if m.Name == name {
			if labelValue == "" || m.Find(labelName) == labelValue {
				sum += m.Value
				found = true
			}
		}
	}
	return sum, found
}

// findMetricByName finds a metric by name, summing all matching metrics (ignoring labels)
func findMetricByName(metrics []metric.RawMetric, name string) (float64, bool) {
	var sum float64
	found := false
	for _, m := range metrics {
		if m.Name == name {
			sum += m.Value
			found = true
		}
	}
	return sum, found
}

func (a *AlertManager) AnalyzeMetrics(snapshot poller.MetricsSnapshot) []string {
	var alerts []string

	// Check for high error rates
	errRate, errRateFound := findMetricByName(snapshot.RawMetrics, "banyandb_liaison_grpc_total_err")
	totalStarted, totalFound := findMetricByName(snapshot.RawMetrics, "banyandb_liaison_grpc_total_started")
	if errRateFound && totalFound && totalStarted > 0 {
		rate := errRate / totalStarted
		if rate > a.errorRateThreshold {
			msg := fmt.Sprintf("High error rate detected: %.2f%% (threshold: %.2f%%)", rate*100, a.errorRateThreshold*100)
			alerts = append(alerts, msg)
			a.alerts = append(a.alerts, Alert{
				Message:   msg,
				Timestamp: snapshot.Timestamp,
				Severity:  "critical",
			})
		}
	}

	// Check memory usage
	// banyandb_system_memory_state has labels: kind="used", kind="total", kind="used_percent"
	memUsed, memUsedFound := findMetricByLabel(snapshot.RawMetrics, "banyandb_system_memory_state", "kind", "used")
	memTotal, memTotalFound := findMetricByLabel(snapshot.RawMetrics, "banyandb_system_memory_state", "kind", "total")
	if memUsedFound && memTotalFound && memTotal > 0 {
		usage := memUsed / memTotal
		if usage > 0.9 {
			msg := fmt.Sprintf("High memory usage: %.2f%%", usage*100)
			alerts = append(alerts, msg)
			a.alerts = append(a.alerts, Alert{
				Message:   msg,
				Timestamp: snapshot.Timestamp,
				Severity:  "warning",
			})
		}
	}

	// banyandb_system_disk has labels: path="...", kind="used", kind="total", kind="used_percent"
	diskUsed, diskUsedFound := findMetricByLabel(snapshot.RawMetrics, "banyandb_system_disk", "kind", "used")
	diskTotal, diskTotalFound := findMetricByLabel(snapshot.RawMetrics, "banyandb_system_disk", "kind", "total")
	if diskUsedFound && diskTotalFound && diskTotal > 0 {
		usage := diskUsed / diskTotal
		if usage > 0.8 {
			msg := fmt.Sprintf("High disk usage: %.2f%%", usage*100)
			alerts = append(alerts, msg)
			a.alerts = append(a.alerts, Alert{
				Message:   msg,
				Timestamp: snapshot.Timestamp,
				Severity:  "warning",
			})
		}
	}

	// Check for zero write rate (might indicate failure)
	writeRate, writeRateFound := findMetricByName(snapshot.RawMetrics, "banyandb_measure_total_written")
	if writeRateFound && writeRate == 0 {
		msg := fmt.Sprintf("Zero write rate detected: %.2f", writeRate)
		alerts = append(alerts, msg)
		a.alerts = append(a.alerts, Alert{
			Message:   msg,
			Timestamp: snapshot.Timestamp,
			Severity:  "warning",
		})
	}

	// Check for any errors in the snapshot
	if len(snapshot.Errors) > 0 {
		for _, err := range snapshot.Errors {
			msg := fmt.Sprintf("Metrics polling error: %s", err)
			alerts = append(alerts, msg)
			a.alerts = append(a.alerts, Alert{
				Message:   msg,
				Timestamp: snapshot.Timestamp,
				Severity:  "warning",
			})
		}
	}

	return alerts
}

func (a *AlertManager) HandleDeathRattle(event DeathRattleEvent) {
	alert := Alert{
		Message:   fmt.Sprintf("Death rattle (%s): %s", event.Type, event.Message),
		Timestamp: event.Timestamp,
		Severity:  event.Severity,
	}
	a.alerts = append(a.alerts, alert)
}
