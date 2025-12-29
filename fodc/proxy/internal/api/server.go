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

// Package api provides functionality for the API server.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/metrics"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Server exposes REST and Prometheus-style endpoints for external consumption.
type Server struct {
	metricsAggregator *metrics.Aggregator
	registry          *registry.AgentRegistry
	server            *http.Server
	logger            *logger.Logger
	startTime         time.Time
}

// NewServer creates a new Server instance.
func NewServer(metricsAggregator *metrics.Aggregator, registry *registry.AgentRegistry, logger *logger.Logger) *Server {
	return &Server{
		metricsAggregator: metricsAggregator,
		registry:          registry,
		logger:            logger,
		startTime:         time.Now(),
	}
}

// Start starts the HTTP server.
func (s *Server) Start(listenAddr string, readTimeout, writeTimeout time.Duration) error {
	mux := http.NewServeMux()

	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/metrics-windows", s.handleMetricsWindows)
	mux.HandleFunc("/health", s.handleHealth)

	s.server = &http.Server{
		Addr:         listenAddr,
		Handler:      mux,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	}

	s.logger.Info().Str("addr", listenAddr).Msg("Starting HTTP API server")

	go func() {
		if serveErr := s.server.ListenAndServe(); serveErr != nil && serveErr != http.ErrServerClosed {
			s.logger.Error().Err(serveErr).Msg("HTTP server error")
		}
	}()

	return nil
}

// Stop gracefully stops the HTTP server.
func (s *Server) Stop() error {
	if s.server == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return s.server.Shutdown(ctx)
}

// handleMetrics handles GET /metrics endpoint.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	filter := &metrics.Filter{
		Role:    r.URL.Query().Get("role"),
		Address: r.URL.Query().Get("address"),
	}

	ctx := r.Context()
	aggregatedMetrics, collectErr := s.metricsAggregator.GetLatestMetrics(ctx)
	if collectErr != nil {
		s.logger.Error().Err(collectErr).Msg("Failed to collect metrics")
		http.Error(w, "Failed to collect metrics", http.StatusInternalServerError)
		return
	}

	if filter.Role != "" || filter.Address != "" {
		filteredMetrics := make([]*metrics.AggregatedMetric, 0)
		for _, metric := range aggregatedMetrics {
			if filter.Role != "" && metric.NodeRole != filter.Role {
				continue
			}
			if filter.Address != "" {
				addressMatch := strings.Contains(metric.Labels["ip"], filter.Address) ||
					strings.Contains(fmt.Sprintf("%s:%s", metric.Labels["ip"], metric.Labels["port"]), filter.Address)
				if !addressMatch {
					continue
				}
			}
			filteredMetrics = append(filteredMetrics, metric)
		}
		aggregatedMetrics = filteredMetrics
	}

	prometheusText := s.formatPrometheusText(aggregatedMetrics)

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(prometheusText))
}

// handleMetricsWindows handles GET /metrics-windows endpoint.
func (s *Server) handleMetricsWindows(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var startTime, endTime time.Time
	var parseErr error

	startTimeStr := r.URL.Query().Get("start_time")
	if startTimeStr != "" {
		startTime, parseErr = time.Parse(time.RFC3339, startTimeStr)
		if parseErr != nil {
			http.Error(w, fmt.Sprintf("Invalid start_time format: %v", parseErr), http.StatusBadRequest)
			return
		}
	}

	endTimeStr := r.URL.Query().Get("end_time")
	if endTimeStr != "" {
		endTime, parseErr = time.Parse(time.RFC3339, endTimeStr)
		if parseErr != nil {
			http.Error(w, fmt.Sprintf("Invalid end_time format: %v", parseErr), http.StatusBadRequest)
			return
		}
	}

	filter := &metrics.Filter{
		Role:    r.URL.Query().Get("role"),
		Address: r.URL.Query().Get("address"),
	}

	ctx := r.Context()
	var aggregatedMetrics []*metrics.AggregatedMetric
	var collectErr error

	if startTimeStr != "" && endTimeStr != "" {
		aggregatedMetrics, collectErr = s.metricsAggregator.GetMetricsWindow(ctx, startTime, endTime, filter)
	} else {
		aggregatedMetrics, collectErr = s.metricsAggregator.GetLatestMetrics(ctx)
	}

	if collectErr != nil {
		s.logger.Error().Err(collectErr).Msg("Failed to collect metrics")
		http.Error(w, "Failed to collect metrics", http.StatusInternalServerError)
		return
	}

	if filter.Role != "" || filter.Address != "" {
		filteredMetrics := make([]*metrics.AggregatedMetric, 0)
		for _, metric := range aggregatedMetrics {
			if filter.Role != "" && metric.NodeRole != filter.Role {
				continue
			}
			if filter.Address != "" {
				addressMatch := strings.Contains(metric.Labels["ip"], filter.Address) ||
					strings.Contains(fmt.Sprintf("%s:%s", metric.Labels["ip"], metric.Labels["port"]), filter.Address)
				if !addressMatch {
					continue
				}
			}
			filteredMetrics = append(filteredMetrics, metric)
		}
		aggregatedMetrics = filteredMetrics
	}

	response := s.formatMetricsWindowJSON(aggregatedMetrics)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if encodeErr := json.NewEncoder(w).Encode(response); encodeErr != nil {
		s.logger.Error().Err(encodeErr).Msg("Failed to encode JSON response")
	}
}

// handleHealth handles GET /health endpoint.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	agents := s.registry.ListAgents()
	agentsOnline := 0
	for _, agentInfo := range agents {
		if agentInfo.Status == registry.AgentStatusOnline {
			agentsOnline++
		}
	}

	response := map[string]interface{}{
		"status":         "healthy",
		"agents_online":  agentsOnline,
		"agents_total":   len(agents),
		"uptime_seconds": int(time.Since(s.startTime).Seconds()),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if encodeErr := json.NewEncoder(w).Encode(response); encodeErr != nil {
		s.logger.Error().Err(encodeErr).Msg("Failed to encode JSON response")
	}
}

// formatPrometheusText formats aggregated metrics as Prometheus text format.
func (s *Server) formatPrometheusText(aggregatedMetrics []*metrics.AggregatedMetric) string {
	if len(aggregatedMetrics) == 0 {
		return ""
	}

	metricMap := make(map[string]*metricGroup)
	for _, metric := range aggregatedMetrics {
		key := metric.Name
		group, exists := metricMap[key]
		if !exists {
			group = &metricGroup{
				name:        metric.Name,
				description: metric.Description,
				metrics:     make([]*metrics.AggregatedMetric, 0),
			}
			metricMap[key] = group
		}
		group.metrics = append(group.metrics, metric)
	}

	var builder strings.Builder
	metricNames := make([]string, 0, len(metricMap))
	for name := range metricMap {
		metricNames = append(metricNames, name)
	}
	sort.Strings(metricNames)

	for _, name := range metricNames {
		group := metricMap[name]
		if group.description != "" {
			builder.WriteString(fmt.Sprintf("# HELP %s %s\n", group.name, group.description))
		}
		builder.WriteString(fmt.Sprintf("# TYPE %s gauge\n", group.name))

		for _, metric := range group.metrics {
			labelParts := make([]string, 0, len(metric.Labels))
			for key, value := range metric.Labels {
				labelParts = append(labelParts, fmt.Sprintf(`%s="%s"`, key, value))
			}
			sort.Strings(labelParts)

			labelStr := ""
			if len(labelParts) > 0 {
				labelStr = "{" + strings.Join(labelParts, ",") + "}"
			}

			builder.WriteString(fmt.Sprintf("%s%s %s\n", group.name, labelStr, formatFloat(metric.Value)))
		}
	}

	return builder.String()
}

// formatMetricsWindowJSON formats aggregated metrics as JSON for metrics-windows endpoint.
func (s *Server) formatMetricsWindowJSON(aggregatedMetrics []*metrics.AggregatedMetric) []map[string]interface{} {
	metricMap := make(map[string]*timeSeriesMetric)

	for _, metric := range aggregatedMetrics {
		key := s.getMetricKey(metric)
		tsMetric, exists := metricMap[key]
		if !exists {
			tsMetric = &timeSeriesMetric{
				name:        metric.Name,
				description: metric.Description,
				labels:      make(map[string]string),
				agentID:     metric.AgentID,
				ip:          metric.Labels["ip"],
				port:        metric.Labels["port"],
				data:        make([]map[string]interface{}, 0),
			}

			for key, value := range metric.Labels {
				if key != "ip" && key != "port" {
					tsMetric.labels[key] = value
				}
			}

			metricMap[key] = tsMetric
		}

		dataPoint := map[string]interface{}{
			"timestamp": metric.Timestamp.Format(time.RFC3339),
			"value":     metric.Value,
		}
		tsMetric.data = append(tsMetric.data, dataPoint)
	}

	result := make([]map[string]interface{}, 0, len(metricMap))
	for _, tsMetric := range metricMap {
		sort.Slice(tsMetric.data, func(i, j int) bool {
			timeI, errI := time.Parse(time.RFC3339, tsMetric.data[i]["timestamp"].(string))
			timeJ, errJ := time.Parse(time.RFC3339, tsMetric.data[j]["timestamp"].(string))
			if errI != nil {
				s.logger.Warn().
					Err(errI).
					Str("timestamp", tsMetric.data[i]["timestamp"].(string)).
					Msg("Failed to parse timestamp for sorting")
			}
			if errJ != nil {
				s.logger.Warn().
					Err(errJ).
					Str("timestamp", tsMetric.data[j]["timestamp"].(string)).
					Msg("Failed to parse timestamp for sorting")
			}
			return timeI.Before(timeJ)
		})

		item := map[string]interface{}{
			"name":        tsMetric.name,
			"description": tsMetric.description,
			"labels":      tsMetric.labels,
			"agent_id":    tsMetric.agentID,
			"ip":          tsMetric.ip,
			"port":        tsMetric.port,
			"data":        tsMetric.data,
		}

		portInt, parseErr := strconv.Atoi(tsMetric.port)
		if parseErr == nil {
			item["port"] = portInt
		}

		result = append(result, item)
	}

	return result
}

// getMetricKey generates a unique key for a metric based on name, labels, and agent ID.
func (s *Server) getMetricKey(metric *metrics.AggregatedMetric) string {
	labelParts := make([]string, 0, len(metric.Labels))
	for key, value := range metric.Labels {
		if key != "ip" && key != "port" {
			labelParts = append(labelParts, fmt.Sprintf("%s=%s", key, value))
		}
	}
	sort.Strings(labelParts)
	return fmt.Sprintf("%s|%s|%s", metric.Name, metric.AgentID, strings.Join(labelParts, ","))
}

// formatFloat formats a float64 value as a string.
func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

type metricGroup struct {
	name        string
	description string
	metrics     []*metrics.AggregatedMetric
}

type timeSeriesMetric struct {
	name        string
	description string
	labels      map[string]string
	agentID     string
	ip          string
	port        string
	data        []map[string]interface{}
}
