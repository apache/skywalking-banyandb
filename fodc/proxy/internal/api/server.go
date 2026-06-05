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

	"google.golang.org/protobuf/encoding/protojson"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/cluster"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/diagnostics"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/lifecycle"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/metrics"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// lifecycleGroupMarshaler emits zero-value protobuf fields (replicas=0, close=false, empty
// stages, etc.) as explicit JSON keys instead of dropping them. The default encoding/json +
// protoc-gen-go combination would silence those fields via `omitempty`, leaving downstream
// consumers (SRE agent) unable to tell "field absent" from "value is zero".
var lifecycleGroupMarshaler = protojson.MarshalOptions{
	UseProtoNames:   true,
	EmitUnpopulated: true,
}

// Server exposes REST and Prometheus-style endpoints for external consumption.
type Server struct {
	metricsAggregator          *metrics.Aggregator
	clusterStateCollector      *cluster.Manager
	lifecycleManager           *lifecycle.Manager
	crashDiagnosticsAggregator *diagnostics.Aggregator
	registry                   *registry.AgentRegistry
	server                     *http.Server
	logger                     *logger.Logger
	startTime                  time.Time
}

// NewServer creates a new Server instance.
func NewServer(
	metricsAggregator *metrics.Aggregator,
	clusterStateCollector *cluster.Manager,
	lifecycleManager *lifecycle.Manager,
	registry *registry.AgentRegistry,
	crashDiagnosticsAggregator *diagnostics.Aggregator,
	logger *logger.Logger,
) *Server {
	return &Server{
		metricsAggregator:          metricsAggregator,
		clusterStateCollector:      clusterStateCollector,
		lifecycleManager:           lifecycleManager,
		crashDiagnosticsAggregator: crashDiagnosticsAggregator,
		registry:                   registry,
		logger:                     logger,
		startTime:                  time.Now(),
	}
}

// Start starts the HTTP server.
func (s *Server) Start(listenAddr string, readTimeout, writeTimeout time.Duration) error {
	mux := http.NewServeMux()

	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/metrics-windows", s.handleMetricsWindows)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/cluster/topology", s.handleClusterTopology)
	mux.HandleFunc("/cluster/lifecycle", s.handleClusterLifecycle)
	mux.HandleFunc("/diagnostics", s.handleDiagnostics)

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
		PodName: r.URL.Query().Get("pod_name"),
	}

	ctx := r.Context()
	aggregatedMetrics, collectErr := s.metricsAggregator.GetLatestMetrics(ctx, filter)
	if collectErr != nil {
		s.logger.Error().Err(collectErr).Msg("Failed to collect metrics")
		http.Error(w, "Failed to collect metrics", http.StatusInternalServerError)
		return
	}

	if filter.Role != "" || filter.PodName != "" {
		filteredMetrics := make([]*metrics.AggregatedMetric, 0)
		for _, metric := range aggregatedMetrics {
			if filter.Role != "" && metric.Labels["node_role"] != filter.Role {
				continue
			}
			if filter.PodName != "" {
				addressMatch := metric.Labels["pod_name"] == filter.PodName
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
		PodName: r.URL.Query().Get("pod_name"),
	}

	ctx := r.Context()
	var aggregatedMetrics []*metrics.AggregatedMetric
	var collectErr error

	if startTimeStr != "" && endTimeStr != "" {
		aggregatedMetrics, collectErr = s.metricsAggregator.GetMetricsWindow(ctx, startTime, endTime, filter)
	} else {
		aggregatedMetrics, collectErr = s.metricsAggregator.GetLatestMetrics(ctx, filter)
	}

	if collectErr != nil {
		s.logger.Error().Err(collectErr).Msg("Failed to collect metrics")
		http.Error(w, "Failed to collect metrics", http.StatusInternalServerError)
		return
	}

	if filter.Role != "" || filter.PodName != "" {
		filteredMetrics := make([]*metrics.AggregatedMetric, 0)
		for _, metric := range aggregatedMetrics {
			if filter.Role != "" && metric.Labels["node_role"] != filter.Role {
				continue
			}
			if filter.PodName != "" {
				addressMatch := metric.Labels["pod_name"] == filter.PodName
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
//
// Metrics with a known Type (non-empty) are emitted using the real type. Metrics with
// Type=="" (pre-upgrade agents) fall back to the legacy suffix-heuristic path so behavior
// is unchanged for those metrics.
func (s *Server) formatPrometheusText(aggregatedMetrics []*metrics.AggregatedMetric) string {
	if len(aggregatedMetrics) == 0 {
		return ""
	}

	// Partition into typed (type known from agent) and untyped (legacy/unknown).
	var typed []*metrics.AggregatedMetric
	var untyped []*metrics.AggregatedMetric
	for _, m := range aggregatedMetrics {
		if m.Type != "" {
			typed = append(typed, m)
		} else {
			untyped = append(untyped, m)
		}
	}

	var builder strings.Builder
	remainingUntyped := writeTypedFamilies(&builder, typed, untyped)
	writeUntypedFamilies(&builder, remainingUntyped)
	return builder.String()
}

// writeTypedFamilies groups typed metrics by family base and emits one "# TYPE" line per
// family using the real type. The family base for histogram/summary component series
// (_bucket/_sum/_count) is the name with the suffix stripped; bare summary quantile series
// and all counter/gauge/untyped series use the name as-is. Untyped metrics whose base
// collides with a typed family are absorbed under the authoritative typed line — this
// prevents a mixed-version rollout from emitting two conflicting "# TYPE" lines for one
// name. The untyped metrics with no typed-family collision are returned for the legacy path.
func writeTypedFamilies(builder *strings.Builder, typed, untyped []*metrics.AggregatedMetric) []*metrics.AggregatedMetric {
	typedFamilyOrder := make([]string, 0)
	typedFamilies := make(map[string]*metricGroup) // base → group
	for _, m := range typed {
		base := typedFamilyBase(m.Name, m.Type)
		grp, exists := typedFamilies[base]
		if !exists {
			grp = &metricGroup{
				name:        base,
				description: m.Description,
				metricType:  m.Type,
				metrics:     make([]*metrics.AggregatedMetric, 0),
			}
			typedFamilies[base] = grp
			typedFamilyOrder = append(typedFamilyOrder, base)
		}
		grp.metrics = append(grp.metrics, m)
	}

	remainingUntyped := make([]*metrics.AggregatedMetric, 0, len(untyped))
	for _, m := range untyped {
		grp := matchTypedFamily(typedFamilies, m.Name)
		if grp == nil {
			remainingUntyped = append(remainingUntyped, m)
			continue
		}
		if grp.description == "" && m.Description != "" {
			grp.description = m.Description
		}
		grp.metrics = append(grp.metrics, m)
	}

	sort.Strings(typedFamilyOrder)
	for _, base := range typedFamilyOrder {
		grp := typedFamilies[base]
		if grp.description != "" {
			builder.WriteString(fmt.Sprintf("# HELP %s %s\n", base, grp.description))
		}
		builder.WriteString(fmt.Sprintf("# TYPE %s %s\n", base, grp.metricType))
		for _, m := range grp.metrics {
			builder.WriteString(formatMetricLine(m))
		}
	}
	return remainingUntyped
}

// writeUntypedFamilies emits metrics with no known type using the legacy suffix heuristic:
// a base name with a _bucket/_sum/_count sibling is treated as a histogram, everything else
// as a gauge. This preserves pre-typed behavior for pre-upgrade agents.
func writeUntypedFamilies(builder *strings.Builder, untyped []*metrics.AggregatedMetric) {
	if len(untyped) == 0 {
		return
	}
	metricMap := make(map[string]*metricGroup)
	histogramBases := make(map[string]bool)
	for _, m := range untyped {
		grp, exists := metricMap[m.Name]
		if !exists {
			grp = &metricGroup{
				name:        m.Name,
				description: m.Description,
				metrics:     make([]*metrics.AggregatedMetric, 0),
			}
			metricMap[m.Name] = grp
		}
		grp.metrics = append(grp.metrics, m)
		if baseName := getHistogramBaseName(m.Name); baseName != "" {
			histogramBases[baseName] = true
		}
	}

	histogramMetricsMap := make(map[string][]*metrics.AggregatedMetric)
	regularMetrics := make(map[string]*metricGroup)
	for name, grp := range metricMap {
		baseName := getHistogramBaseName(name)
		if baseName != "" && histogramBases[baseName] {
			histogramMetricsMap[baseName] = append(histogramMetricsMap[baseName], grp.metrics...)
		} else {
			regularMetrics[name] = grp
		}
	}

	histogramNames := make([]string, 0, len(histogramBases))
	for baseName := range histogramBases {
		histogramNames = append(histogramNames, baseName)
	}
	sort.Strings(histogramNames)
	for _, baseName := range histogramNames {
		allM := histogramMetricsMap[baseName]
		if len(allM) == 0 {
			continue
		}
		if description := allM[0].Description; description != "" {
			builder.WriteString(fmt.Sprintf("# HELP %s %s\n", baseName, description))
		}
		builder.WriteString(fmt.Sprintf("# TYPE %s histogram\n", baseName))
		for _, m := range allM {
			builder.WriteString(formatMetricLine(m))
		}
	}

	regularNames := make([]string, 0, len(regularMetrics))
	for name := range regularMetrics {
		regularNames = append(regularNames, name)
	}
	sort.Strings(regularNames)
	for _, name := range regularNames {
		grp := regularMetrics[name]
		if grp.description != "" {
			builder.WriteString(fmt.Sprintf("# HELP %s %s\n", grp.name, grp.description))
		}
		builder.WriteString(fmt.Sprintf("# TYPE %s gauge\n", grp.name))
		for _, m := range grp.metrics {
			builder.WriteString(formatMetricLine(m))
		}
	}
}

// matchTypedFamily returns the typed family an untyped series belongs to, or nil if none.
// It checks the exact name first (counter/gauge/untyped series and bare summary quantile
// series), then the histogram/summary component base after trimming a trailing
// _bucket/_sum/_count suffix. Used to fold pre-upgrade (untyped) samples into the
// authoritative typed family so the proxy never emits two TYPE lines for one name.
func matchTypedFamily(typedFamilies map[string]*metricGroup, name string) *metricGroup {
	if grp, ok := typedFamilies[name]; ok {
		return grp
	}
	for _, suffix := range []string{"_bucket", "_sum", "_count"} {
		if strings.HasSuffix(name, suffix) {
			base := strings.TrimSuffix(name, suffix)
			if base != "" {
				if grp, ok := typedFamilies[base]; ok {
					return grp
				}
			}
			break
		}
	}
	return nil
}

// typedFamilyBase returns the Prometheus family base name for a typed series.
// For histogram/summary component series the trailing _bucket/_sum/_count is stripped.
// For all other types (counter, gauge, untyped) the name is used as-is.
func typedFamilyBase(name, metricType string) string {
	switch metricType {
	case "histogram", "summary":
		for _, suffix := range []string{"_bucket", "_sum", "_count"} {
			if strings.HasSuffix(name, suffix) {
				base := strings.TrimSuffix(name, suffix)
				if base != "" {
					return base
				}
			}
		}
	}
	return name
}

// labelValueEscaper escapes the three characters that are special inside a Prometheus
// text-exposition label value: backslash, double-quote, and line feed. The replacements are
// applied in a single left-to-right pass, so an escaped backslash is not re-escaped.
var labelValueEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

// formatMetricLine renders a single metric sample as a Prometheus text line.
func formatMetricLine(m *metrics.AggregatedMetric) string {
	labelParts := make([]string, 0, len(m.Labels))
	for k, v := range m.Labels {
		labelParts = append(labelParts, fmt.Sprintf(`%s="%s"`, k, labelValueEscaper.Replace(v)))
	}
	sort.Strings(labelParts)
	labelStr := ""
	if len(labelParts) > 0 {
		labelStr = "{" + strings.Join(labelParts, ",") + "}"
	}
	return fmt.Sprintf("%s%s %s\n", m.Name, labelStr, formatFloat(m.Value))
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
				podName:     metric.Labels["pod_name"],
				data:        make([]map[string]interface{}, 0),
			}

			for key, value := range metric.Labels {
				tsMetric.labels[key] = value
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
			"pod_name":    tsMetric.podName,
			"data":        tsMetric.data,
		}

		result = append(result, item)
	}

	return result
}

// getMetricKey generates a unique key for a metric based on name, labels, and agent ID.
func (s *Server) getMetricKey(metric *metrics.AggregatedMetric) string {
	labelParts := make([]string, 0, len(metric.Labels))
	for key, value := range metric.Labels {
		labelParts = append(labelParts, fmt.Sprintf("%s=%s", key, value))
	}
	sort.Strings(labelParts)
	return fmt.Sprintf("%s|%s|%s", metric.Name, metric.AgentID, strings.Join(labelParts, ","))
}

// formatFloat formats a float64 value as a string.
func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

// getHistogramBaseName extracts the base name from histogram metric names.
// Returns empty string if the metric is not a histogram component.
func getHistogramBaseName(metricName string) string {
	if strings.HasSuffix(metricName, "_bucket") {
		return strings.TrimSuffix(metricName, "_bucket")
	}
	if strings.HasSuffix(metricName, "_sum") {
		return strings.TrimSuffix(metricName, "_sum")
	}
	if strings.HasSuffix(metricName, "_count") {
		return strings.TrimSuffix(metricName, "_count")
	}
	return ""
}

type metricGroup struct {
	name        string
	description string
	metricType  string
	metrics     []*metrics.AggregatedMetric
}

type timeSeriesMetric struct {
	name        string
	description string
	labels      map[string]string
	agentID     string
	podName     string
	data        []map[string]interface{}
}

// handleClusterLifecycle handles GET /cluster/lifecycle endpoint.
//
// The handler adds an "error" field to the response body whenever the proxy could not
// gather any lifecycle data — that is, when no agent was registered (Total == 0), no
// registered agent supported the lifecycle stream (Requested == 0), or none of the
// requested agents responded within the collection window (Responded == 0). The error
// message describes which of these three sub-cases occurred so callers can distinguish an
// infrastructure-layer outage (e.g. proxy pod restart with every agent failing to
// reconnect) from a cluster that genuinely has no groups. The HTTP status stays 200 in
// every case; the distinguishing signal is in the body, not in the status code.
func (s *Server) handleClusterLifecycle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.lifecycleManager == nil {
		http.Error(w, "Lifecycle manager not available", http.StatusServiceUnavailable)
		return
	}
	lifecycleData, agentSummary := s.lifecycleManager.CollectLifecycle(r.Context())

	groupsJSON, err := marshalLifecycleGroups(lifecycleData.Groups)
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to marshal lifecycle groups")
		http.Error(w, "Failed to serialize lifecycle groups", http.StatusInternalServerError)
		return
	}

	statusesJSON, err := marshalLifecycleStatuses(lifecycleData.LifecycleStatuses)
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to marshal lifecycle statuses")
		http.Error(w, "Failed to serialize lifecycle statuses", http.StatusInternalServerError)
		return
	}

	body := map[string]interface{}{
		"groups":             groupsJSON,
		"lifecycle_statuses": statusesJSON,
		"agent_summary":      agentSummary,
	}
	switch {
	case agentSummary.Total == 0:
		body["error"] = "FODC unavailable: no agents registered"
	case agentSummary.Requested == 0:
		body["error"] = fmt.Sprintf("FODC unavailable: 0/%d registered agents support the lifecycle stream",
			agentSummary.Total)
	case agentSummary.Responded == 0:
		body["error"] = fmt.Sprintf("FODC unavailable: 0/%d requested agents responded",
			agentSummary.Requested)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if encodeErr := json.NewEncoder(w).Encode(body); encodeErr != nil {
		s.logger.Error().Err(encodeErr).Msg("Failed to encode lifecycle response")
	}
}

// handleDiagnostics handles GET /diagnostics endpoint.
func (s *Server) handleDiagnostics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.crashDiagnosticsAggregator == nil {
		http.Error(w, "Diagnostics aggregator not available", http.StatusServiceUnavailable)
		return
	}
	filter := &diagnostics.Filter{
		Role:    r.URL.Query().Get("role"),
		PodName: r.URL.Query().Get("pod_name"),
	}
	records, collectErr := s.crashDiagnosticsAggregator.CollectDiagnostics(r.Context(), filter)
	if collectErr != nil {
		s.logger.Error().Err(collectErr).Msg("Failed to collect diagnostics")
		http.Error(w, "Failed to collect diagnostics", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if encodeErr := json.NewEncoder(w).Encode(records); encodeErr != nil {
		s.logger.Error().Err(encodeErr).Msg("Failed to encode diagnostics response")
	}
}

func marshalLifecycleGroups(groups []*fodcv1.GroupLifecycleInfo) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, 0, len(groups))
	for _, g := range groups {
		raw, err := lifecycleGroupMarshaler.Marshal(g)
		if err != nil {
			return nil, fmt.Errorf("marshal group %q: %w", g.GetName(), err)
		}
		out = append(out, raw)
	}
	return out, nil
}

// lifecycleStatusJSON mirrors lifecycle.PodLifecycleStatus but holds each report as a
// pre-marshaled protojson blob so the LifecycleReport's protobuf zero-value fields are
// emitted consistently with the groups payload.
type lifecycleStatusJSON struct {
	PodName string            `json:"pod_name"`
	Reports []json.RawMessage `json:"reports"`
}

func marshalLifecycleStatuses(statuses []*lifecycle.PodLifecycleStatus) ([]lifecycleStatusJSON, error) {
	out := make([]lifecycleStatusJSON, 0, len(statuses))
	for _, s := range statuses {
		reports := make([]json.RawMessage, 0, len(s.Reports))
		for _, r := range s.Reports {
			raw, err := lifecycleGroupMarshaler.Marshal(r)
			if err != nil {
				return nil, fmt.Errorf("marshal report %q: %w", r.GetFilename(), err)
			}
			reports = append(reports, raw)
		}
		out = append(out, lifecycleStatusJSON{PodName: s.PodName, Reports: reports})
	}
	return out, nil
}

// handleClusterTopology handles GET /cluster/topology endpoint.
func (s *Server) handleClusterTopology(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.clusterStateCollector == nil {
		http.Error(w, "Cluster topology collector not available", http.StatusServiceUnavailable)
		return
	}
	clusterTopology := s.clusterStateCollector.CollectClusterTopology(r.Context())
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if encodeErr := json.NewEncoder(w).Encode(clusterTopology); encodeErr != nil {
		s.logger.Error().Err(encodeErr).Msg("Failed to encode cluster topology response")
	}
}
