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

package server

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"

	ebpfv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/ebpf/v1"
	"github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/collector"
	"github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/metrics"
)

// grpcServer implements the EBPFMetricsService.
type grpcServer struct {
	ebpfv1.UnimplementedEBPFMetricsServiceServer
	collector *collector.Collector
	logger    *zap.Logger
	startTime time.Time
	version   string
}

// NewGRPCServer creates a new gRPC server implementation.
func NewGRPCServer(coll *collector.Collector, logger *zap.Logger) ebpfv1.EBPFMetricsServiceServer {
	return &grpcServer{
		collector: coll,
		logger:    logger,
		startTime: time.Now(),
		version:   "v0.1.0", // TODO: Get from build info
	}
}

// GetMetrics retrieves current metrics from all modules.
func (s *grpcServer) GetMetrics(ctx context.Context, req *ebpfv1.GetMetricsRequest) (*ebpfv1.GetMetricsResponse, error) {
	store := s.collector.GetMetrics()
	if store == nil {
		return nil, status.Error(codes.Internal, "metrics store not available")
	}

	allMetrics := store.GetAll()
	response := &ebpfv1.GetMetricsResponse{
		MetricSets: make([]*ebpfv1.MetricSet, 0),
		Timestamp:  timestamppb.Now(),
	}

	// Filter by requested modules if specified
	modulesToInclude := make(map[string]bool)
	if len(req.Modules) > 0 {
		for _, module := range req.Modules {
			modulesToInclude[module] = true
		}
	}

	totalMetrics := uint32(0)
	for moduleName, metricSet := range allMetrics {
		// Skip if specific modules requested and this isn't one
		if len(modulesToInclude) > 0 && !modulesToInclude[moduleName] {
			continue
		}

		pbMetricSet := &ebpfv1.MetricSet{
			ModuleName:  moduleName,
			Metrics:     make([]*ebpfv1.Metric, 0),
			CollectedAt: timestamppb.Now(),
		}

		for _, metric := range metricSet.GetMetrics() {
			// Apply label filters if specified
			if !matchLabels(metric.Labels, req.LabelFilters) {
				continue
			}

			pbMetric := &ebpfv1.Metric{
				Name:      metric.Name,
				Value:     metric.Value,
				Type:      convertMetricType(metric.Type),
				Labels:    metric.Labels,
				Timestamp: timestamppb.Now(),
			}
			pbMetricSet.Metrics = append(pbMetricSet.Metrics, pbMetric)
			totalMetrics++
		}

		if len(pbMetricSet.Metrics) > 0 {
			response.MetricSets = append(response.MetricSets, pbMetricSet)
		}
	}

	response.TotalMetrics = totalMetrics
	return response, nil
}

// StreamMetrics streams metrics updates in real-time.
func (s *grpcServer) StreamMetrics(req *ebpfv1.StreamMetricsRequest, stream ebpfv1.EBPFMetricsService_StreamMetricsServer) error {
	interval := time.Duration(req.IntervalSeconds) * time.Second
	if interval < time.Second {
		interval = 10 * time.Second // Default interval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Create module filter
	modulesToInclude := make(map[string]bool)
	if len(req.Modules) > 0 {
		for _, module := range req.Modules {
			modulesToInclude[module] = true
		}
	}

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
			store := s.collector.GetMetrics()
			if store == nil {
				continue
			}

			allMetrics := store.GetAll()
			for moduleName, metricSet := range allMetrics {
				// Skip if specific modules requested and this isn't one
				if len(modulesToInclude) > 0 && !modulesToInclude[moduleName] {
					continue
				}

				pbMetricSet := &ebpfv1.MetricSet{
					ModuleName:  moduleName,
					Metrics:     make([]*ebpfv1.Metric, 0),
					CollectedAt: timestamppb.Now(),
				}

				for _, metric := range metricSet.GetMetrics() {
					// Apply label filters if specified
					if !matchLabels(metric.Labels, req.LabelFilters) {
						continue
					}

					pbMetric := &ebpfv1.Metric{
						Name:      metric.Name,
						Value:     metric.Value,
						Type:      convertMetricType(metric.Type),
						Labels:    metric.Labels,
						Timestamp: timestamppb.Now(),
					}
					pbMetricSet.Metrics = append(pbMetricSet.Metrics, pbMetric)
				}

				if len(pbMetricSet.Metrics) > 0 {
					response := &ebpfv1.StreamMetricsResponse{
						MetricSet: pbMetricSet,
					}
					if err := stream.Send(response); err != nil {
						s.logger.Error("Failed to send metrics", zap.Error(err))
						return err
					}
				}
			}
		}
	}
}

// GetIOStats retrieves detailed I/O statistics.
func (s *grpcServer) GetIOStats(ctx context.Context, req *ebpfv1.GetIOStatsRequest) (*ebpfv1.GetIOStatsResponse, error) {
	store := s.collector.GetMetrics()
	if store == nil {
		return nil, status.Error(codes.Internal, "metrics store not available")
	}

	// Get metrics from iomonitor module
	allMetrics := store.GetAll()
	ioMetrics, ok := allMetrics["iomonitor"]
	if !ok {
		return nil, status.Error(codes.NotFound, "I/O monitor module not found")
	}

	stats := &ebpfv1.IOStats{
		Fadvise: &ebpfv1.FadviseStats{
			AdviceCounts: make(map[string]uint64),
		},
		Cache:  &ebpfv1.CacheStats{},
		Memory: &ebpfv1.MemoryStats{},
	}

	// Parse metrics into IOStats structure
	for _, metric := range ioMetrics.GetMetrics() {
		switch metric.Name {
		case "ebpf_fadvise_calls_total":
			stats.Fadvise.TotalCalls = uint64(metric.Value)
		case "ebpf_fadvise_success_total":
			stats.Fadvise.SuccessCalls = uint64(metric.Value)
		case "ebpf_fadvise_failed_total":
			stats.Fadvise.FailedCalls = uint64(metric.Value)
		case "ebpf_fadvise_advice_total":
			if advice, ok := metric.Labels["advice"]; ok {
				stats.Fadvise.AdviceCounts[advice] = uint64(metric.Value)
			}
		case "ebpf_cache_read_attempts_total":
			stats.Cache.ReadAttempts = uint64(metric.Value)
		case "ebpf_cache_hits_total":
			stats.Cache.Hits = uint64(metric.Value)
		case "ebpf_cache_misses_total":
			stats.Cache.Misses = uint64(metric.Value)
		case "ebpf_cache_hit_rate_percent":
			stats.Cache.HitRatePercent = metric.Value
		case "ebpf_cache_miss_rate_percent":
			stats.Cache.MissRatePercent = metric.Value
		case "ebpf_page_cache_adds_total":
			stats.Cache.PagesAdded = uint64(metric.Value)
		case "ebpf_memory_lru_pages_scanned":
			stats.Memory.LruPagesScanned = uint64(metric.Value)
		case "ebpf_memory_lru_pages_reclaimed":
			stats.Memory.PagesReclaimed = uint64(metric.Value)
		case "ebpf_memory_reclaim_efficiency_percent":
			stats.Memory.ReclaimEfficiencyPercent = metric.Value
		case "ebpf_memory_direct_reclaim_processes":
			stats.Memory.DirectReclaimProcesses = uint32(metric.Value)
		}
	}

	return &ebpfv1.GetIOStatsResponse{
		Stats:     stats,
		Timestamp: timestamppb.Now(),
	}, nil
}

// GetModuleStatus retrieves status of eBPF modules.
func (s *grpcServer) GetModuleStatus(ctx context.Context, req *ebpfv1.GetModuleStatusRequest) (*ebpfv1.GetModuleStatusResponse, error) {
	// Get module status from collector
	modules := make([]*ebpfv1.ModuleStatus, 0)

	// For now, we only have iomonitor module
	// TODO: Get actual status from collector
	ioStatus := &ebpfv1.ModuleStatus{
		Name:          "iomonitor",
		Enabled:       true,
		Running:       true,
		LastError:     "",
		MetricCount:   0,
		LastCollected: timestamppb.Now(),
	}

	// Count metrics for the module
	if store := s.collector.GetMetrics(); store != nil {
		if ioMetrics, ok := store.GetAll()["iomonitor"]; ok {
			ioStatus.MetricCount = uint32(ioMetrics.Count())
		}
	}

	modules = append(modules, ioStatus)

	// System status
	systemStatus := &ebpfv1.SystemStatus{
		EbpfSupported: true, // TODO: Actually check eBPF support
		KernelVersion: getKernelVersion(),
		ActiveModules: 1,
		TotalMetrics:  ioStatus.MetricCount,
		UptimeSeconds: uint64(time.Since(s.startTime).Seconds()),
	}

	return &ebpfv1.GetModuleStatusResponse{
		Modules:      modules,
		SystemStatus: systemStatus,
	}, nil
}

// ConfigureModule enables or disables an eBPF module.
func (s *grpcServer) ConfigureModule(ctx context.Context, req *ebpfv1.ConfigureModuleRequest) (*ebpfv1.ConfigureModuleResponse, error) {
	// TODO: Implement module configuration
	// For now, return not implemented
	return nil, status.Error(codes.Unimplemented, "module configuration not yet implemented")
}

// GetHealth returns health status.
func (s *grpcServer) GetHealth(ctx context.Context, req *ebpfv1.GetHealthRequest) (*ebpfv1.GetHealthResponse, error) {
	healthStatus := ebpfv1.HealthStatus_HEALTH_STATUS_HEALTHY
	message := "eBPF sidecar is healthy"

	// Check if collector is working
	if store := s.collector.GetMetrics(); store == nil {
		healthStatus = ebpfv1.HealthStatus_HEALTH_STATUS_DEGRADED
		message = "Metrics collection is degraded"
	}

	return &ebpfv1.GetHealthResponse{
		Status:        healthStatus,
		Message:       message,
		Version:       s.version,
		UptimeSeconds: uint64(time.Since(s.startTime).Seconds()),
		LastError:     "", // TODO: Track last error
	}, nil
}

// Helper functions.

func convertMetricType(t metrics.MetricType) ebpfv1.MetricType {
	switch t {
	case metrics.MetricTypeCounter:
		return ebpfv1.MetricType_METRIC_TYPE_COUNTER
	case metrics.MetricTypeGauge:
		return ebpfv1.MetricType_METRIC_TYPE_GAUGE
	case metrics.MetricTypeHistogram:
		return ebpfv1.MetricType_METRIC_TYPE_HISTOGRAM
	default:
		return ebpfv1.MetricType_METRIC_TYPE_UNSPECIFIED
	}
}

func matchLabels(metricLabels, filterLabels map[string]string) bool {
	if len(filterLabels) == 0 {
		return true
	}

	for k, v := range filterLabels {
		if metricLabels[k] != v {
			return false
		}
	}
	return true
}

func getKernelVersion() string {
	// Simple kernel version retrieval
	return fmt.Sprintf("%s %s", runtime.GOOS, runtime.GOARCH)
}
