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

package export

import (
	"context"
	"fmt"
	"io"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/metrics"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
)

// BanyanDBConfig holds configuration for BanyanDB export
type BanyanDBConfig struct {
	Endpoint      string        `yaml:"endpoint"`
	Group         string        `yaml:"group"`
	MeasureName   string        `yaml:"measure_name"`
	Timeout       time.Duration `yaml:"timeout"`
	BatchSize     int           `yaml:"batch_size"`
	FlushInterval time.Duration `yaml:"flush_interval"`
}

// DefaultBanyanDBConfig returns default configuration
func DefaultBanyanDBConfig() BanyanDBConfig {
	return BanyanDBConfig{
		Endpoint:      "localhost:17912",
		Group:         "ebpf-metrics",
		MeasureName:   "system-metrics",
		Timeout:       10 * time.Second,
		BatchSize:     1000,
		FlushInterval: 10 * time.Second,
	}
}

// BanyanDBExporter exports metrics to BanyanDB
type BanyanDBExporter struct {
	config  BanyanDBConfig
	logger  *zap.Logger
	conn    *grpc.ClientConn
	client  measurev1.MeasureServiceClient
	buffer  []*measurev1.DataPointValue
	lastErr error
}

// NewBanyanDBExporter creates a new BanyanDB exporter
func NewBanyanDBExporter(config BanyanDBConfig, logger *zap.Logger) (*BanyanDBExporter, error) {
	if config.Endpoint == "" {
		return nil, fmt.Errorf("BanyanDB endpoint is required")
	}
	if config.Group == "" {
		config.Group = "ebpf-metrics"
	}
	if config.MeasureName == "" {
		config.MeasureName = "system-metrics"
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 1000
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 10 * time.Second
	}

	return &BanyanDBExporter{
		config: config,
		logger: logger,
		buffer: make([]*measurev1.DataPointValue, 0, config.BatchSize),
	}, nil
}

// Connect establishes connection to BanyanDB
func (e *BanyanDBExporter) Connect(ctx context.Context) error {
	var err error
	e.conn, err = grpchelper.Conn(e.config.Endpoint, e.config.Timeout,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to BanyanDB at %s: %w", e.config.Endpoint, err)
	}

	e.client = measurev1.NewMeasureServiceClient(e.conn)
	e.logger.Info("Connected to BanyanDB", zap.String("endpoint", e.config.Endpoint))
	return nil
}

// Close closes the connection to BanyanDB
func (e *BanyanDBExporter) Close() error {
	if e.conn != nil {
		if err := e.Flush(context.Background()); err != nil {
			e.logger.Error("Failed to flush remaining metrics", zap.Error(err))
		}
		return e.conn.Close()
	}
	return nil
}

// Export sends metrics to BanyanDB
func (e *BanyanDBExporter) Export(ctx context.Context, store *metrics.Store) error {
	if e.client == nil {
		return fmt.Errorf("not connected to BanyanDB")
	}

	allMetrics := store.GetAll()
	now := time.Now()

	for moduleName, metricSet := range allMetrics {
		for _, metric := range metricSet.GetMetrics() {
			dp := e.createDataPoint(moduleName, metric, now)
			e.buffer = append(e.buffer, dp)

			// Flush if buffer is full
			if len(e.buffer) >= e.config.BatchSize {
				if err := e.Flush(ctx); err != nil {
					return err
				}
			}
		}
	}

	// Flush remaining if any
	if len(e.buffer) > 0 {
		return e.Flush(ctx)
	}

	return nil
}

// Flush sends buffered metrics to BanyanDB
func (e *BanyanDBExporter) Flush(ctx context.Context) error {
	if len(e.buffer) == 0 {
		return nil
	}

	req := &measurev1.WriteRequest{
		Metadata: &commonv1.Metadata{
			Name:  e.config.MeasureName,
			Group: e.config.Group,
		},
		DataPoint: &measurev1.DataPointValue{
			Timestamp: timestamppb.New(time.Now()),
			TagFamilies: []*modelv1.TagFamilyForWrite{
				{
					Tags: []*modelv1.TagValue{
						{
							Value: &modelv1.TagValue_Str{
								Str: &modelv1.Str{
									Value: "ebpf-sidecar",
								},
							},
						},
					},
				},
			},
			Fields: e.convertToFields(),
		},
	}

	// Create streaming client for batch write
	stream, err := e.client.Write(ctx)
	if err != nil {
		e.lastErr = err
		e.logger.Error("Failed to create write stream",
			zap.Error(err))
		return fmt.Errorf("failed to create write stream: %w", err)
	}

	// Send the write request through the stream
	if err := stream.Send(req); err != nil {
		e.lastErr = err
		e.logger.Error("Failed to send metrics to BanyanDB",
			zap.Error(err),
			zap.Int("batch_size", len(e.buffer)))
		return fmt.Errorf("failed to send metrics: %w", err)
	}

	// Close the send side to signal we're done sending
	if err := stream.CloseSend(); err != nil {
		e.lastErr = err
		e.logger.Error("Failed to close write stream",
			zap.Error(err))
		return fmt.Errorf("failed to close stream: %w", err)
	}

	// Receive response from the stream
	_, err = stream.Recv()
	if err != nil && err != io.EOF {
		e.lastErr = err
		e.logger.Error("Failed to receive write response",
			zap.Error(err))
		return fmt.Errorf("failed to receive response: %w", err)
	}

	e.logger.Debug("Successfully wrote metrics to BanyanDB",
		zap.Int("batch_size", len(e.buffer)))

	// Clear buffer after successful write
	e.buffer = e.buffer[:0]
	return nil
}

// createDataPoint creates a BanyanDB data point from a metric
func (e *BanyanDBExporter) createDataPoint(moduleName string, metric metrics.Metric, timestamp time.Time) *measurev1.DataPointValue {
	tags := []*modelv1.TagValue{
		{
			Value: &modelv1.TagValue_Str{
				Str: &modelv1.Str{
					Value: moduleName,
				},
			},
		},
		{
			Value: &modelv1.TagValue_Str{
				Str: &modelv1.Str{
					Value: metric.Name,
				},
			},
		},
	}

	// Add metric labels as tags
	for k, v := range metric.Labels {
		tags = append(tags, &modelv1.TagValue{
			Value: &modelv1.TagValue_Str{
				Str: &modelv1.Str{
					Value: fmt.Sprintf("%s=%s", k, v),
				},
			},
		})
	}

	return &measurev1.DataPointValue{
		Timestamp: timestamppb.New(timestamp),
		TagFamilies: []*modelv1.TagFamilyForWrite{
			{
				Tags: tags,
			},
		},
		Fields: []*modelv1.FieldValue{
			{
				Value: &modelv1.FieldValue_Float{
					Float: &modelv1.Float{
						Value: metric.Value,
					},
				},
			},
		},
	}
}

// convertToFields converts buffered data points to field values
func (e *BanyanDBExporter) convertToFields() []*modelv1.FieldValue {
	fields := make([]*modelv1.FieldValue, 0, len(e.buffer))
	
	// Aggregate metrics by name
	aggregated := make(map[string]float64)
	for _, dp := range e.buffer {
		if len(dp.Fields) > 0 {
			if floatField, ok := dp.Fields[0].Value.(*modelv1.FieldValue_Float); ok {
				// Extract metric name from tags
				if len(dp.TagFamilies) > 0 && len(dp.TagFamilies[0].Tags) > 1 {
					if strTag, ok := dp.TagFamilies[0].Tags[1].Value.(*modelv1.TagValue_Str); ok {
						metricName := strTag.Str.Value
						aggregated[metricName] += floatField.Float.Value
					}
				}
			}
		}
	}

	// Convert aggregated metrics to fields
	for _, value := range aggregated {
		fields = append(fields, &modelv1.FieldValue{
			Value: &modelv1.FieldValue_Float{
				Float: &modelv1.Float{
					Value: value,
				},
			},
		})
	}

	return fields
}

// GetLastError returns the last error encountered during export
func (e *BanyanDBExporter) GetLastError() error {
	return e.lastErr
}

// StartPeriodicExport starts periodic export to BanyanDB
func (e *BanyanDBExporter) StartPeriodicExport(ctx context.Context, store *metrics.Store) {
	ticker := time.NewTicker(e.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := e.Export(ctx, store); err != nil {
				e.logger.Error("Failed to export metrics", zap.Error(err))
			}
		}
	}
}