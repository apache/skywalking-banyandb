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

// Package tracestreaming provides stress testing tools for trace streaming performance.
package tracestreaming

import (
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
)

// Scale represents the test scale configuration.
type Scale string

const (
	// SmallScale represents a small scale test (5 min).
	SmallScale Scale = "small"
)

// ScaleConfig defines the configuration for each scale.
type ScaleConfig struct {
	TotalSpans       int
	TotalTraces      int
	Services         int
	ServiceInstances int
	TimeRange        time.Duration
	ErrorRate        float64
	OperationNames   int
	Components       int
}

// GetScaleConfig returns the configuration for a given scale.
func GetScaleConfig(scale Scale) ScaleConfig {
	switch scale {
	case SmallScale:
		return ScaleConfig{
			TotalSpans:       1000000, // 1M spans
			TotalTraces:      100000,  // 100K traces
			Services:         100,
			ServiceInstances: 500,
			TimeRange:        time.Hour,
			ErrorRate:        0.05,
			OperationNames:   1000,
			Components:       50,
		}
	default:
		return GetScaleConfig(SmallScale)
	}
}

// SpanData represents a single span with all its attributes.
type SpanData struct {
	StartTime         time.Time
	ServiceID         string
	ServiceInstanceID string
	TraceID           string
	SpanID            string
	ParentSpanID      string
	OperationName     string
	Component         string
	DataBinary        []byte
	Latency           time.Duration
	IsError           bool
}

// TraceData represents a complete trace with multiple spans.
type TraceData struct {
	TraceID string
	Spans   []*SpanData
}

// TraceGenerator generates realistic trace data for performance testing.
type TraceGenerator struct {
	baseTime     time.Time
	zipfServices *ZipfGenerator
	zipfOps      *ZipfGenerator
	zipfComps    *ZipfGenerator
	services     []string
	instances    []string
	operations   []string
	components   []string
	config       ScaleConfig
}

// NewTraceGenerator creates a new trace generator with the given scale.
func NewTraceGenerator(scale Scale) *TraceGenerator {
	config := GetScaleConfig(scale)

	// Generate service names
	services := make([]string, config.Services)
	for i := 0; i < config.Services; i++ {
		services[i] = fmt.Sprintf("service_%d", i)
	}

	// Generate service instance names
	instances := make([]string, config.ServiceInstances)
	for i := 0; i < config.ServiceInstances; i++ {
		instances[i] = fmt.Sprintf("instance_%d", i)
	}

	// Generate operation names
	operations := make([]string, config.OperationNames)
	for i := 0; i < config.OperationNames; i++ {
		operations[i] = fmt.Sprintf("/api/v1/endpoint_%d", i)
	}

	// Generate component names
	components := make([]string, config.Components)
	componentTypes := []string{"http", "mysql", "redis", "kafka", "grpc", "dubbo", "spring", "tomcat", "nginx", "postgresql"}
	for i := 0; i < config.Components; i++ {
		components[i] = fmt.Sprintf("%s_%d", componentTypes[i%len(componentTypes)], i/len(componentTypes))
	}

	// Create Zipfian generators for realistic distribution
	zipfServices := NewZipfGenerator(len(services), 1.2) // 80/20 distribution
	zipfOps := NewZipfGenerator(len(operations), 1.5)    // More skewed for operations
	zipfComps := NewZipfGenerator(len(components), 1.1)  // Less skewed for components

	return &TraceGenerator{
		config:       config,
		services:     services,
		instances:    instances,
		operations:   operations,
		components:   components,
		zipfServices: zipfServices,
		zipfOps:      zipfOps,
		zipfComps:    zipfComps,
		baseTime:     time.Now().Add(-config.TimeRange),
	}
}

// GenerateTrace generates a complete trace with the specified number of spans.
func (g *TraceGenerator) GenerateTrace(spanCount int) *TraceData {
	traceID := g.generateTraceID()
	spans := make([]*SpanData, spanCount)

	// Generate spans with parent-child relationships
	for i := 0; i < spanCount; i++ {
		var parentSpanID string
		if i > 0 {
			// Randomly select a parent span (simplified tree structure)
			parentIdx := g.randomInt(i)
			parentSpanID = spans[parentIdx].SpanID
		}

		spans[i] = g.GenerateSpan(traceID, parentSpanID)
	}

	return &TraceData{
		TraceID: traceID,
		Spans:   spans,
	}
}

// GenerateSpan generates a single span.
func (g *TraceGenerator) GenerateSpan(traceID, parentSpanID string) *SpanData {
	spanID := g.generateSpanID()

	// Select service and instance using Zipfian distribution
	serviceIdx := g.zipfServices.Next()
	instanceIdx := g.randomInt(len(g.instances))

	// Select operation and component using Zipfian distribution
	opIdx := g.zipfOps.Next()
	compIdx := g.zipfComps.Next()

	// Generate timing information
	startTime := g.generateStartTime()
	latency := g.generateLatency()

	// Determine if this is an error span
	isError := g.randomFloat() < g.config.ErrorRate

	// Generate data binary (simulated span data)
	dataBinary := g.generateDataBinary(spanID, isError)

	return &SpanData{
		ServiceID:         g.services[serviceIdx],
		ServiceInstanceID: g.instances[instanceIdx],
		TraceID:           traceID,
		SpanID:            spanID,
		ParentSpanID:      parentSpanID,
		StartTime:         startTime,
		Latency:           latency,
		IsError:           isError,
		OperationName:     g.operations[opIdx],
		Component:         g.components[compIdx],
		DataBinary:        dataBinary,
	}
}

// GenerateTraces generates multiple traces with realistic span counts.
func (g *TraceGenerator) GenerateTraces() []*TraceData {
	traces := make([]*TraceData, g.config.TotalTraces)

	for i := 0; i < g.config.TotalTraces; i++ {
		// Generate span count based on complexity distribution
		spanCount := g.generateSpanCount()
		traces[i] = g.GenerateTrace(spanCount)
	}

	return traces
}

// generateSpanCount generates span count based on complexity distribution.
func (g *TraceGenerator) generateSpanCount() int {
	rand := g.randomFloat()

	// 20% simple traces (1-5 spans)
	if rand < 0.2 {
		return g.randomInt(5) + 1
	}

	// 60% medium traces (6-20 spans)
	if rand < 0.8 {
		return g.randomInt(15) + 6
	}

	// 20% complex traces (21-100 spans)
	return g.randomInt(80) + 21
}

// generateStartTime generates a random start time within the configured time range.
func (g *TraceGenerator) generateStartTime() time.Time {
	offset := time.Duration(g.randomInt(int(g.config.TimeRange)))
	// Truncate to millisecond precision for BanyanDB compatibility
	return g.baseTime.Add(offset).Truncate(time.Millisecond)
}

// generateLatency generates a realistic latency using exponential distribution.
func (g *TraceGenerator) generateLatency() time.Duration {
	// Use exponential distribution with mean of 100ms
	lambda := 0.01 // 1/100ms
	latencyMs := -math.Log(1-g.randomFloat()) / lambda

	// Cap at 30 seconds
	if latencyMs > 30000 {
		latencyMs = 30000
	}

	return time.Duration(latencyMs) * time.Millisecond
}

// generateTraceID generates a unique trace ID.
func (g *TraceGenerator) generateTraceID() string {
	return fmt.Sprintf("trace_%d_%d", time.Now().UnixNano(), g.randomInt(1000000))
}

// generateSpanID generates a unique span ID.
func (g *TraceGenerator) generateSpanID() string {
	return fmt.Sprintf("span_%d_%d", time.Now().UnixNano(), g.randomInt(1000000))
}

// generateDataBinary generates simulated binary data for the span.
func (g *TraceGenerator) generateDataBinary(spanID string, isError bool) []byte {
	status := "success"
	if isError {
		status = "error"
	}

	// Create a realistic binary content structure
	data := fmt.Sprintf(`{"spanId":"%s","status":"%s","timestamp":%d}`,
		spanID, status, time.Now().UnixNano())

	return []byte(data)
}

// randomInt generates a random integer between 0 and maxVal-1.
func (g *TraceGenerator) randomInt(maxVal int) int {
	if maxVal <= 0 {
		return 0
	}

	// Use crypto/rand for better randomness
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(maxVal)))
	return int(n.Int64())
}

// randomFloat generates a random float between 0 and 1.
func (g *TraceGenerator) randomFloat() float64 {
	n := g.randomInt(1000000)
	return float64(n) / 1000000.0
}

// ToTraceWriteRequest converts a SpanData to a BanyanDB Trace write request.
func (s *SpanData) ToTraceWriteRequest() *tracev1.WriteRequest {
	var isError int64
	if s.IsError {
		isError = 1
	}

	return &tracev1.WriteRequest{
		Metadata: &commonv1.Metadata{
			Name:  "segment_trace",
			Group: "trace_streaming_test",
		},
		Tags: []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s.ServiceID}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s.ServiceInstanceID}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s.TraceID}}},
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(s.StartTime)}},
			{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: s.Latency.Milliseconds()}}},
			{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: isError}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s.SpanID}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s.ParentSpanID}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s.OperationName}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s.Component}}},
		},
		Span: s.DataBinary,
	}
}

// ZipfGenerator generates values following a Zipfian distribution.
type ZipfGenerator struct {
	n    int
	s    float64
	zeta float64
}

// NewZipfGenerator creates a new Zipfian generator.
func NewZipfGenerator(n int, s float64) *ZipfGenerator {
	if s <= 1.0 {
		s = 1.1 // minimum valid value
	}

	// Calculate normalization constant (Riemann zeta function)
	zeta := 0.0
	for i := 1; i <= n; i++ {
		zeta += 1.0 / math.Pow(float64(i), s)
	}

	return &ZipfGenerator{
		n:    n,
		s:    s,
		zeta: zeta,
	}
}

// Next generates the next Zipfian-distributed value.
func (z *ZipfGenerator) Next() int {
	if z.n <= 0 {
		return 0
	}

	// Generate random value between 0 and 1
	randVal := z.randomFloat()

	// Find the value using inverse transform sampling
	cumulative := 0.0
	for i := 1; i <= z.n; i++ {
		probability := (1.0 / math.Pow(float64(i), z.s)) / z.zeta
		cumulative += probability

		if randVal <= cumulative {
			return i - 1 // Convert to 0-based index
		}
	}

	// Fallback (shouldn't happen with proper math)
	return z.n - 1
}

// randomFloat generates a random float between 0 and 1.
func (z *ZipfGenerator) randomFloat() float64 {
	n, _ := rand.Int(rand.Reader, big.NewInt(1000000))
	return float64(n.Int64()) / 1000000.0
}
