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

package streamvstrace

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
)

// Scale represents the test scale configuration.
type Scale string

const (
	// SmallScale represents a small scale test.
	SmallScale Scale = "small"
	// MediumScale represents a medium scale test.
	MediumScale Scale = "medium"
	// LargeScale represents a large scale test.
	LargeScale Scale = "large"
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
			TotalSpans:       100000,
			TotalTraces:      10000,
			Services:         100,
			ServiceInstances: 500,
			TimeRange:        time.Hour,
			ErrorRate:        0.05,
			OperationNames:   1000,
			Components:       50,
		}
	case MediumScale:
		return ScaleConfig{
			TotalSpans:       10000000,
			TotalTraces:      1000000,
			Services:         1000,
			ServiceInstances: 5000,
			TimeRange:        24 * time.Hour,
			ErrorRate:        0.05,
			OperationNames:   1000,
			Components:       50,
		}
	case LargeScale:
		return ScaleConfig{
			TotalSpans:       1000000000,
			TotalTraces:      100000000,
			Services:         10000,
			ServiceInstances: 50000,
			TimeRange:        7 * 24 * time.Hour,
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

// SpanGenerator generates realistic span data for performance testing.
type SpanGenerator struct {
	baseTime     time.Time
	rand         *big.Int
	zipfServices *ZipfGenerator
	zipfOps      *ZipfGenerator
	zipfComps    *ZipfGenerator
	services     []string
	instances    []string
	operations   []string
	components   []string
	config       ScaleConfig
}

// NewSpanGenerator creates a new span generator with the given scale.
func NewSpanGenerator(scale Scale) *SpanGenerator {
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

	return &SpanGenerator{
		config:       config,
		services:     services,
		instances:    instances,
		operations:   operations,
		components:   components,
		zipfServices: zipfServices,
		zipfOps:      zipfOps,
		zipfComps:    zipfComps,
		baseTime:     time.Now().Add(-config.TimeRange),
		rand:         big.NewInt(0),
	}
}

// GenerateTrace generates a complete trace with the specified number of spans.
func (g *SpanGenerator) GenerateTrace(spanCount int) *TraceData {
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
func (g *SpanGenerator) GenerateSpan(traceID, parentSpanID string) *SpanData {
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
func (g *SpanGenerator) GenerateTraces() []*TraceData {
	traces := make([]*TraceData, g.config.TotalTraces)

	for i := 0; i < g.config.TotalTraces; i++ {
		// Generate span count based on complexity distribution
		spanCount := g.generateSpanCount()
		traces[i] = g.GenerateTrace(spanCount)
	}

	return traces
}

// generateSpanCount generates span count based on complexity distribution.
func (g *SpanGenerator) generateSpanCount() int {
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
func (g *SpanGenerator) generateStartTime() time.Time {
	offset := time.Duration(g.randomInt(int(g.config.TimeRange)))
	// Truncate to millisecond precision for BanyanDB compatibility
	return g.baseTime.Add(offset).Truncate(time.Millisecond)
}

// generateLatency generates a realistic latency using exponential distribution.
func (g *SpanGenerator) generateLatency() time.Duration {
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
func (g *SpanGenerator) generateTraceID() string {
	return fmt.Sprintf("trace_%d_%d", time.Now().UnixNano(), g.randomInt(1000000))
}

// generateSpanID generates a unique span ID.
func (g *SpanGenerator) generateSpanID() string {
	return fmt.Sprintf("span_%d_%d", time.Now().UnixNano(), g.randomInt(1000000))
}

// generateDataBinary generates simulated binary data for the span.
// In SkyWalking, a span represents a collection of all local function invocations in a process.
func (g *SpanGenerator) generateDataBinary(spanID string, isError bool) []byte {
	// Generate a realistic span structure with multiple local function calls
	spanData := g.generateRealisticSpanData(spanID, isError)

	// Serialize to JSON-like binary format (simulating SkyWalking's segment format)
	data := fmt.Sprintf(`{
		"segmentId": "%s",
		"serviceId": "%s",
		"serviceInstanceId": "%s",
		"spans": %s,
		"globalTraceIds": ["%s"],
		"isSizeLimited": false,
		"segmentSize": %d
	}`,
		spanID,
		g.services[g.randomInt(len(g.services))],
		g.instances[g.randomInt(len(g.instances))],
		spanData,
		g.generateTraceID(),
		len(spanData))

	return []byte(data)
}

// generateRealisticSpanData creates a comprehensive span structure with local function calls.
func (g *SpanGenerator) generateRealisticSpanData(_ string, isError bool) string {
	// Generate 3-8 local spans representing function calls within the process
	numLocalSpans := g.randomInt(6) + 3 // 3-8 local spans

	var spans []string
	spanStartTime := time.Now().UnixNano()

	// Generate the main entry span
	entrySpan := g.generateEntrySpan(spanStartTime, isError)
	spans = append(spans, entrySpan)

	// Generate local spans representing function calls
	for i := 0; i < numLocalSpans-1; i++ {
		localSpan := g.generateLocalSpan(spanStartTime, i, isError)
		spans = append(spans, localSpan)
	}

	return fmt.Sprintf("[%s]", strings.Join(spans, ","))
}

// generateEntrySpan creates the main entry span for the process.
func (g *SpanGenerator) generateEntrySpan(startTime int64, isError bool) string {
	operationName := g.operations[g.zipfOps.Next()]
	component := g.components[g.zipfComps.Next()]

	// Generate realistic timing
	latency := g.generateLatency()
	endTime := startTime + latency.Nanoseconds()

	// Generate tags for the entry span
	tags := g.generateSpanTags(operationName, component, isError)

	// Generate logs for the entry span
	logs := g.generateSpanLogs(startTime, endTime, isError)

	return fmt.Sprintf(`{
		"spanId": %d,
		"parentSpanId": -1,
		"segmentSpanId": 0,
		"refs": [],
		"operationName": "%s",
		"peer": "localhost:8080",
		"spanType": "Entry",
		"spanLayer": "Http",
		"componentId": %d,
		"component": "%s",
		"isError": %t,
		"tags": %s,
		"logs": %s,
		"startTime": %d,
		"endTime": %d,
		"endpointName": "%s",
		"endpointId": %d,
		"dataBinary": "%s"
	}`,
		g.randomInt(1000000),
		operationName,
		g.randomInt(100),
		component,
		isError,
		tags,
		logs,
		startTime,
		endTime,
		operationName,
		g.randomInt(1000000),
		g.generateDataBinaryContent(operationName, isError))
}

// generateLocalSpan creates a local span representing a function call.
func (g *SpanGenerator) generateLocalSpan(traceStartTime int64, index int, isError bool) string {
	// Generate function names that represent typical local operations
	functionNames := []string{
		"processRequest", "validateInput", "parseData", "transformData",
		"executeBusinessLogic", "saveToDatabase", "sendResponse", "cleanup",
		"authenticateUser", "authorizeRequest", "logActivity", "updateCache",
		"calculateMetrics", "formatOutput", "handleException", "retryOperation",
	}

	functionName := functionNames[g.randomInt(len(functionNames))]
	component := g.components[g.zipfComps.Next()]

	// Generate timing for local span (nested within the main span)
	spanOffset := time.Duration(g.randomInt(100)) * time.Millisecond
	startTime := traceStartTime + spanOffset.Nanoseconds()
	latency := g.generateLatency()
	endTime := startTime + latency.Nanoseconds()

	// Generate tags for the local span
	tags := g.generateLocalSpanTags(functionName, component, isError)

	// Generate logs for the local span
	logs := g.generateSpanLogs(startTime, endTime, isError)

	return fmt.Sprintf(`{
		"spanId": %d,
		"parentSpanId": 0,
		"segmentSpanId": %d,
		"refs": [],
		"operationName": "%s",
		"peer": "",
		"spanType": "Local",
		"spanLayer": "Unknown",
		"componentId": %d,
		"component": "%s",
		"isError": %t,
		"tags": %s,
		"logs": %s,
		"startTime": %d,
		"endTime": %d,
		"endpointName": "%s",
		"endpointId": %d,
		"dataBinary": "%s"
	}`,
		g.randomInt(1000000),
		index+1,
		functionName,
		g.randomInt(100),
		component,
		isError,
		tags,
		logs,
		startTime,
		endTime,
		functionName,
		g.randomInt(1000000),
		g.generateDataBinaryContent(functionName, isError))
}

// generateSpanTags creates realistic tags for a span.
func (g *SpanGenerator) generateSpanTags(operationName, component string, isError bool) string {
	tags := []string{
		fmt.Sprintf(`{"key": "http.method", "value": "%s"}`, g.getRandomHTTPMethod()),
		fmt.Sprintf(`{"key": "http.url", "value": "%s"}`, operationName),
		fmt.Sprintf(`{"key": "component", "value": "%s"}`, component),
		fmt.Sprintf(`{"key": "layer", "value": "%s"}`, g.getRandomLayer()),
	}

	if isError {
		tags = append(tags, `{"key": "error", "value": "true"}`, fmt.Sprintf(`{"key": "error.message", "value": "%s"}`, g.getRandomErrorMessage()))
	}

	// Add some random business tags
	businessTags := []string{
		`{"key": "user.id", "value": "12345"}`,
		`{"key": "request.id", "value": "req-12345"}`,
		`{"key": "session.id", "value": "sess-67890"}`,
		`{"key": "tenant.id", "value": "tenant-001"}`,
	}

	// Randomly select 2-4 business tags
	numBusinessTags := g.randomInt(3) + 2
	for i := 0; i < numBusinessTags && i < len(businessTags); i++ {
		tags = append(tags, businessTags[i])
	}

	return fmt.Sprintf("[%s]", strings.Join(tags, ","))
}

// generateLocalSpanTags creates tags specific to local function calls.
func (g *SpanGenerator) generateLocalSpanTags(functionName, component string, isError bool) string {
	tags := []string{
		fmt.Sprintf(`{"key": "function.name", "value": "%s"}`, functionName),
		fmt.Sprintf(`{"key": "component", "value": "%s"}`, component),
		`{"key": "span.type", "value": "Local"}`,
		`{"key": "thread.name", "value": "main-thread"}`,
	}

	if isError {
		tags = append(tags, `{"key": "error", "value": "true"}`, fmt.Sprintf(`{"key": "error.message", "value": "%s"}`, g.getRandomErrorMessage()))
	}

	// Add function-specific tags
	functionTags := []string{
		`{"key": "function.line", "value": "42"}`,
		`{"key": "function.class", "value": "com.example.Service"}`,
		`{"key": "function.package", "value": "com.example.service"}`,
	}

	// Randomly select 1-2 function tags
	numFunctionTags := g.randomInt(2) + 1
	for i := 0; i < numFunctionTags && i < len(functionTags); i++ {
		tags = append(tags, functionTags[i])
	}

	return fmt.Sprintf("[%s]", strings.Join(tags, ","))
}

// generateSpanLogs creates realistic logs for a span.
func (g *SpanGenerator) generateSpanLogs(startTime, endTime int64, isError bool) string {
	logs := []string{
		fmt.Sprintf(`{
			"time": %d,
			"data": [
				{"key": "event", "value": "span_start"},
				{"key": "message", "value": "Starting operation"}
			]
		}`, startTime),
	}

	// Add middle logs
	middleTime := startTime + (endTime-startTime)/2
	logs = append(logs, fmt.Sprintf(`{
		"time": %d,
		"data": [
			{"key": "event", "value": "processing"},
			{"key": "message", "value": "Processing request"},
			{"key": "progress", "value": "50%%"}
		]
	}`, middleTime))

	// Add end log
	if isError {
		logs = append(logs, fmt.Sprintf(`{
			"time": %d,
			"data": [
				{"key": "event", "value": "error"},
				{"key": "message", "value": "Operation failed"},
				{"key": "error.code", "value": "500"}
			]
		}`, endTime))
	} else {
		logs = append(logs, fmt.Sprintf(`{
			"time": %d,
			"data": [
				{"key": "event", "value": "span_end"},
				{"key": "message", "value": "Operation completed successfully"}
			]
		}`, endTime))
	}

	return fmt.Sprintf("[%s]", strings.Join(logs, ","))
}

// generateDataBinaryContent creates the actual binary content for the span.
func (g *SpanGenerator) generateDataBinaryContent(operationName string, isError bool) string {
	status := "success"
	if isError {
		status = "error"
	}

	// Create a more realistic binary content structure
	content := fmt.Sprintf(`{
		"operationName": "%s",
		"status": "%s",
		"timestamp": %d,
		"duration": %d,
		"metadata": {
			"version": "1.0",
			"source": "skywalking-agent",
			"collector": "banyandb"
		},
		"metrics": {
			"cpu_usage": %.2f,
			"memory_usage": %d,
			"gc_count": %d,
			"thread_count": %d
		},
		"context": {
			"trace_id": "%s",
			"span_id": "%s",
			"parent_span_id": "%s"
		}
	}`,
		operationName,
		status,
		time.Now().UnixNano(),
		g.randomInt(1000),
		g.randomFloat()*100,
		g.randomInt(1000000),
		g.randomInt(100),
		g.randomInt(50),
		g.generateTraceID(),
		g.generateSpanID(),
		g.generateSpanID())

	return content
}

// Helper functions for generating random values.
func (g *SpanGenerator) getRandomHTTPMethod() string {
	methods := []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}
	return methods[g.randomInt(len(methods))]
}

func (g *SpanGenerator) getRandomLayer() string {
	layers := []string{"Http", "Database", "Cache", "MQ", "RPC", "Unknown"}
	return layers[g.randomInt(len(layers))]
}

func (g *SpanGenerator) getRandomErrorMessage() string {
	errors := []string{
		"Database connection failed",
		"Invalid input parameters",
		"Service unavailable",
		"Timeout occurred",
		"Authentication failed",
		"Resource not found",
		"Internal server error",
		"Network timeout",
	}
	return errors[g.randomInt(len(errors))]
}

// randomInt generates a random integer between 0 and maxVal-1.
func (g *SpanGenerator) randomInt(maxVal int) int {
	if maxVal <= 0 {
		return 0
	}

	// Use crypto/rand for better randomness
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(maxVal)))
	return int(n.Int64())
}

// randomFloat generates a random float between 0 and 1.
func (g *SpanGenerator) randomFloat() float64 {
	n := g.randomInt(1000000)
	return float64(n) / 1000000.0
}

// ToStreamWriteRequest converts a SpanData to a BanyanDB Stream write request.
func (s *SpanData) ToStreamWriteRequest() *streamv1.WriteRequest {
	var isError int64
	if s.IsError {
		isError = 1
	}

	return &streamv1.WriteRequest{
		Metadata: &commonv1.Metadata{
			Name:  "segment_stream",
			Group: "stream_performance_test",
		},
		Element: &streamv1.ElementValue{
			ElementId: s.SpanID,
			Timestamp: timestamppb.New(s.StartTime.Truncate(time.Millisecond)),
			TagFamilies: []*modelv1.TagFamilyForWrite{
				{
					// Primary tag family
					Tags: []*modelv1.TagValue{
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s.ServiceID}}},
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s.ServiceInstanceID}}},
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s.TraceID}}},
						{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: s.StartTime.UnixMilli()}}},
						{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: s.Latency.Milliseconds()}}},
						{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: isError}}},
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s.SpanID}}},
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s.ParentSpanID}}},
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s.OperationName}}},
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s.Component}}},
					},
				},
				{
					// Data binary tag family
					Tags: []*modelv1.TagValue{
						{Value: &modelv1.TagValue_BinaryData{BinaryData: s.DataBinary}},
					},
				},
			},
		},
	}
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
			Group: "trace_performance_test",
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
		Span: s.DataBinary, // For trace model, span data goes in the span field
	}
}

// WriteStreamData writes span data to the stream service.
func (c *StreamClient) WriteStreamData(ctx context.Context, spans []*SpanData) error {
	stream, err := c.Write(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to create stream write client: %w", err)
	}

	for i, span := range spans {
		req := span.ToStreamWriteRequest()
		req.MessageId = uint64(i + 1)

		if sendErr := stream.Send(req); sendErr != nil {
			return fmt.Errorf("failed to send stream write request: %w", sendErr)
		}
	}

	// Close send and wait for final response
	if closeErr := stream.CloseSend(); closeErr != nil {
		return fmt.Errorf("failed to close stream send: %w", closeErr)
	}

	// Wait for final response
	for i := 0; i < len(spans); i++ {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive final response: %w", err)
		}
	}
	return nil
}

// WriteTraceData writes span data to the trace service.
func (c *TraceClient) WriteTraceData(ctx context.Context, spans []*SpanData) error {
	stream, err := c.Write(ctx)
	if err != nil {
		return fmt.Errorf("failed to create trace write client: %w", err)
	}

	for i, span := range spans {
		req := span.ToTraceWriteRequest()
		req.Version = uint64(i + 1)

		if sendErr := stream.Send(req); sendErr != nil {
			return fmt.Errorf("failed to send trace write request: %w", sendErr)
		}
	}

	// Close send and wait for final response
	if closeErr := stream.CloseSend(); closeErr != nil {
		return fmt.Errorf("failed to close trace send: %w", closeErr)
	}

	// Wait for final response
	for i := 0; i < len(spans); i++ {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive final response: %w", err)
		}
	}

	return nil
}
