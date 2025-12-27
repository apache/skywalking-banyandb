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

// Package testhelper provides test helpers for FODC agent components.
package testhelper

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/flightrecorder"
	agentmetrics "github.com/apache/skywalking-banyandb/fodc/agent/internal/metrics"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/proxy"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// NewFlightRecorder creates a new FlightRecorder instance for testing.
func NewFlightRecorder(capacitySize int64) *flightrecorder.FlightRecorder {
	return flightrecorder.NewFlightRecorder(capacitySize)
}

// RawMetric represents a metric for testing.
type RawMetric struct {
	Desc   string
	Name   string
	Labels []Label
	Value  float64
}

// Label represents a metric label.
type Label struct {
	Name  string
	Value string
}

// UpdateMetrics updates the FlightRecorder with metrics.
// fr should be a *flightrecorder.FlightRecorder instance.
func UpdateMetrics(fr interface{}, rawMetrics []RawMetric) error {
	frTyped, ok := fr.(*flightrecorder.FlightRecorder)
	if !ok {
		return fmt.Errorf("invalid FlightRecorder type")
	}
	// Convert test RawMetric to internal RawMetric
	internalMetrics := make([]agentmetrics.RawMetric, len(rawMetrics))
	for idx := range rawMetrics {
		internalLabels := make([]agentmetrics.Label, len(rawMetrics[idx].Labels))
		for labelIdx := range rawMetrics[idx].Labels {
			internalLabels[labelIdx] = agentmetrics.Label{
				Name:  rawMetrics[idx].Labels[labelIdx].Name,
				Value: rawMetrics[idx].Labels[labelIdx].Value,
			}
		}
		internalMetrics[idx] = agentmetrics.RawMetric{
			Name:   rawMetrics[idx].Name,
			Value:  rawMetrics[idx].Value,
			Desc:   rawMetrics[idx].Desc,
			Labels: internalLabels,
		}
	}
	return frTyped.Update(internalMetrics)
}

// NewProxyClient creates a new ProxyClient instance for testing.
func NewProxyClient(
	proxyAddr string,
	nodeIP string,
	nodePort int,
	nodeRole string,
	labels map[string]string,
	heartbeatInterval time.Duration,
	reconnectInterval time.Duration,
	flightRecorder interface{},
	logger *logger.Logger,
) *proxy.Client {
	frTyped, ok := flightRecorder.(*flightrecorder.FlightRecorder)
	if !ok {
		// This should not happen in normal usage, but handle gracefully
		return nil
	}
	return proxy.NewClient(
		proxyAddr,
		nodeIP,
		nodePort,
		nodeRole,
		labels,
		heartbeatInterval,
		reconnectInterval,
		frTyped,
		logger,
	)
}

// ProxyClientWrapper wraps ProxyClient methods for testing.
type ProxyClientWrapper struct {
	client *proxy.Client
}

// Connect establishes a gRPC connection to Proxy.
func (w *ProxyClientWrapper) Connect(ctx context.Context) error {
	return w.client.Connect(ctx)
}

// StartRegistrationStream establishes bi-directional registration stream with Proxy.
func (w *ProxyClientWrapper) StartRegistrationStream(ctx context.Context) error {
	return w.client.StartRegistrationStream(ctx)
}

// StartMetricsStream establishes bi-directional metrics stream with Proxy.
func (w *ProxyClientWrapper) StartMetricsStream(ctx context.Context) error {
	return w.client.StartMetricsStream(ctx)
}

// Disconnect closes connection to Proxy.
func (w *ProxyClientWrapper) Disconnect() error {
	return w.client.Disconnect()
}

// NewProxyClientWrapper creates a wrapped ProxyClient for testing.
func NewProxyClientWrapper(
	proxyAddr string,
	nodeIP string,
	nodePort int,
	nodeRole string,
	labels map[string]string,
	heartbeatInterval time.Duration,
	reconnectInterval time.Duration,
	flightRecorder interface{},
	logger *logger.Logger,
) *ProxyClientWrapper {
	client := NewProxyClient(
		proxyAddr,
		nodeIP,
		nodePort,
		nodeRole,
		labels,
		heartbeatInterval,
		reconnectInterval,
		flightRecorder,
		logger,
	)
	if client == nil {
		return nil
	}
	return &ProxyClientWrapper{client: client}
}
