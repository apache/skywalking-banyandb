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

package grpc

import (
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

type metrics struct {
	totalStarted  meter.Counter
	totalFinished meter.Counter
	totalErr      meter.Counter
	totalPanic    meter.Counter
	totalLatency  meter.Counter

	totalStreamStarted  meter.Counter
	totalStreamFinished meter.Counter
	totalStreamErr      meter.Counter
	totalStreamLatency  meter.Counter

	totalStreamMsgReceived    meter.Counter
	totalStreamMsgReceivedErr meter.Counter
	totalStreamMsgSent        meter.Counter
	totalStreamMsgSentErr     meter.Counter

	totalRegistryStarted  meter.Counter
	totalRegistryFinished meter.Counter
	totalRegistryErr      meter.Counter
	totalRegistryLatency  meter.Counter
}

func newMetrics(factory *observability.Factory) *metrics {
	return &metrics{
		totalStarted:              factory.NewCounter("total_started", "group", "service", "method"),
		totalFinished:             factory.NewCounter("total_finished", "group", "service", "method"),
		totalErr:                  factory.NewCounter("total_err", "group", "service", "method"),
		totalPanic:                factory.NewCounter("total_panic"),
		totalLatency:              factory.NewCounter("total_latency", "group", "service", "method"),
		totalStreamStarted:        factory.NewCounter("total_stream_started", "service", "method"),
		totalStreamFinished:       factory.NewCounter("total_stream_finished", "service", "method"),
		totalStreamErr:            factory.NewCounter("total_stream_err", "service", "method"),
		totalStreamLatency:        factory.NewCounter("total_stream_latency", "service", "method"),
		totalStreamMsgReceived:    factory.NewCounter("total_stream_msg_received", "group", "service", "method"),
		totalStreamMsgReceivedErr: factory.NewCounter("total_stream_msg_received_err", "group", "service", "method"),
		totalStreamMsgSent:        factory.NewCounter("total_stream_msg_sent", "group", "service", "method"),
		totalStreamMsgSentErr:     factory.NewCounter("total_stream_msg_sent_err", "group", "service", "method"),
		totalRegistryStarted:      factory.NewCounter("total_registry_started", "group", "service", "method"),
		totalRegistryFinished:     factory.NewCounter("total_registry_finished", "group", "service", "method"),
		totalRegistryErr:          factory.NewCounter("total_registry_err", "group", "service", "method"),
		totalRegistryLatency:      factory.NewCounter("total_registry_latency", "group", "service", "method"),
	}
}
