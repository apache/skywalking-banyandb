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

package storage

import (
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

type metrics struct {
	lastTickTime meter.Gauge
	totalSegRefs meter.Gauge

	totalRotationStarted  meter.Counter
	totalRotationFinished meter.Counter
	totalRotationErr      meter.Counter

	totalRetentionStarted        meter.Counter
	totalRetentionFinished       meter.Counter
	totalRetentionHasData        meter.Counter
	totalRetentionErr            meter.Counter
	totalRetentionHasDataLatency meter.Counter

	schedulerMetrics *observability.SchedulerMetrics
}

func newMetrics(factory *observability.Factory) *metrics {
	if factory == nil {
		return nil
	}
	return &metrics{
		lastTickTime:                 factory.NewGauge("last_tick_time"),
		totalSegRefs:                 factory.NewGauge("total_segment_refs"),
		totalRotationStarted:         factory.NewCounter("total_rotation_started"),
		totalRotationFinished:        factory.NewCounter("total_rotation_finished"),
		totalRotationErr:             factory.NewCounter("total_rotation_err"),
		totalRetentionStarted:        factory.NewCounter("total_retention_started"),
		totalRetentionFinished:       factory.NewCounter("total_retention_finished"),
		totalRetentionErr:            factory.NewCounter("total_retention_err"),
		totalRetentionHasDataLatency: factory.NewCounter("total_retention_has_data_latency"),
		totalRetentionHasData:        factory.NewCounter("total_retention_has_data"),
		schedulerMetrics:             observability.NewSchedulerMetrics(factory),
	}
}

func (d *database[T, O]) incTotalRotationStarted(delta int) {
	if d.metrics == nil {
		return
	}
	d.metrics.totalRotationStarted.Inc(float64(delta))
}

func (d *database[T, O]) incTotalRotationFinished(delta int) {
	if d.metrics == nil {
		return
	}
	d.metrics.totalRotationFinished.Inc(float64(delta))
}

func (d *database[T, O]) incTotalRotationErr(delta int) {
	if d.metrics == nil {
		return
	}
	d.metrics.totalRotationErr.Inc(float64(delta))
}

func (d *database[T, O]) incTotalRetentionStarted(delta int) {
	if d.metrics == nil {
		return
	}
	d.metrics.totalRetentionStarted.Inc(float64(delta))
}

func (d *database[T, O]) incTotalRetentionFinished(delta int) {
	if d.metrics == nil {
		return
	}
	d.metrics.totalRetentionFinished.Inc(float64(delta))
}

func (d *database[T, O]) incTotalRetentionHasData(delta int) {
	if d.metrics == nil {
		return
	}
	d.metrics.totalRetentionHasData.Inc(float64(delta))
}

func (d *database[T, O]) incTotalRetentionErr(delta int) {
	if d.metrics == nil {
		return
	}
	d.metrics.totalRetentionErr.Inc(float64(delta))
}

func (d *database[T, O]) incTotalRetentionHasDataLatency(delta float64) {
	if d.metrics == nil {
		return
	}
	d.metrics.totalRetentionHasDataLatency.Inc(delta)
}
