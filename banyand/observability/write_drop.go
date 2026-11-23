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

package observability

import "github.com/apache/skywalking-banyandb/pkg/meter"

// NewWriteDropCounter returns <model>_write_dropped_total, the counter of the rows a write
// listener of model ("stream", "measure" or "trace") skipped, by group and reason (see
// common.DropReason). Such a row is logged and not reported to the writer, so this counter is
// where the loss shows. The name carries the model because the native meter provider keys a
// measure by its bare name, and every model counts the same labels. A nil registry returns a
// nil counter.
func NewWriteDropCounter(omr MetricsRegistry, model string) meter.Counter {
	if omr == nil {
		return nil
	}
	return omr.With(RootScope).NewCounter(model+"_write_dropped_total", "group", "reason")
}
