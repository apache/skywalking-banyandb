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

package trace

import (
	"context"
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

const idFormatV1 byte = 0x01

// IDDecode decodes idFormatV1 trace-ID payloads into the traceID column.
type IDDecode struct {
	schema *vectorized.BatchSchema
	closed bool
}

// NewIDDecode constructs a trace-ID decode fusible.
func NewIDDecode(schema *vectorized.BatchSchema) *IDDecode {
	return &IDDecode{schema: schema}
}

// Init is a no-op.
func (d *IDDecode) Init(context.Context) error {
	return nil
}

// OutputSchema returns the unchanged input schema.
func (d *IDDecode) OutputSchema() *vectorized.BatchSchema {
	return d.schema
}

// Process decodes every active row's payload into traceID.
func (d *IDDecode) Process(_ context.Context, batch *vectorized.RecordBatch) error {
	payloads := phase1Payloads(batch).Data()
	traceIDs := phase1TraceIDs(batch)
	for _, rowIdx := range activeIndices(batch) {
		traceID, decodeErr := decodeTraceIDPayload(payloads[rowIdx])
		if decodeErr != nil {
			return fmt.Errorf("decode trace id at row %d: %w", rowIdx, decodeErr)
		}
		traceIDs.SetAt(int(rowIdx), traceID)
	}
	return nil
}

// Close is idempotent and a no-op.
func (d *IDDecode) Close() error {
	if d.closed {
		return nil
	}
	d.closed = true
	return nil
}

func decodeTraceIDPayload(data []byte) (string, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("empty trace id payload")
	}
	if data[0] != idFormatV1 {
		return "", fmt.Errorf("invalid trace id format: %x", data[0])
	}
	return string(data[1:]), nil
}
