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

package panicdiag

import (
	"encoding/json"
	"fmt"
	"os"
)

const (
	defaultStateLimitBytes = 5 * 1024 * 1024
	deepDumpFileName       = "deep-dump.json"
)

// BoundedStateWriter writes JSON snapshots under a fixed size limit.
type BoundedStateWriter interface {
	WriteJSON(path string, value any, limitBytes int64) (truncated bool, err error)
}

type jsonStateWriter struct{}

// NewBoundedStateWriter returns the default bounded JSON state writer.
func NewBoundedStateWriter() BoundedStateWriter {
	return jsonStateWriter{}
}

// WriteJSON writes a formatted JSON document if it fits within the size limit.
func (jsonStateWriter) WriteJSON(path string, value any, limitBytes int64) (bool, error) {
	if limitBytes <= 0 {
		limitBytes = defaultStateLimitBytes
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return false, fmt.Errorf("marshal state dump: %w", err)
	}
	data = append(data, '\n')
	if int64(len(data)) > limitBytes {
		return true, fmt.Errorf("state dump exceeds limit: %d > %d", len(data), limitBytes)
	}
	if writeErr := os.WriteFile(path, data, 0o644); writeErr != nil {
		return false, fmt.Errorf("write state dump: %w", writeErr)
	}
	return false, nil
}
