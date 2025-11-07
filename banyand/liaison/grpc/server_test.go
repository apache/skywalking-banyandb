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
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestGrpcBufferMemoryRatioFlagDefault verifies default value.
func TestGrpcBufferMemoryRatioFlagDefault(t *testing.T) {
	s := &server{
		streamSVC:      &streamService{},
		measureSVC:     &measureService{},
		traceSVC:       &traceService{},
		propertyServer: &propertyServer{},
	}
	fs := s.FlagSet()
	flag := fs.Lookup("grpc-buffer-memory-ratio")
	assert.NotNil(t, flag)
	assert.Equal(t, "0.1", flag.DefValue)
}

// TestGrpcBufferMemoryRatioValidation verifies validation logic.
func TestGrpcBufferMemoryRatioValidation(t *testing.T) {
	tests := []struct {
		name      string
		ratio     float64
		expectErr bool
	}{
		{"valid_ratio_0_1", 0.1, false},
		{"valid_ratio_0_5", 0.5, false},
		{"valid_ratio_1_0", 1.0, false},
		{"invalid_ratio_zero", 0.0, true},
		{"invalid_ratio_negative", -0.1, true},
		{"invalid_ratio_too_high", 1.1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &server{
				grpcBufferMemoryRatio: tt.ratio,
				host:                  "localhost",
				port:                  17912,
			}
			err := s.Validate()
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
