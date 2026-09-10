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

package dns

import (
	"testing"

	"github.com/rs/zerolog"
)

func TestDNSFailureLevel(t *testing.T) {
	tests := []struct {
		name        string
		failures    int
		want        zerolog.Level
		inInitPhase bool
	}{
		{name: "first failure in the init phase", inInitPhase: true, failures: 1, want: zerolog.WarnLevel},
		{
			name:        "a run of failures in the init phase is still expected",
			inInitPhase: true,
			failures:    consecutiveDNSFailuresBeforeError,
			want:        zerolog.WarnLevel,
		},
		{name: "the init phase never escalates", inInitPhase: true, failures: 100, want: zerolog.WarnLevel},
		{name: "first failure after the init phase", failures: 1, want: zerolog.WarnLevel},
		{
			name:     "one short of the threshold",
			failures: consecutiveDNSFailuresBeforeError - 1,
			want:     zerolog.WarnLevel,
		},
		{
			name:     "the threshold escalates",
			failures: consecutiveDNSFailuresBeforeError,
			want:     zerolog.ErrorLevel,
		},
		{
			name:     "past the threshold stays escalated",
			failures: consecutiveDNSFailuresBeforeError + 1,
			want:     zerolog.ErrorLevel,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := dnsFailureLevel(tt.inInitPhase, tt.failures); got != tt.want {
				t.Errorf("dnsFailureLevel(%t, %d) = %v, want %v", tt.inInitPhase, tt.failures, got, tt.want)
			}
		})
	}
}
