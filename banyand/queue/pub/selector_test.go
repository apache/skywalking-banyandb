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

package pub

import (
	"testing"
)

func TestParseLabelSelector(t *testing.T) {
	tests := []struct {
		want    *labelSelector
		name    string
		input   string
		wantErr bool
	}{
		{
			name:  "empty selector",
			input: "",
			want:  &labelSelector{},
		},
		{
			name:  "single equality",
			input: "key=value",
			want: &labelSelector{
				criteria: []condition{
					{Key: "key", Op: opEquals, Values: []string{"value"}},
				},
			},
		},
		{
			name:  "not equals",
			input: "key != value",
			want: &labelSelector{
				criteria: []condition{
					{Key: "key", Op: opNotEquals, Values: []string{"value"}},
				},
			},
		},
		{
			name:  "in clause",
			input: "key in (v1, v2)",
			want: &labelSelector{
				criteria: []condition{
					{Key: "key", Op: opIn, Values: []string{"v1", "v2"}},
				},
			},
		},
		{
			name:  "not in clause",
			input: "key notin (v1,v2)",
			want: &labelSelector{
				criteria: []condition{
					{Key: "key", Op: opNotIn, Values: []string{"v1", "v2"}},
				},
			},
		},
		{
			name:  "exists",
			input: "key",
			want: &labelSelector{
				criteria: []condition{
					{Key: "key", Op: opExists},
				},
			},
		},
		{
			name:  "not exists",
			input: "!key",
			want: &labelSelector{
				criteria: []condition{
					{Key: "key", Op: opNotExists},
				},
			},
		},
		{
			name:  "multiple conditions",
			input: "key1=value1,key2 in (v1, v2),!key3",
			want: &labelSelector{
				criteria: []condition{
					{Key: "key1", Op: opEquals, Values: []string{"value1"}},
					{Key: "key2", Op: opIn, Values: []string{"v1", "v2"}},
					{Key: "key3", Op: opNotExists},
				},
			},
		},
		{
			name:    "invalid key",
			input:   "INVALID=value",
			wantErr: true,
		},
		{
			name:    "invalid operator",
			input:   "key~value",
			wantErr: true,
		},
		{
			name:    "in without parentheses",
			input:   "key in v1,v2",
			wantErr: true,
		},
		{
			name:    "empty value list",
			input:   "key in ()",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseLabelSelector(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseLabelSelector() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if len(got.criteria) != len(tt.want.criteria) {
				t.Fatalf("expected %d conditions, got %d", len(tt.want.criteria), len(got.criteria))
			}
			for i, cond := range got.criteria {
				wantCond := tt.want.criteria[i]
				if cond.Key != wantCond.Key || cond.Op != wantCond.Op {
					t.Errorf("condition %d: got (%s, %d), want (%s, %d)", i, cond.Key, cond.Op, wantCond.Key, wantCond.Op)
				}
				if len(cond.Values) != len(wantCond.Values) {
					t.Errorf("condition %d: values length mismatch: got %d, want %d", i, len(cond.Values), len(wantCond.Values))
					continue
				}
				for j, v := range cond.Values {
					if v != wantCond.Values[j] {
						t.Errorf("condition %d value %d: got %s, want %s", i, j, v, wantCond.Values[j])
					}
				}
			}
		})
	}
}

func TestValidateLabelKey(t *testing.T) {
	tests := []struct {
		key     string
		wantErr bool
	}{
		{"valid-key", false},
		{"key.with.dots", false},
		{"key_with_underscore", false},
		{"123key", false},
		{"", true},
		{"InvalidKey", true},     // uppercase
		{"key/with/slash", true}, // invalid character
		{"key with space", true},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			err := validateLabelKey(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateLabelKey(%q) error = %v, wantErr %v", tt.key, err, tt.wantErr)
			}
		})
	}
}

func TestConditionMatches(t *testing.T) {
	tests := []struct {
		labels    map[string]string
		name      string
		condition condition
		want      bool
	}{
		{
			name:      "exists true",
			condition: condition{Key: "k", Op: opExists},
			labels:    map[string]string{"k": "v"},
			want:      true,
		},
		{
			name:      "exists false",
			condition: condition{Key: "k", Op: opExists},
			labels:    map[string]string{"other": "v"},
			want:      false,
		},
		{
			name:      "not exists true",
			condition: condition{Key: "k", Op: opNotExists},
			labels:    map[string]string{"other": "v"},
			want:      true,
		},
		{
			name:      "not exists false",
			condition: condition{Key: "k", Op: opNotExists},
			labels:    map[string]string{"k": "v"},
			want:      false,
		},
		{
			name:      "equals match",
			condition: condition{Key: "k", Op: opEquals, Values: []string{"v"}},
			labels:    map[string]string{"k": "v"},
			want:      true,
		},
		{
			name:      "equals no match",
			condition: condition{Key: "k", Op: opEquals, Values: []string{"v"}},
			labels:    map[string]string{"k": "x"},
			want:      false,
		},
		{
			name:      "equals key missing",
			condition: condition{Key: "k", Op: opEquals, Values: []string{"v"}},
			labels:    map[string]string{"other": "v"},
			want:      false,
		},
		{
			name:      "not equals match",
			condition: condition{Key: "k", Op: opNotEquals, Values: []string{"v"}},
			labels:    map[string]string{"k": "x"},
			want:      true,
		},
		{
			name:      "not equals key missing",
			condition: condition{Key: "k", Op: opNotEquals, Values: []string{"v"}},
			labels:    map[string]string{"other": "v"},
			want:      true,
		},
		{
			name:      "not equals no match",
			condition: condition{Key: "k", Op: opNotEquals, Values: []string{"v"}},
			labels:    map[string]string{"k": "v"},
			want:      false,
		},
		{
			name:      "in match",
			condition: condition{Key: "k", Op: opIn, Values: []string{"v1", "v2"}},
			labels:    map[string]string{"k": "v1"},
			want:      true,
		},
		{
			name:      "in no match",
			condition: condition{Key: "k", Op: opIn, Values: []string{"v1", "v2"}},
			labels:    map[string]string{"k": "v3"},
			want:      false,
		},
		{
			name:      "in key missing",
			condition: condition{Key: "k", Op: opIn, Values: []string{"v1", "v2"}},
			labels:    map[string]string{"other": "v1"},
			want:      false,
		},
		{
			name:      "not in match",
			condition: condition{Key: "k", Op: opNotIn, Values: []string{"v1", "v2"}},
			labels:    map[string]string{"k": "v3"},
			want:      true,
		},
		{
			name:      "not in no match",
			condition: condition{Key: "k", Op: opNotIn, Values: []string{"v1", "v2"}},
			labels:    map[string]string{"k": "v1"},
			want:      false,
		},
		{
			name:      "not in key missing",
			condition: condition{Key: "k", Op: opNotIn, Values: []string{"v1", "v2"}},
			labels:    map[string]string{"other": "v1"},
			want:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.condition.matches(tt.labels)
			if got != tt.want {
				t.Errorf("matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLabelSelectorMatches(t *testing.T) {
	tests := []struct {
		selector *labelSelector
		labels   map[string]string
		name     string
		want     bool
	}{
		{
			name: "all conditions met",
			selector: &labelSelector{
				criteria: []condition{
					{Key: "k1", Op: opExists},
					{Key: "k2", Op: opEquals, Values: []string{"v2"}},
					{Key: "k3", Op: opNotIn, Values: []string{"v3"}},
				},
			},
			labels: map[string]string{
				"k1": "v1",
				"k2": "v2",
				"k3": "v4",
			},
			want: true,
		},
		{
			name: "one condition fails",
			selector: &labelSelector{
				criteria: []condition{
					{Key: "k1", Op: opExists},
					{Key: "k2", Op: opEquals, Values: []string{"v2"}},
				},
			},
			labels: map[string]string{
				"k1": "v1",
				"k2": "v3",
			},
			want: false,
		},
		{
			name:     "empty selector",
			selector: &labelSelector{},
			labels:   map[string]string{"any": "value"},
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.selector.matches(tt.labels)
			if got != tt.want {
				t.Errorf("matches() = %v, want %v", got, tt.want)
			}
		})
	}
}
