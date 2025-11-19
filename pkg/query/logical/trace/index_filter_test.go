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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

type MockFilterOp struct {
	eqResults     map[string]map[string]bool
	havingResults map[string]map[string]bool
}

func NewMockFilterOp() *MockFilterOp {
	return &MockFilterOp{
		eqResults:     make(map[string]map[string]bool),
		havingResults: make(map[string]map[string]bool),
	}
}

func (m *MockFilterOp) Eq(tagName string, tagValue string) bool {
	if tagResults, ok := m.eqResults[tagName]; ok {
		if result, ok := tagResults[tagValue]; ok {
			return result
		}
	}
	return false
}

func (m *MockFilterOp) Range(_ string, _ index.RangeOpts) (bool, error) {
	return false, nil
}

func (m *MockFilterOp) Having(tagName string, tagValues []string) bool {
	if tagResults, ok := m.havingResults[tagName]; ok {
		key := ""
		for i, val := range tagValues {
			if i > 0 {
				key += ","
			}
			key += val
		}
		if result, ok := tagResults[key]; ok {
			return result
		}
	}
	return false
}

func (m *MockFilterOp) SetHavingResult(tagName string, tagValues []string, result bool) {
	if m.havingResults[tagName] == nil {
		m.havingResults[tagName] = make(map[string]bool)
	}
	key := ""
	for i, val := range tagValues {
		if i > 0 {
			key += ","
		}
		key += val
	}
	m.havingResults[tagName][key] = result
}

type MockLiteralExpr struct {
	stringValue string
	subExprs    []logical.LiteralExpr
	elements    []string
}

func NewMockLiteralExpr(stringValue string) *MockLiteralExpr {
	return &MockLiteralExpr{
		stringValue: stringValue,
		elements:    []string{stringValue},
	}
}

func NewMockLiteralExprWithSubExprs(subExprs []logical.LiteralExpr) *MockLiteralExpr {
	elements := make([]string, len(subExprs))
	for i, expr := range subExprs {
		elements[i] = expr.String()
	}
	return &MockLiteralExpr{
		subExprs: subExprs,
		elements: elements,
	}
}

func (m *MockLiteralExpr) String() string {
	return m.stringValue
}

func (m *MockLiteralExpr) Elements() []string {
	return m.elements
}

func (m *MockLiteralExpr) Equal(other logical.Expr) bool {
	if o, ok := other.(*MockLiteralExpr); ok {
		return m.stringValue == o.stringValue
	}
	return false
}

func (m *MockLiteralExpr) SubExprs() []logical.LiteralExpr {
	return m.subExprs
}

func (m *MockLiteralExpr) Bytes() [][]byte {
	result := make([][]byte, len(m.elements))
	for i, elem := range m.elements {
		result[i] = []byte(elem)
	}
	return result
}

func (m *MockLiteralExpr) Field(_ index.FieldKey) index.Field {
	return index.Field{}
}

func (m *MockLiteralExpr) RangeOpts(_ bool, _ bool, _ bool) index.RangeOpts {
	return index.RangeOpts{}
}

func TestTraceHavingFilterShouldSkip(t *testing.T) {
	tests := []struct {
		name           string
		tagName        string
		description    string
		subExprStrings []string
		havingResult   bool
		expectedSkip   bool
	}{
		{
			name:           "having matches - should not skip",
			tagName:        "service",
			subExprStrings: []string{"service-1", "service-2"},
			havingResult:   true,
			expectedSkip:   false,
			description:    "should not skip when Having returns true",
		},
		{
			name:           "having doesn't match - should skip",
			tagName:        "service",
			subExprStrings: []string{"service-1", "service-2"},
			havingResult:   false,
			expectedSkip:   true,
			description:    "should skip when Having returns false",
		},
		{
			name:           "single value matches - should not skip",
			tagName:        "endpoint",
			subExprStrings: []string{"/api/v1/users"},
			havingResult:   true,
			expectedSkip:   false,
			description:    "should not skip when single value Having returns true",
		},
		{
			name:           "multiple values no match - should skip",
			tagName:        "endpoint",
			subExprStrings: []string{"/api/v1/unknown", "/api/v2/unknown"},
			havingResult:   false,
			expectedSkip:   true,
			description:    "should skip when multiple values Having returns false",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock sub-expressions
			subExprs := make([]logical.LiteralExpr, len(tt.subExprStrings))
			for i, str := range tt.subExprStrings {
				subExprs[i] = NewMockLiteralExpr(str)
			}

			// Create mock expression
			mockExpr := NewMockLiteralExprWithSubExprs(subExprs)

			// Create trace having filter
			filter := &traceHavingFilter{
				expr:    mockExpr,
				op:      "having",
				tagName: tt.tagName,
			}

			// Create mock FilterOp
			mockFilterOp := NewMockFilterOp()
			mockFilterOp.SetHavingResult(tt.tagName, tt.subExprStrings, tt.havingResult)

			// Test ShouldSkip
			result, err := filter.ShouldSkip(mockFilterOp)

			assert.NoError(t, err, tt.description)
			assert.Equal(t, tt.expectedSkip, result, tt.description)
		})
	}
}

func TestTraceHavingFilterShouldSkipNilExpr(t *testing.T) {
	filter := &traceHavingFilter{
		expr:    nil,
		op:      "having",
		tagName: "service",
	}

	mockFilterOp := NewMockFilterOp()

	result, err := filter.ShouldSkip(mockFilterOp)

	assert.NoError(t, err)
	assert.False(t, result, "should not skip when expr is nil")
}

func TestTraceHavingFilterExecutePanics(t *testing.T) {
	filter := &traceHavingFilter{
		expr:    nil,
		op:      "having",
		tagName: "service",
	}

	assert.Panics(t, func() {
		filter.Execute(nil, 0, nil)
	}, "Execute should panic")
}

func TestTraceHavingFilterString(t *testing.T) {
	filter := &traceHavingFilter{
		expr:    nil,
		op:      "having",
		tagName: "service",
	}

	result := filter.String()
	assert.Equal(t, "having:service", result)
}

func TestTraceHavingFilterIntegration(t *testing.T) {
	// Integration test to ensure the filter works with realistic data

	// Create sub-expressions
	subExpr1 := NewMockLiteralExpr("order-service")
	subExpr2 := NewMockLiteralExpr("user-service")

	// Create main expression
	mockExpr := NewMockLiteralExprWithSubExprs([]logical.LiteralExpr{subExpr1, subExpr2})

	// Create filter
	filter := &traceHavingFilter{
		expr:    mockExpr,
		op:      "having",
		tagName: "service_name",
	}

	// Create mock FilterOp that simulates bloom filter behavior
	mockFilterOp := NewMockFilterOp()
	mockFilterOp.SetHavingResult("service_name", []string{"order-service", "user-service"}, true)

	// Test
	shouldSkip, err := filter.ShouldSkip(mockFilterOp)

	assert.NoError(t, err)
	assert.False(t, shouldSkip, "should not skip when services are found")
}
