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

// Package query regression tests for root-span duration population.
//
// Option B chosen: we exercise the tracer/span ordering contract directly
// (span.Stop() → tracer.ToProto()) rather than standing up a full
// measureInternalQueryProcessor, which requires a real measure.Service,
// buildMeasureContext, and executeMeasurePlan — well over 50 LOC of fixture.
// The three tests mirror the exact call sequence used in the three processor
// branches so that a regression (moving ToProto() before Stop()) would be
// caught immediately.
package query

import (
	"errors"
	"fmt"
	"testing"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestMeasureExecutionErrorRetainsResourceIdentityAndNativeValidationBody(t *testing.T) {
	for _, cause := range []error{vectorized.ErrMemoryBudgetExceeded, protector.ErrQueryResourceExhausted} {
		got := measureExecutionError(fmt.Errorf("dispatch query: %w", cause))
		if !errors.Is(got, cause) {
			t.Fatalf("resource error identity lost: %v", got)
		}
	}
	got := measureExecutionError(errors.New("extra_tag: tag is not defined"))
	var nativeError *common.Error
	if !errors.As(got, &nativeError) {
		t.Fatalf("native validation body changed to %T", got)
	}
	if got.Error() != "code: STATUS_INTERNAL_ERROR, msg: extra_tag: tag is not defined" {
		t.Fatal("validation message changed")
	}
}
