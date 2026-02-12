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

package grpchelper

import (
	"errors"
	"fmt"
	"testing"
	"time"

	pkgerrors "github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// customUnwrapError wraps a single error via Unwrap() error.
type customUnwrapError struct {
	cause error
	msg   string
}

func (e *customUnwrapError) Error() string { return e.msg + ": " + e.cause.Error() }
func (e *customUnwrapError) Unwrap() error { return e.cause }

// multiUnwrapError wraps multiple errors via Unwrap() []error.
type multiUnwrapError struct {
	msg    string
	causes []error
}

func (e *multiUnwrapError) Error() string   { return e.msg }
func (e *multiUnwrapError) Unwrap() []error { return e.causes }

func TestJitteredBackoff(t *testing.T) {
	tests := []struct {
		name         string
		baseBackoff  time.Duration
		maxBackoff   time.Duration
		attempt      int
		jitterFactor float64
		expectedMin  time.Duration
		expectedMax  time.Duration
	}{
		{
			name:         "zero_attempt",
			baseBackoff:  100 * time.Millisecond,
			maxBackoff:   10 * time.Second,
			attempt:      0,
			jitterFactor: 0.2,
			expectedMin:  80 * time.Millisecond,
			expectedMax:  120 * time.Millisecond,
		},
		{
			name:         "first_attempt",
			baseBackoff:  100 * time.Millisecond,
			maxBackoff:   10 * time.Second,
			attempt:      1,
			jitterFactor: 0.2,
			expectedMin:  160 * time.Millisecond,
			expectedMax:  240 * time.Millisecond,
		},
		{
			name:         "max_backoff_reached",
			baseBackoff:  100 * time.Millisecond,
			maxBackoff:   300 * time.Millisecond,
			attempt:      10,
			jitterFactor: 0.2,
			expectedMin:  240 * time.Millisecond,
			expectedMax:  300 * time.Millisecond,
		},
		{
			name:         "no_jitter",
			baseBackoff:  100 * time.Millisecond,
			maxBackoff:   10 * time.Second,
			attempt:      0,
			jitterFactor: 0.0,
			expectedMin:  100 * time.Millisecond,
			expectedMax:  100 * time.Millisecond,
		},
		{
			name:         "max_jitter",
			baseBackoff:  100 * time.Millisecond,
			maxBackoff:   10 * time.Second,
			attempt:      0,
			jitterFactor: 1.0,
			expectedMin:  0,
			expectedMax:  200 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test multiple times to account for randomness
			for i := 0; i < 100; i++ {
				result := JitteredBackoff(tt.baseBackoff, tt.maxBackoff, tt.attempt, tt.jitterFactor)

				assert.GreaterOrEqual(t, result, tt.expectedMin,
					"result %v should be >= expected min %v", result, tt.expectedMin)
				assert.LessOrEqual(t, result, tt.expectedMax,
					"result %v should be <= expected max %v", result, tt.expectedMax)
			}
		})
	}
}

func TestJitteredBackoffEdgeCases(t *testing.T) {
	tests := []struct {
		name         string
		baseBackoff  time.Duration
		maxBackoff   time.Duration
		attempt      int
		jitterFactor float64
	}{
		{
			name:         "negative_jitter_factor",
			baseBackoff:  100 * time.Millisecond,
			maxBackoff:   10 * time.Second,
			attempt:      0,
			jitterFactor: -0.5,
		},
		{
			name:         "jitter_factor_greater_than_one",
			baseBackoff:  100 * time.Millisecond,
			maxBackoff:   10 * time.Second,
			attempt:      0,
			jitterFactor: 1.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not panic and should return a reasonable value
			result := JitteredBackoff(tt.baseBackoff, tt.maxBackoff, tt.attempt, tt.jitterFactor)
			assert.GreaterOrEqual(t, result, time.Duration(0), "result should be non-negative")
			assert.LessOrEqual(t, result, tt.maxBackoff, "result should not exceed max backoff")
		})
	}
}

func TestIsTransientError(t *testing.T) {
	tests := []struct {
		err         error
		name        string
		expectRetry bool
	}{
		{
			name:        "nil_error",
			err:         nil,
			expectRetry: false,
		},
		{
			name:        "unavailable_error",
			err:         status.Error(codes.Unavailable, "service unavailable"),
			expectRetry: true,
		},
		{
			name:        "deadline_exceeded_error",
			err:         status.Error(codes.DeadlineExceeded, "deadline exceeded"),
			expectRetry: true,
		},
		{
			name:        "resource_exhausted_error",
			err:         status.Error(codes.ResourceExhausted, "rate limited"),
			expectRetry: true,
		},
		{
			name:        "internal_error",
			err:         status.Error(codes.Internal, "internal"),
			expectRetry: true,
		},
		{
			name:        "not_found_error",
			err:         status.Error(codes.NotFound, "not found"),
			expectRetry: false,
		},
		{
			name:        "permission_denied_error",
			err:         status.Error(codes.PermissionDenied, "permission denied"),
			expectRetry: false,
		},
		{
			name:        "invalid_argument_error",
			err:         status.Error(codes.InvalidArgument, "invalid argument"),
			expectRetry: false,
		},
		{
			name:        "ok_error",
			err:         status.Error(codes.OK, "ok"),
			expectRetry: false,
		},
		{
			name:        "canceled_error",
			err:         status.Error(codes.Canceled, "canceled"),
			expectRetry: false,
		},
		{
			name:        "unknown_error",
			err:         status.Error(codes.Unknown, "unknown"),
			expectRetry: false,
		},
		{
			name:        "already_exists_error",
			err:         status.Error(codes.AlreadyExists, "exists"),
			expectRetry: false,
		},
		{
			name:        "failed_precondition_error",
			err:         status.Error(codes.FailedPrecondition, "failed"),
			expectRetry: false,
		},
		{
			name:        "aborted_error",
			err:         status.Error(codes.Aborted, "aborted"),
			expectRetry: false,
		},
		{
			name:        "out_of_range_error",
			err:         status.Error(codes.OutOfRange, "range"),
			expectRetry: false,
		},
		{
			name:        "unimplemented_error",
			err:         status.Error(codes.Unimplemented, "unimpl"),
			expectRetry: false,
		},
		{
			name:        "data_loss_error",
			err:         status.Error(codes.DataLoss, "loss"),
			expectRetry: false,
		},
		{
			name:        "unauthenticated_error",
			err:         status.Error(codes.Unauthenticated, "unauth"),
			expectRetry: false,
		},
		{
			name:        "generic_error",
			err:         fmt.Errorf("generic error"),
			expectRetry: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsTransientError(tt.err)
			assert.Equal(t, tt.expectRetry, result, "transient error classification mismatch")
		})
	}
}

func TestIsFailoverError(t *testing.T) {
	tests := []struct {
		err            error
		name           string
		expectFailover bool
	}{
		{name: "nil_error", err: nil, expectFailover: false},
		{name: "unavailable_error", err: status.Error(codes.Unavailable, "unavailable"), expectFailover: true},
		{name: "deadline_exceeded_error", err: status.Error(codes.DeadlineExceeded, "timeout"), expectFailover: true},
		{name: "internal_error", err: status.Error(codes.Internal, "internal"), expectFailover: false},
		{name: "invalid_argument_error", err: status.Error(codes.InvalidArgument, "bad"), expectFailover: false},
		{name: "ok_error", err: status.Error(codes.OK, "ok"), expectFailover: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsFailoverError(tt.err)
			assert.Equal(t, tt.expectFailover, result, "failover error classification mismatch")
		})
	}
}

func TestIsTransientErrorWrapped(t *testing.T) {
	grpcUnavailable := status.Error(codes.Unavailable, "unavailable")
	grpcInternal := status.Error(codes.Internal, "internal")
	grpcInvalidArg := status.Error(codes.InvalidArgument, "bad arg")
	grpcDeadline := status.Error(codes.DeadlineExceeded, "timeout")

	tests := []struct {
		err         error
		name        string
		expectRetry bool
	}{
		// direct gRPC status (baseline)
		{name: "direct_unavailable", err: grpcUnavailable, expectRetry: true},
		{name: "direct_internal", err: grpcInternal, expectRetry: true},
		{name: "direct_invalid_argument", err: grpcInvalidArg, expectRetry: false},
		{name: "direct_deadline_exceeded", err: grpcDeadline, expectRetry: true},
		// fmt.Errorf %w
		{name: "fmt_errorf_unavailable", err: fmt.Errorf("wrap: %w", grpcUnavailable), expectRetry: true},
		{name: "fmt_errorf_internal", err: fmt.Errorf("wrap: %w", grpcInternal), expectRetry: true},
		{name: "fmt_errorf_invalid_argument", err: fmt.Errorf("wrap: %w", grpcInvalidArg), expectRetry: false},
		{
			name: "fmt_errorf_double_wrapped",
			err:  fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", grpcDeadline)), expectRetry: true,
		},
		// errors.New with message only — no gRPC status in chain
		{name: "errors_new_with_grpc_message_text", err: errors.New(grpcUnavailable.Error()), expectRetry: false},
		// errors.Join
		{name: "errors_join_unavailable", err: errors.Join(errors.New("extra"), grpcUnavailable), expectRetry: true},
		{name: "errors_join_invalid_argument", err: errors.Join(errors.New("extra"), grpcInvalidArg), expectRetry: false},
		{
			name: "errors_join_multiple_grpc_errors",
			err:  errors.Join(grpcInvalidArg, grpcUnavailable), expectRetry: false,
		},
		// custom Unwrap() error
		{
			name: "custom_unwrap_internal",
			err:  &customUnwrapError{msg: "custom", cause: grpcInternal}, expectRetry: true,
		},
		{
			name: "custom_unwrap_invalid_argument",
			err:  &customUnwrapError{msg: "custom", cause: grpcInvalidArg}, expectRetry: false,
		},
		{
			name: "custom_unwrap_nested_in_fmt_errorf",
			err:  fmt.Errorf("outer: %w", &customUnwrapError{msg: "inner", cause: grpcUnavailable}), expectRetry: true,
		},
		// custom Unwrap() []error
		{
			name: "multi_unwrap_internal",
			err:  &multiUnwrapError{msg: "multi", causes: []error{errors.New("other"), grpcInternal}}, expectRetry: true,
		},
		{
			name: "multi_unwrap_all_non_retryable",
			err:  &multiUnwrapError{msg: "multi", causes: []error{errors.New("a"), grpcInvalidArg}}, expectRetry: false,
		},
		// pkg/errors Wrap and Wrapf
		{name: "pkgerrors_wrap_unavailable", err: pkgerrors.Wrap(grpcUnavailable, "wrap"), expectRetry: true},
		{name: "pkgerrors_wrapf_internal", err: pkgerrors.Wrapf(grpcInternal, "wrap %s", "ctx"), expectRetry: true},
		{name: "pkgerrors_wrap_invalid_argument", err: pkgerrors.Wrap(grpcInvalidArg, "wrap"), expectRetry: false},
		{
			name: "pkgerrors_wrap_double_wrapped",
			err:  pkgerrors.Wrap(pkgerrors.Wrap(grpcDeadline, "inner"), "outer"), expectRetry: true,
		},
		// plain errors — no gRPC status
		{name: "plain_errors_new", err: errors.New("plain error"), expectRetry: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsTransientError(tt.err)
			assert.Equal(t, tt.expectRetry, result, "transient error classification mismatch")
		})
	}
}

func TestIsFailoverErrorWrapped(t *testing.T) {
	grpcUnavailable := status.Error(codes.Unavailable, "unavailable")
	grpcDeadline := status.Error(codes.DeadlineExceeded, "timeout")
	grpcInternal := status.Error(codes.Internal, "internal")

	tests := []struct {
		err            error
		name           string
		expectFailover bool
	}{
		// direct gRPC status (baseline)
		{name: "direct_unavailable", err: grpcUnavailable, expectFailover: true},
		{name: "direct_deadline_exceeded", err: grpcDeadline, expectFailover: true},
		{name: "direct_internal", err: grpcInternal, expectFailover: false},
		// fmt.Errorf %w
		{name: "fmt_errorf_unavailable", err: fmt.Errorf("wrap: %w", grpcUnavailable), expectFailover: true},
		{name: "fmt_errorf_deadline_exceeded", err: fmt.Errorf("wrap: %w", grpcDeadline), expectFailover: true},
		{name: "fmt_errorf_internal", err: fmt.Errorf("wrap: %w", grpcInternal), expectFailover: false},
		{
			name: "fmt_errorf_double_wrapped",
			err:  fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", grpcUnavailable)), expectFailover: true,
		},
		// errors.New with message only — no gRPC status in chain
		{name: "errors_new_with_grpc_message_text", err: errors.New(grpcUnavailable.Error()), expectFailover: false},
		// errors.Join
		{name: "errors_join_unavailable", err: errors.Join(errors.New("extra"), grpcUnavailable), expectFailover: true},
		{name: "errors_join_internal", err: errors.Join(errors.New("extra"), grpcInternal), expectFailover: false},
		// custom Unwrap() error
		{
			name: "custom_unwrap_deadline_exceeded",
			err:  &customUnwrapError{msg: "custom", cause: grpcDeadline}, expectFailover: true,
		},
		{
			name: "custom_unwrap_internal",
			err:  &customUnwrapError{msg: "custom", cause: grpcInternal}, expectFailover: false,
		},
		// custom Unwrap() []error
		{
			name: "multi_unwrap_unavailable",
			err:  &multiUnwrapError{msg: "multi", causes: []error{errors.New("other"), grpcUnavailable}}, expectFailover: true,
		},
		// pkg/errors Wrap and Wrapf
		{name: "pkgerrors_wrap_unavailable", err: pkgerrors.Wrap(grpcUnavailable, "wrap"), expectFailover: true},
		{name: "pkgerrors_wrapf_deadline_exceeded", err: pkgerrors.Wrapf(grpcDeadline, "wrap %s", "ctx"), expectFailover: true},
		{name: "pkgerrors_wrap_internal", err: pkgerrors.Wrap(grpcInternal, "wrap"), expectFailover: false},
		// plain errors — no gRPC status
		{name: "plain_errors_new", err: errors.New("plain error"), expectFailover: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsFailoverError(tt.err)
			assert.Equal(t, tt.expectFailover, result, "failover error classification mismatch")
		})
	}
}

func TestIsInternalErrorWrapped(t *testing.T) {
	grpcInternal := status.Error(codes.Internal, "internal")
	grpcUnavailable := status.Error(codes.Unavailable, "unavailable")

	tests := []struct {
		err            error
		name           string
		expectInternal bool
	}{
		// direct gRPC status (baseline)
		{name: "direct_internal", err: grpcInternal, expectInternal: true},
		{name: "direct_unavailable", err: grpcUnavailable, expectInternal: false},
		// fmt.Errorf %w
		{name: "fmt_errorf_internal", err: fmt.Errorf("wrap: %w", grpcInternal), expectInternal: true},
		{name: "fmt_errorf_unavailable", err: fmt.Errorf("wrap: %w", grpcUnavailable), expectInternal: false},
		{
			name: "fmt_errorf_double_wrapped",
			err:  fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", grpcInternal)), expectInternal: true,
		},
		// errors.New with message only — no gRPC status in chain
		{name: "errors_new_with_grpc_message_text", err: errors.New(grpcInternal.Error()), expectInternal: false},
		// errors.Join
		{name: "errors_join_internal", err: errors.Join(errors.New("extra"), grpcInternal), expectInternal: true},
		{name: "errors_join_unavailable", err: errors.Join(errors.New("extra"), grpcUnavailable), expectInternal: false},
		// custom Unwrap() error
		{
			name: "custom_unwrap_internal",
			err:  &customUnwrapError{msg: "custom", cause: grpcInternal}, expectInternal: true,
		},
		{
			name: "custom_unwrap_unavailable",
			err:  &customUnwrapError{msg: "custom", cause: grpcUnavailable}, expectInternal: false,
		},
		// custom Unwrap() []error
		{
			name: "multi_unwrap_internal",
			err:  &multiUnwrapError{msg: "multi", causes: []error{errors.New("other"), grpcInternal}}, expectInternal: true,
		},
		{
			name: "multi_unwrap_all_non_internal",
			err:  &multiUnwrapError{msg: "multi", causes: []error{errors.New("a"), grpcUnavailable}}, expectInternal: false,
		},
		// pkg/errors Wrap and Wrapf
		{name: "pkgerrors_wrap_internal", err: pkgerrors.Wrap(grpcInternal, "wrap"), expectInternal: true},
		{name: "pkgerrors_wrapf_internal", err: pkgerrors.Wrapf(grpcInternal, "wrap %s", "ctx"), expectInternal: true},
		{name: "pkgerrors_wrap_unavailable", err: pkgerrors.Wrap(grpcUnavailable, "wrap"), expectInternal: false},
		// plain errors — no gRPC status
		{name: "plain_errors_new", err: errors.New("plain error"), expectInternal: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsInternalError(tt.err)
			assert.Equal(t, tt.expectInternal, result, "internal error classification mismatch")
		})
	}
}

func TestBackoffDistribution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping distribution test in short mode")
	}

	const samples = 100
	baseBackoff := 100 * time.Millisecond
	maxBackoff := 10 * time.Second
	jitter := 0.2

	var totalBackoff time.Duration
	for i := 0; i < samples; i++ {
		b := JitteredBackoff(baseBackoff, maxBackoff, 0, jitter)
		totalBackoff += b
		assert.GreaterOrEqual(t, b, 80*time.Millisecond, "backoff should be >= 80ms with 0.2 jitter")
		assert.LessOrEqual(t, b, 120*time.Millisecond, "backoff should be <= 120ms with 0.2 jitter")
	}

	avgBackoff := totalBackoff / time.Duration(samples)
	assert.InDelta(t, float64(100*time.Millisecond), float64(avgBackoff), float64(20*time.Millisecond),
		"average backoff should be near base duration")
}
