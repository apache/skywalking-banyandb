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
	"crypto/rand"
	"errors"
	"math/big"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

const (
	defaultJitterFactor      = 0.2
	defaultMaxRetries        = 3
	defaultPerRequestTimeout = 2 * time.Second
	defaultBackoffBase       = 500 * time.Millisecond
	defaultBackoffMax        = 30 * time.Second
)

var (
	// Retry policy for health check.
	initBackoff = time.Second
	maxBackoff  = 20 * time.Second

	// Retryable gRPC status codes for streaming send retries.
	retryableCodes = map[codes.Code]bool{
		codes.OK:                 false,
		codes.Canceled:           false,
		codes.Unknown:            false,
		codes.InvalidArgument:    false,
		codes.DeadlineExceeded:   true, // Retryable - operation exceeded deadline
		codes.NotFound:           false,
		codes.AlreadyExists:      false,
		codes.PermissionDenied:   false,
		codes.ResourceExhausted:  true, // Retryable - server resource limits exceeded
		codes.FailedPrecondition: false,
		codes.Aborted:            false,
		codes.OutOfRange:         false,
		codes.Unimplemented:      false,
		codes.Internal:           true, // Retryable - internal server error should participate in circuit breaker
		codes.Unavailable:        true, // Retryable - service temporarily unavailable
		codes.DataLoss:           false,
		codes.Unauthenticated:    false,
	}
)

// secureRandFloat64 generates a cryptographically secure random float64 in [0, 1).
func secureRandFloat64() float64 {
	// Generate a random uint64
	maxVal := big.NewInt(1 << 53) // Use 53 bits for precision similar to math/rand
	n, err := rand.Int(rand.Reader, maxVal)
	if err != nil {
		// Fallback to a reasonable value if crypto/rand fails
		return 0.5
	}
	return float64(n.Uint64()) / float64(1<<53)
}

// jitteredBackoff calculates backoff duration with jitter to avoid thundering herds.
// Uses bounded symmetric jitter: backoff * (1 + jitter * (rand() - 0.5) * 2).
func jitteredBackoff(baseBackoff, maxBackoff time.Duration, attempt int, jitterFactor float64) time.Duration {
	if jitterFactor < 0 {
		jitterFactor = 0
	}
	if jitterFactor > 1 {
		jitterFactor = 1
	}

	// Exponential backoff: base * 2^attempt
	backoff := baseBackoff
	for i := 0; i < attempt; i++ {
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
			break
		}
	}

	// Apply jitter: backoff * (1 + jitter * (rand() - 0.5) * 2)
	// This gives us a range of [backoff * (1-jitter), backoff * (1+jitter)]
	jitterRange := float64(backoff) * jitterFactor
	randomFloat := secureRandFloat64()
	randomOffset := (randomFloat - 0.5) * 2 * jitterRange

	jitteredDuration := time.Duration(float64(backoff) + randomOffset)
	if jitteredDuration < 0 {
		jitteredDuration = baseBackoff / 10 // Minimum backoff
	}
	if jitteredDuration > maxBackoff {
		jitteredDuration = maxBackoff
	}

	return jitteredDuration
}

// isTransientError checks if the error is considered transient and retryable.
func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	// Handle gRPC status errors
	if s, ok := status.FromError(err); ok {
		return retryableCodes[s.Code()]
	}

	// Handle common.Error types
	var ce *common.Error
	if errors.As(err, &ce) {
		// Map common status to gRPC codes for consistency
		switch ce.Status() {
		case modelv1.Status_STATUS_INTERNAL_ERROR:
			return retryableCodes[codes.Internal]
		default:
			return false
		}
	}

	return false
}

// isInternalError checks if the error is an internal server error.
func isInternalError(err error) bool {
	if err == nil {
		return false
	}

	// Handle gRPC status errors
	if s, ok := status.FromError(err); ok {
		return s.Code() == codes.Internal
	}

	// Handle common.Error types
	var ce *common.Error
	if errors.As(err, &ce) {
		return ce.Status() == modelv1.Status_STATUS_INTERNAL_ERROR
	}

	return false
}
