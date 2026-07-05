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

package test_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

func TestEventuallyConsistentlyUsesQuickWindowOnFirstTryConvergence(t *testing.T) {
	gomega.RegisterTestingT(t)
	var calls int32
	start := time.Now()
	test.EventuallyConsistently(func() bool {
		atomic.AddInt32(&calls, 1)
		return true
	}).Should(gomega.BeTrue())
	elapsed := time.Since(start)
	if got := atomic.LoadInt32(&calls); got < 2 {
		t.Fatalf("the stability window must still re-sample after a first-try convergence, got %d evaluations", got)
	}
	if elapsed < flags.QuickStabilityTimeout {
		t.Fatalf("first-try convergence must hold the quick window %v, took %v", flags.QuickStabilityTimeout, elapsed)
	}
	if elapsed >= flags.StabilityTimeout {
		t.Fatalf("first-try convergence should pay the quick window, not the full one, took %v", elapsed)
	}
}

func TestEventuallyConsistentlyHoldsWindowAfterRetriedConvergence(t *testing.T) {
	gomega.RegisterTestingT(t)
	var calls int32
	start := time.Now()
	test.EventuallyConsistently(func() bool {
		return atomic.AddInt32(&calls, 1) >= 3
	}).Should(gomega.BeTrue())
	elapsed := time.Since(start)
	if elapsed < flags.StabilityTimeout {
		t.Fatalf("retried convergence must hold the full stability window %v, took %v", flags.StabilityTimeout, elapsed)
	}
}
