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

package initerror_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/initerror"
)

var errSentinel = errors.New("sentinel")

func TestAsPermanent_NilReturnsNil(t *testing.T) {
	if got := initerror.AsPermanent(nil); got != nil {
		t.Fatalf("AsPermanent(nil) = %v, want nil", got)
	}
}

func TestIsPermanent_Nil(t *testing.T) {
	if initerror.IsPermanent(nil) {
		t.Fatalf("IsPermanent(nil) = true, want false")
	}
}

func TestIsPermanent_PlainError(t *testing.T) {
	if initerror.IsPermanent(errSentinel) {
		t.Fatalf("IsPermanent(plain error) = true, want false")
	}
}

func TestIsPermanent_Direct(t *testing.T) {
	wrapped := initerror.AsPermanent(errSentinel)
	if !initerror.IsPermanent(wrapped) {
		t.Fatalf("IsPermanent(AsPermanent(...)) = false, want true")
	}
}

func TestIsPermanent_FmtErrorfChain(t *testing.T) {
	wrapped := fmt.Errorf("init failed: %w", initerror.AsPermanent(errSentinel))
	if !initerror.IsPermanent(wrapped) {
		t.Fatalf("IsPermanent through fmt.Errorf chain = false, want true")
	}
	if !errors.Is(wrapped, errSentinel) {
		t.Fatalf("errors.Is should still see the inner sentinel")
	}
}

func TestIsPermanent_DoubleWrap(t *testing.T) {
	inner := initerror.AsPermanent(errSentinel)
	outer := fmt.Errorf("outer: %w", fmt.Errorf("middle: %w", inner))
	if !initerror.IsPermanent(outer) {
		t.Fatalf("IsPermanent through double wrap = false, want true")
	}
}
