// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for additional
// information regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package query

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestTransportScopeClosesRegisteredReleasesOnce(t *testing.T) {
	ctx, scope := NewTransportScope(context.Background())
	var released atomic.Int32
	release := RegisterTransportRelease(ctx, func() { released.Add(1) })
	release()
	scope.Close()
	scope.Close()
	if got := released.Load(); got != 1 {
		t.Fatalf("release count = %d, want 1", got)
	}
}

func TestRegisterTransportReleaseWithoutScopeReturnsRelease(t *testing.T) {
	var released atomic.Int32
	release := RegisterTransportRelease(context.Background(), func() { released.Add(1) })
	release()
	if got := released.Load(); got != 1 {
		t.Fatalf("release count = %d, want 1", got)
	}
}
