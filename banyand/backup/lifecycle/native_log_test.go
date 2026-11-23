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

package lifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// runUnit runs u through its run.Group lifecycle and fails if Serve's stop
// notice does not close once GracefulStop returns.
func runUnit(t *testing.T, u *nativeLogUnit) {
	t.Helper()
	if err := u.PreRun(context.Background()); err != nil {
		t.Fatalf("PreRun: %v", err)
	}
	stopped := u.Serve()
	u.GracefulStop()
	waitStopped(t, stopped)
}

func waitStopped(t *testing.T, stopped run.StopNotify) {
	t.Helper()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("the unit did not stop")
	}
}

func TestNativeLogUnitIsIdleWithNativeLogsOff(t *testing.T) {
	u := newNativeLogUnit(&logger.NativeLogging{}, &lifecycleService{gRPCAddr: "127.0.0.1:1"}, observability.BypassRegistry)
	runUnit(t, u)
	if u.producer != nil {
		t.Fatal("the unit built a producer with native logging off")
	}
}

// TestNativeLogUnitWritesToTheDataNodeItServes checks that the unit builds a
// producer from the lifecycle's own --grpc-addr once flags are parsed, and
// that it stops with the group. Nothing listens at the address; delivery is
// the producer's concern, tested in its own package.
func TestNativeLogUnitWritesToTheDataNodeItServes(t *testing.T) {
	t.Cleanup(func() { _ = logger.Init(logger.Logging{Env: "dev", Level: "info"}) })
	svc := &lifecycleService{}
	u := newNativeLogUnit(&logger.NativeLogging{Enabled: true, Level: "info"}, svc, observability.BypassRegistry)
	// The address is a flag, parsed after the unit is built.
	svc.gRPCAddr = "127.0.0.1:1"
	runUnit(t, u)
	if u.producer == nil {
		t.Fatal("the unit built no producer with native logging on")
	}
}

func TestNativeLogUnitRefusesNativeLogsWithNoAddress(t *testing.T) {
	u := newNativeLogUnit(&logger.NativeLogging{Enabled: true, Level: "info"}, &lifecycleService{}, observability.BypassRegistry)
	if err := u.PreRun(context.Background()); err == nil {
		t.Fatal("PreRun accepted native logging with no data node to write to")
	}
}
