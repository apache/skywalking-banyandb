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

	"github.com/apache/skywalking-banyandb/banyand/observability"
	nativelog "github.com/apache/skywalking-banyandb/banyand/observability/logging"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// nativeLogUnit stores the lifecycle's own logs on the data node it serves,
// through that node's cluster server, as its migration writes do. The
// producer is built in PreRun: the address and TLS settings are flags of the
// lifecycle service, known only once flags are parsed and logging initialized.
type nativeLogUnit struct {
	cfg      *logger.NativeLogging
	svc      *lifecycleService
	omr      observability.MetricsRegistry
	producer *nativelog.Service
	closer   *run.Closer
}

var (
	_ run.PreRunner = (*nativeLogUnit)(nil)
	_ run.Service   = (*nativeLogUnit)(nil)
)

func newNativeLogUnit(cfg *logger.NativeLogging, svc *lifecycleService, omr observability.MetricsRegistry) *nativeLogUnit {
	return &nativeLogUnit{cfg: cfg, svc: svc, omr: omr, closer: run.NewCloser(1)}
}

func (u *nativeLogUnit) Name() string { return "lifecycle-native-log" }

func (u *nativeLogUnit) PreRun(ctx context.Context) error {
	// The producer outlives PreRun's context: its buffer and delivery run
	// until GracefulStop.
	producer, err := nativelog.NewProducer(nativelog.ProducerConfig{ //nolint:contextcheck // see above
		Native: u.cfg,
		Node:   nativelog.NodeInfo{NodeID: nativelog.ProducerNodeID("", lifecycleRoleName), NodeType: lifecycleRoleName},
		Destination: nativelog.Destination{
			Addr:               u.svc.gRPCAddr,
			TLS:                u.svc.enableTLS,
			CACert:             u.svc.cert,
			InsecureSkipVerify: u.svc.insecure,
		},
		// Resolved when the producer starts, after the registry's own PreRun.
		Gauges: func(name string, labelNames ...string) meter.Gauge {
			return u.omr.With(nativelog.Scope).NewGauge(name, labelNames...)
		},
	})
	if err != nil || producer == nil {
		return err
	}
	u.producer = producer
	return producer.PreRun(ctx)
}

func (u *nativeLogUnit) Serve() run.StopNotify {
	if u.producer == nil {
		u.closer.Done()
		return u.closer.CloseNotify()
	}
	return u.producer.Serve()
}

func (u *nativeLogUnit) GracefulStop() {
	if u.producer != nil {
		u.producer.GracefulStop()
		return
	}
	u.closer.CloseThenWait()
}
