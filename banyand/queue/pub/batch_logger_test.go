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
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/api/data"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// TestBatchLogsUnderTheCallersLogger covers the liaison side of the feedback
// protection: a batch logs its node errors under the logger the context of
// Publish carries. A native log write carries NATIVE-LOG, whose lines reach
// the console and never native storage, so a failing native log batch cannot
// feed one more stored line into each flush. Other callers keep the
// publisher's own logger.
func TestBatchLogsUnderTheCallersLogger(t *testing.T) {
	p := newPubWithConnMgrForMetrics(t, nil)
	message := bus.NewBatchMessageWithNode(bus.MessageID(1), "unknown-node", &streamv1.InternalWriteRequest{})
	for name, tc := range map[string]struct {
		ctx  context.Context
		want func(module string) bool
	}{
		"a native log write": {
			ctx: logger.WithNativeWrite(context.Background()),
			want: func(module string) bool {
				return strings.HasPrefix(module, strings.ToUpper(logger.NativeWriteModule)+".")
			},
		},
		"any other write": {
			ctx:  context.Background(),
			want: func(module string) bool { return module == p.log.Module() },
		},
	} {
		t.Run(name, func(t *testing.T) {
			bp := p.NewBatchPublisher(time.Second).(*batchPublisher)
			_, _ = bp.Publish(tc.ctx, data.TopicStreamWrite, message)
			_, _ = bp.Close()
			assert.True(t, tc.want(bp.f.l.Module()), "the batch logs under %q", bp.f.l.Module())
		})
	}
}
