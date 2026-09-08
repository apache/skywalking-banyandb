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

package sidx

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query"
)

type tinySIDXLease struct{ remaining uint64 }

func (l *tinySIDXLease) Charge(bytes uint64) error {
	if bytes > l.remaining {
		return errors.New("sidx test budget exhausted")
	}
	l.remaining -= bytes
	return nil
}
func (*tinySIDXLease) Limit() uint64  { return 1024 }
func (l *tinySIDXLease) Used() uint64 { return 1024 - l.remaining }
func (*tinySIDXLease) Owner() any     { return "sidx-test" }

func TestBuildCursorsForBatchSyncChargesBeforeBlockDecode(t *testing.T) {
	s := &sidx{}
	ctx := query.WithBudgetLease(context.Background(), &tinySIDXLease{remaining: 1024})
	batch := &blockScanResultBatch{bss: []blockScanResult{{bm: blockMetadata{uncompressedSize: 1 << 20}}}}
	cursors, err := s.buildCursorsForBatchSync(ctx, batch, nil, QueryRequest{}, true, &batchMetrics{})
	if err == nil {
		t.Fatal("expected oversized block charge failure")
	}
	if cursors != nil {
		t.Fatalf("expected no cursors, got %d", len(cursors))
	}
}
