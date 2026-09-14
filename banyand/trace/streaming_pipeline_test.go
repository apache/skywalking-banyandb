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

package trace

import (
	"context"

	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/queue"
)

type fakeSIDX struct {
	responses []*sidx.QueryResponse
}

func (f *fakeSIDX) StreamingQuery(ctx context.Context, _ sidx.QueryRequest) (<-chan *sidx.QueryResponse, <-chan error) {
	results := make(chan *sidx.QueryResponse, len(f.responses))
	errCh := make(chan error, 1)

	go func() {
		defer close(results)
		defer close(errCh)

		for _, resp := range f.responses {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case results <- resp:
			}
		}
	}()

	return results, errCh
}

func (f *fakeSIDX) IntroduceMemPart(uint64, *sidx.MemPart)          { panic("not implemented") }
func (f *fakeSIDX) IntroduceFlushed(*sidx.FlusherIntroduction)      {}
func (f *fakeSIDX) IntroduceMerged(*sidx.MergerIntroduction) func() { return func() {} }
func (f *fakeSIDX) ConvertToMemPart([]sidx.WriteRequest, int64, *int64, *int64) (*sidx.MemPart, error) {
	panic("not implemented")
}

func (f *fakeSIDX) Query(context.Context, sidx.QueryRequest) (*sidx.QueryResponse, error) {
	panic("not implemented")
}

func (f *fakeSIDX) QuerySync(_ context.Context, _ sidx.QueryRequest) ([]*sidx.QueryResponse, error) {
	return f.responses, nil
}
func (f *fakeSIDX) Stats(context.Context) (*sidx.Stats, error) { return &sidx.Stats{}, nil }
func (f *fakeSIDX) Close() error                               { return nil }
func (f *fakeSIDX) Flush(map[uint64]struct{}) (*sidx.FlusherIntroduction, error) {
	panic("not implemented")
}

func (f *fakeSIDX) Merge(<-chan struct{}, map[uint64]struct{}, uint64, func([]byte) bool) (*sidx.MergerIntroduction, error) {
	panic("not implemented")
}

func (f *fakeSIDX) StreamingParts(map[uint64]struct{}, string, uint32, string) ([]queue.StreamingPartData, []func()) {
	panic("not implemented")
}
func (f *fakeSIDX) PartPaths(map[uint64]struct{}) map[uint64]string { return map[uint64]string{} }
func (f *fakeSIDX) IntroduceSynced(map[uint64]struct{}) func()      { return func() {} }
func (f *fakeSIDX) TakeFileSnapshot(_ string) error                 { return nil }
func (f *fakeSIDX) ScanQuery(context.Context, sidx.ScanQueryRequest) ([]*sidx.QueryResponse, error) {
	return nil, nil
}

func (f *fakeSIDX) PrepareMemPart(uint64, *sidx.MemPart) func(cur *sidx.Snapshot) *sidx.Snapshot {
	return func(cur *sidx.Snapshot) *sidx.Snapshot { return cur }
}

func (f *fakeSIDX) PrepareFilePart(uint64, string) func(cur *sidx.Snapshot) *sidx.Snapshot {
	return func(cur *sidx.Snapshot) *sidx.Snapshot { return cur }
}

func (f *fakeSIDX) PrepareFlushed(*sidx.FlusherIntroduction) func(cur *sidx.Snapshot) *sidx.Snapshot {
	return func(cur *sidx.Snapshot) *sidx.Snapshot { return cur }
}

func (f *fakeSIDX) PrepareMerged(*sidx.MergerIntroduction) func(cur *sidx.Snapshot) *sidx.Snapshot {
	return func(cur *sidx.Snapshot) *sidx.Snapshot { return cur }
}

func (f *fakeSIDX) PrepareSynced(map[uint64]struct{}) func(cur *sidx.Snapshot) *sidx.Snapshot {
	return func(cur *sidx.Snapshot) *sidx.Snapshot { return cur }
}
func (f *fakeSIDX) CurrentSnapshot() *sidx.Snapshot { return nil }
func (f *fakeSIDX) ReplaceSnapshot(*sidx.Snapshot)  {}

func encodeTraceIDForTest(id string) []byte {
	buf := make([]byte, len(id)+1)
	buf[0] = byte(idFormatV1)
	copy(buf[1:], id)
	return buf
}
