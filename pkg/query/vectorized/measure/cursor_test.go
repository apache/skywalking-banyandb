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

package measure

import (
	"errors"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

func TestSeriesCursor_Init_FirstSeriesActivated(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResult(1, 100, 200, 300)}}
	var c SeriesCursor
	c.Init(qr)
	if got := c.RemainingInSeries(); got != 3 {
		t.Fatalf("RemainingInSeries after Init: want 3, got %d", got)
	}
}

func TestSeriesCursor_NextRow_AdvancesWithinSeries(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResult(1, 10, 20, 30)}}
	var c SeriesCursor
	c.Init(qr)
	r1, err := c.NextRow()
	if err != nil {
		t.Fatal(err)
	}
	if r1.timestamp != 10 || r1.sid != 1 {
		t.Fatalf("row 1: ts=%d sid=%d", r1.timestamp, r1.sid)
	}
	r2, _ := c.NextRow()
	if r2.timestamp != 20 {
		t.Fatalf("row 2: ts=%d", r2.timestamp)
	}
	r3, _ := c.NextRow()
	if r3.timestamp != 30 {
		t.Fatalf("row 3: ts=%d", r3.timestamp)
	}
}

func TestSeriesCursor_NextRow_CrossesSeriesBoundary(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{
		mkResult(1, 100, 200),
		mkResult(2, 300),
	}}
	var c SeriesCursor
	c.Init(qr)
	_, _ = c.NextRow() // ts=100, sid=1
	_, _ = c.NextRow() // ts=200, sid=1
	r, err := c.NextRow()
	if err != nil {
		t.Fatal(err)
	}
	if r.sid != 2 || r.timestamp != 300 {
		t.Fatalf("expected (sid=2, ts=300), got (sid=%d, ts=%d)", r.sid, r.timestamp)
	}
}

func TestSeriesCursor_NextRow_SkipsEmptyMeasureResult(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{
		mkResult(1, 100),
		mkResult(2),       // empty — must be skipped
		mkResult(3, 300),
	}}
	var c SeriesCursor
	c.Init(qr)
	_, _ = c.NextRow() // sid=1
	r, _ := c.NextRow()
	if r.sid != 3 || r.timestamp != 300 {
		t.Fatalf("expected to skip empty series 2; got sid=%d ts=%d", r.sid, r.timestamp)
	}
}

func TestSeriesCursor_NextRow_EOF_ReturnsZeroValueAndNilErr(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResult(1, 100)}}
	var c SeriesCursor
	c.Init(qr)
	_, _ = c.NextRow()
	r, err := c.NextRow()
	if err != nil {
		t.Fatalf("EOF must return nil err, got %v", err)
	}
	if r.timestamp != 0 || r.sid != 0 {
		t.Fatalf("EOF must return zero seriesRow, got %+v", r)
	}
}

func TestSeriesCursor_StorageError_StickyOnFirstCall(t *testing.T) {
	boom := errors.New("storage boom")
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResultErr(boom)}}
	var c SeriesCursor
	c.Init(qr)
	_, err := c.NextRow()
	if !errors.Is(err, boom) {
		t.Fatalf("first NextRow must surface storage error, got %v", err)
	}
}

// R1-critical — sticky-error contract. The previous draft of this code lost
// the error and silently returned EOF. This test ensures the regression cannot recur.
func TestSeriesCursor_StorageError_StickyOnSubsequentCalls(t *testing.T) {
	boom := errors.New("storage boom")
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResultErr(boom)}}
	var c SeriesCursor
	c.Init(qr)
	for i := range 3 {
		_, err := c.NextRow()
		if !errors.Is(err, boom) {
			t.Fatalf("call %d: storage error must remain sticky, got %v", i, err)
		}
	}
}

func TestSeriesCursor_RemainingInSeries_DropsAsPosAdvances(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResult(1, 10, 20, 30)}}
	var c SeriesCursor
	c.Init(qr)
	if c.RemainingInSeries() != 3 {
		t.Fatalf("initial RemainingInSeries: want 3, got %d", c.RemainingInSeries())
	}
	_, _ = c.NextRow()
	if c.RemainingInSeries() != 2 {
		t.Fatalf("after 1 NextRow: want 2, got %d", c.RemainingInSeries())
	}
	_, _ = c.NextRow()
	if c.RemainingInSeries() != 1 {
		t.Fatalf("after 2 NextRows: want 1, got %d", c.RemainingInSeries())
	}
}

func TestSeriesCursor_Current_ReturnsCurrentResultAndPos(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResult(7, 11, 22, 33)}}
	var c SeriesCursor
	c.Init(qr)
	cur, pos := c.Current()
	if cur == nil || cur.SID != 7 || pos != 0 {
		t.Fatalf("Current after Init: got cur=%v pos=%d", cur, pos)
	}
	_, _ = c.NextRow()
	cur, pos = c.Current()
	if cur == nil || pos != 1 {
		t.Fatalf("Current after 1 NextRow: pos=%d", pos)
	}
}

func TestSeriesCursor_Advance_CrossesSeriesWhenPosReachesEnd(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{
		mkResult(1, 100, 200, 300),
		mkResult(2, 400),
	}}
	var c SeriesCursor
	c.Init(qr)
	c.Advance(3) // exhaust series 1
	r, err := c.NextRow()
	if err != nil {
		t.Fatal(err)
	}
	if r.sid != 2 || r.timestamp != 400 {
		t.Fatalf("after Advance(3) we should be at series 2: got sid=%d ts=%d", r.sid, r.timestamp)
	}
}

func TestSeriesCursor_Close_Idempotent_SafeAfterEOF(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResult(1, 100)}}
	var c SeriesCursor
	c.Init(qr)
	_, _ = c.NextRow()
	_, _ = c.NextRow() // EOF
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Close must not panic: %v", r)
		}
	}()
	c.Close()
	c.Close()
}

func TestSeriesCursor_Close_ReleasesUnderlyingMeasureQueryResult(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResult(1, 100)}}
	var c SeriesCursor
	c.Init(qr)
	c.Close()
	if qr.releaseCnt != 1 {
		t.Fatalf("Release must be called exactly once on first Close: got %d", qr.releaseCnt)
	}
	c.Close() // idempotent — must not double-release
	if qr.releaseCnt != 1 {
		t.Fatalf("second Close must not re-release: got %d", qr.releaseCnt)
	}
}
