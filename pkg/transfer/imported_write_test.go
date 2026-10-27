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

package transfer

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/transfer/ratelimit"
)

// recordingListener stands in for a catalog's write listener.
type recordingListener struct {
	// replies answers the messages in turn; a message beyond them gets the zero message.
	replies []bus.Message
	got     []bus.Message
}

func (r *recordingListener) Rev(_ context.Context, m bus.Message) bus.Message {
	r.got = append(r.got, m)
	if i := len(r.got) - 1; i < len(r.replies) {
		return r.replies[i]
	}
	return bus.Message{}
}

func (r *recordingListener) CheckHealth() *common.Error { return nil }

func newTestImportListener(write bus.MessageListener) *importListener {
	return &importListener{write: write, rate: ratelimit.NewNode()}
}

func initWrite(rowsPerSecond uint64) *transferv1.ImportWrite {
	return &transferv1.ImportWrite{Init: &transferv1.ImportWriteInit{RateLimit: &transferv1.RateLimit{RowsPerSecond: rowsPerSecond}}}
}

func initBytes(bytesPerSecond uint64) *transferv1.ImportWrite {
	return &transferv1.ImportWrite{Init: &transferv1.ImportWriteInit{RateLimit: &transferv1.RateLimit{BytesPerSecond: bytesPerSecond}}}
}

func propertyWrite(shard uint64) *transferv1.ImportWrite {
	return &transferv1.ImportWrite{Row: &transferv1.ImportWrite_Property{Property: &propertyv1.InternalRepairRequest{ShardId: shard}}}
}

func wantStatus(t *testing.T, reply bus.Message, want modelv1.Status) *common.Error {
	t.Helper()
	ce, ok := reply.Data().(*common.Error)
	if !ok {
		t.Fatalf("reply = %v, want an error reply", reply.Data())
	}
	if ce.Status() != want {
		t.Fatalf("reply status = %v (%s), want %v", ce.Status(), ce.Error(), want)
	}
	return ce
}

func streamWrite(id string) *transferv1.ImportWrite {
	return &transferv1.ImportWrite{Row: &transferv1.ImportWrite_Stream{Stream: &streamv1.InternalWriteRequest{
		Request: &streamv1.WriteRequest{Element: &streamv1.ElementValue{ElementId: id}},
	}}}
}

func TestImportListener_HandsTheRowsOfABatchToTheWriteListener(t *testing.T) {
	write := &recordingListener{}
	l := newTestImportListener(write)
	measure := &measurev1.InternalWriteRequest{ShardId: 3}
	encoded, err := proto.Marshal(streamWrite("b"))
	if err != nil {
		t.Fatal(err)
	}
	frames := []any{initWrite(0), &transferv1.ImportWrite{Row: &transferv1.ImportWrite_Measure{Measure: measure}}, encoded}
	if reply := l.Rev(context.Background(), bus.NewMessage(1, frames)); reply.Data() != nil {
		t.Fatalf("reply = %v, want none", reply.Data())
	}
	if len(write.got) != 1 {
		t.Fatalf("the write listener got %d message(s), want 1", len(write.got))
	}
	rows := write.got[0].Data().([]any)
	if len(rows) != 2 {
		t.Fatalf("the write listener got %d row(s), want 2 without the init frame", len(rows))
	}
	// A measure row is marked as imported, so that it is kept out of Top-N aggregation.
	if m, ok := rows[0].(*data.ImportedMeasureWrite); !ok || m.InternalWriteRequest != measure {
		t.Fatalf("row 0 = %T, want the measure write marked as imported", rows[0])
	}
	if s, ok := rows[1].(*streamv1.InternalWriteRequest); !ok || s.GetRequest().GetElement().GetElementId() != "b" {
		t.Fatalf("row 1 = %v, want the decoded stream write", rows[1])
	}
}

func TestImportListener_AdmitsRowsAtTheInitRate(t *testing.T) {
	l := newTestImportListener(&recordingListener{})
	frames := []any{initWrite(20)}
	for range 30 {
		frames = append(frames, streamWrite("x"))
	}
	start := time.Now()
	l.Rev(context.Background(), bus.NewMessage(1, frames))
	// 30 rows at 20 rows/s with a burst of 20: the last 10 wait for about 500ms.
	if elapsed := time.Since(start); elapsed < 400*time.Millisecond {
		t.Fatalf("30 rows at 20 rows/s took %v, want the rows beyond the burst paced", elapsed)
	}
}

func TestImportListener_AnswersWithTheWriteReply(t *testing.T) {
	failure := bus.NewMessage(1, common.NewError("2 of 2 rows were not written"))
	write := &recordingListener{replies: []bus.Message{failure}}
	reply := newTestImportListener(write).Rev(context.Background(), bus.NewMessage(1, []any{initWrite(0), streamWrite("a"), streamWrite("b")}))
	if reply.Data() != failure.Data() {
		t.Fatalf("reply = %v, want the write listener's failure", reply.Data())
	}
}

func TestImportListener_InitOnlyBatchWritesNothing(t *testing.T) {
	write := &recordingListener{}
	if reply := newTestImportListener(write).Rev(context.Background(), bus.NewMessage(1, []any{initWrite(1)})); reply.Data() != nil {
		t.Fatalf("reply = %v, want none", reply.Data())
	}
	if len(write.got) != 0 {
		t.Fatal("a batch without rows reached the write listener")
	}
}

func TestImportRowListener_HandsTheRowsOverOneAtATime(t *testing.T) {
	write := &recordingListener{replies: []bus.Message{bus.NewMessage(1, &propertyv1.InternalRepairResponse{})}}
	l := &importListener{write: write, rate: ratelimit.NewNode(), perRow: true}
	reply := l.Rev(context.Background(), bus.NewMessage(1, []any{initWrite(0), propertyWrite(1), propertyWrite(2)}))
	if reply.Data() != nil {
		t.Fatalf("reply = %v, want none once every row was written", reply.Data())
	}
	if len(write.got) != 2 {
		t.Fatalf("the write listener got %d message(s), want one per row", len(write.got))
	}
	for i, m := range write.got {
		r, ok := m.Data().(*propertyv1.InternalRepairRequest)
		if !ok || r.GetShardId() != uint64(i+1) {
			t.Fatalf("message %d = %v, want repair request %d alone", i, m.Data(), i+1)
		}
	}
}

func TestImportListener_BusyNodeWritesNothing(t *testing.T) {
	write := &recordingListener{}
	l := newTestImportListener(write)
	// 40 rows at 2 rows/s would wait 19s, past the 1s the request has left.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	frames := []any{initWrite(2)}
	for range 40 {
		frames = append(frames, streamWrite("x"))
	}
	ce := wantStatus(t, l.Rev(ctx, bus.NewMessage(1, frames)), modelv1.Status_STATUS_SERVER_BUSY)
	if !strings.Contains(ce.Error(), "import rate limit") {
		t.Fatalf("busy reply %q does not name the import rate limit", ce.Error())
	}
	if len(write.got) != 0 {
		t.Fatal("rows of a busy batch were written")
	}
}

func TestImportListener_ChargesTheEncodedBytes(t *testing.T) {
	encoded, err := proto.Marshal(streamWrite("0123456789"))
	if err != nil {
		t.Fatal(err)
	}
	inProcess := streamWrite("0123456789")
	for name, row := range map[string]any{"encoded frame": encoded, "in-process frame": inProcess} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			frames := []any{initBytes(1)}
			for range 10 {
				frames = append(frames, row)
			}
			// Ten rows of more than ten bytes at 1 B/s wait longer than the request has left.
			write := &recordingListener{}
			wantStatus(t, newTestImportListener(write).Rev(ctx, bus.NewMessage(1, frames)), modelv1.Status_STATUS_SERVER_BUSY)
			// The same rows at a byte rate that admits them in time are written.
			frames[0] = initBytes(1 << 20)
			if reply := newTestImportListener(write).Rev(ctx, bus.NewMessage(1, frames)); reply.Data() != nil {
				t.Fatalf("reply = %v, want the rows written", reply.Data())
			}
			if len(write.got) != 1 {
				t.Fatalf("the write listener got %d message(s), want 1", len(write.got))
			}
		})
	}
}

func TestImportListener_RefusesABadFrame(t *testing.T) {
	write := &recordingListener{}
	l := newTestImportListener(write)
	for name, data := range map[string]any{
		"undecodable":     []any{[]byte{0xff}},
		"wrong type":      []any{"row"},
		"not a batch":     initWrite(0),
		"a row, no batch": streamWrite("a"),
	} {
		if _, ok := l.Rev(context.Background(), bus.NewMessage(1, data)).Data().(*common.Error); !ok {
			t.Fatalf("%s: want an error reply", name)
		}
	}
	if len(write.got) != 0 {
		t.Fatalf("the write listener got %d message(s) of bad frames, want none", len(write.got))
	}
}

func TestImportListener_CanceledWaitIsAnErrorReply(t *testing.T) {
	write := &recordingListener{}
	l := newTestImportListener(write)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := l.Rev(ctx, bus.NewMessage(1, []any{initWrite(1), streamWrite("a"), streamWrite("b")})).Data().(*common.Error); !ok {
		t.Fatal("a wait canceled by the stream must answer with an error")
	}
	if len(write.got) != 0 {
		t.Fatal("rows of a canceled wait were written")
	}
}

// fastImportRetry shortens the retry bounds of the import listener until the test ends.
func fastImportRetry(t *testing.T, budget time.Duration) {
	b, base, maxBackoff := importRetryBudget, importRetryBackoff, importRetryBackoffMax
	importRetryBudget, importRetryBackoff, importRetryBackoffMax = budget, time.Millisecond, 4*time.Millisecond
	t.Cleanup(func() { importRetryBudget, importRetryBackoff, importRetryBackoffMax = b, base, maxBackoff })
}

// scriptedWrite stands in for a write listener whose every call is scripted: it gets the rows
// handed over, as a batch or one at a time, and the context to record failures in.
type scriptedWrite struct {
	onCall func(ctx context.Context, call int, rows []any) bus.Message
	calls  [][]any
}

func (w *scriptedWrite) Rev(ctx context.Context, m bus.Message) bus.Message {
	rows, ok := m.Data().([]any)
	if !ok {
		rows = []any{m.Data()}
	}
	w.calls = append(w.calls, rows)
	if w.onCall == nil {
		return bus.Message{}
	}
	return w.onCall(ctx, len(w.calls)-1, rows)
}

func (w *scriptedWrite) CheckHealth() *common.Error { return nil }

func streamBatch(ids ...string) []any {
	frames := []any{initWrite(0)}
	for _, id := range ids {
		frames = append(frames, streamWrite(id))
	}
	return frames
}

// idsOf returns the element ids of the stream rows of one call.
func idsOf(rows []any) []string {
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.(*streamv1.InternalWriteRequest).GetRequest().GetElement().GetElementId())
	}
	return ids
}

func wantCalls(t *testing.T, w *scriptedWrite, want ...[]string) {
	t.Helper()
	got := make([][]string, 0, len(w.calls))
	for _, c := range w.calls {
		got = append(got, idsOf(c))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
}

func TestImportListener_WritesAgainOnlyTheRowsNotStored(t *testing.T) {
	fastImportRetry(t, time.Minute)
	write := &scriptedWrite{onCall: func(ctx context.Context, call int, _ []any) bus.Message {
		if call == 0 {
			common.RecordRowFailure(ctx, 1, errors.New("stream sw/log: cannot load tsdb for group sw"))
			common.RecordRowFailure(ctx, 3, errors.New("stream sw/log: cannot load tsdb for group sw"))
		}
		return bus.Message{}
	}}
	if reply := newTestImportListener(write).Rev(context.Background(), bus.NewMessage(1, streamBatch("a", "b", "c", "d"))); reply.Data() != nil {
		t.Fatalf("reply = %v, want none once the failed rows were written", reply.Data())
	}
	wantCalls(t, write, []string{"a", "b", "c", "d"}, []string{"b", "d"})
}

func TestImportListener_RowIndexesOfARetryMapBackToTheBatch(t *testing.T) {
	fastImportRetry(t, time.Minute)
	write := &scriptedWrite{onCall: func(ctx context.Context, call int, _ []any) bus.Message {
		switch call {
		case 0:
			common.RecordRowFailure(ctx, 1, errors.New("not yet"))
			common.RecordRowFailure(ctx, 3, errors.New("not yet"))
			common.RecordRowFailure(ctx, 4, errors.New("not yet"))
		case 1:
			// Index 1 of the retry is row 3 of the batch.
			common.RecordRowFailure(ctx, 1, errors.New("still not"))
		}
		return bus.Message{}
	}}
	if reply := newTestImportListener(write).Rev(context.Background(), bus.NewMessage(1, streamBatch("a", "b", "c", "d", "e"))); reply.Data() != nil {
		t.Fatalf("reply = %v, want none", reply.Data())
	}
	wantCalls(t, write, []string{"a", "b", "c", "d", "e"}, []string{"b", "d", "e"}, []string{"d"})
}

func TestImportListener_RowsStillFailingAtTheBudgetAreAnswered(t *testing.T) {
	fastImportRetry(t, 50*time.Millisecond)
	write := &scriptedWrite{onCall: func(ctx context.Context, _ int, rows []any) bus.Message {
		for i, r := range rows {
			if idsOf([]any{r})[0] == "c" {
				common.RecordRowFailure(ctx, i, errors.New("stream sw/log: cannot create segment: disk read-only"))
			}
		}
		return bus.Message{}
	}}
	start := time.Now()
	ce := wantStatus(t, newTestImportListener(write).Rev(context.Background(), bus.NewMessage(1, streamBatch("a", "b", "c"))),
		modelv1.Status_STATUS_INTERNAL_ERROR)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("the retries took %v, want them bounded by the 50ms budget", elapsed)
	}
	want := "1 of 3 imported row(s) were not written after retries (rows 2), the first: stream sw/log: cannot create segment: disk read-only"
	if !strings.Contains(ce.Error(), want) {
		t.Fatalf("reply %q, want it to contain %q", ce.Error(), want)
	}
	if len(write.calls) < 2 {
		t.Fatalf("the write listener was called %d time(s), want the failed row written again", len(write.calls))
	}
	for _, c := range write.calls[1:] {
		if !reflect.DeepEqual(idsOf(c), []string{"c"}) {
			t.Fatalf("a retry wrote %v, want only the failed row", idsOf(c))
		}
	}
}

func TestImportListener_FailureAfterStoringIsNotWrittenAgain(t *testing.T) {
	fastImportRetry(t, time.Minute)
	write := &scriptedWrite{onCall: func(ctx context.Context, _ int, _ []any) bus.Message {
		common.RecordWriteFailure(ctx, errors.New("stream group sw: cannot write element index: owner closed"))
		common.RecordRowFailure(ctx, 0, errors.New("not yet"))
		return bus.Message{}
	}}
	ce := wantStatus(t, newTestImportListener(write).Rev(context.Background(), bus.NewMessage(1, streamBatch("a", "b"))), modelv1.Status_STATUS_INTERNAL_ERROR)
	for _, want := range []string{"1 write(s) after storing imported rows failed", "cannot write element index", "store stream and trace rows twice"} {
		if !strings.Contains(ce.Error(), want) {
			t.Fatalf("reply %q, want it to contain %q", ce.Error(), want)
		}
	}
	if len(write.calls) != 1 {
		t.Fatalf("the write listener was called %d time(s), want no retry once rows are stored", len(write.calls))
	}
}

func TestImportListener_TheWritesOwnErrorEndsTheBatch(t *testing.T) {
	fastImportRetry(t, time.Minute)
	own := bus.NewMessage(1, common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "disk full"))
	write := &scriptedWrite{onCall: func(ctx context.Context, _ int, _ []any) bus.Message {
		common.RecordRowFailure(ctx, 0, errors.New("not yet"))
		return own
	}}
	if reply := newTestImportListener(write).Rev(context.Background(), bus.NewMessage(1, streamBatch("a"))); reply.Data() != own.Data() {
		t.Fatalf("reply = %v, want the write listener's own error", reply.Data())
	}
	if len(write.calls) != 1 {
		t.Fatalf("the write listener was called %d time(s), want no retry", len(write.calls))
	}
}

func TestImportListener_NoRecordedFailureKeepsTheWriteReply(t *testing.T) {
	write := &scriptedWrite{}
	if reply := newTestImportListener(write).Rev(context.Background(), bus.NewMessage(1, streamBatch("a", "b"))); reply.Data() != nil {
		t.Fatalf("reply = %v, want none when the write recorded nothing", reply.Data())
	}
	wantCalls(t, write, []string{"a", "b"})
}

func TestImportListener_RetryWriteGetsTheBatchBudget(t *testing.T) {
	fastImportRetry(t, time.Minute)
	writes := 0
	write := &scriptedWrite{onCall: func(ctx context.Context, _ int, _ []any) bus.Message {
		err := common.RetryWrite(ctx, func() error {
			writes++
			if writes < 3 {
				return errors.New("owner closing")
			}
			return nil
		})
		common.RecordWriteFailure(ctx, err)
		return bus.Message{}
	}}
	if reply := newTestImportListener(write).Rev(context.Background(), bus.NewMessage(1, streamBatch("a"))); reply.Data() != nil {
		t.Fatalf("reply = %v, want none once the retried write succeeded", reply.Data())
	}
	if writes != 3 {
		t.Fatalf("the index write ran %d time(s), want it retried until it succeeded on the third", writes)
	}
}

func TestImportListener_CanceledStreamStopsTheRetries(t *testing.T) {
	fastImportRetry(t, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	write := &scriptedWrite{onCall: func(rctx context.Context, call int, _ []any) bus.Message {
		common.RecordRowFailure(rctx, 0, errors.New("not yet"))
		if call == 1 {
			cancel()
		}
		return bus.Message{}
	}}
	start := time.Now()
	ce := wantStatus(t, newTestImportListener(write).Rev(ctx, bus.NewMessage(1, streamBatch("a"))), modelv1.Status_STATUS_INTERNAL_ERROR)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("the retries took %v after the stream was canceled", elapsed)
	}
	if !strings.Contains(ce.Error(), "1 of 1 imported row(s) were not written") {
		t.Fatalf("reply %q, want the row named as not written", ce.Error())
	}
}

func TestImportListener_EveryAttemptHasItsOwnRecorder(t *testing.T) {
	fastImportRetry(t, time.Minute)
	write := &scriptedWrite{onCall: func(ctx context.Context, call int, _ []any) bus.Message {
		if call == 0 {
			common.RecordRowFailure(ctx, 0, errors.New("not yet"))
		}
		return bus.Message{}
	}}
	l := newTestImportListener(write)
	if reply := l.Rev(context.Background(), bus.NewMessage(1, streamBatch("a"))); reply.Data() != nil {
		t.Fatalf("reply = %v, want none: the retry's recorder starts empty", reply.Data())
	}
	if reply := l.Rev(context.Background(), bus.NewMessage(2, streamBatch("b"))); reply.Data() != nil {
		t.Fatalf("reply = %v, want none: an earlier batch's failure is not carried over", reply.Data())
	}
	if len(write.calls) != 3 {
		t.Fatalf("calls = %d, want the first batch written twice and the second once", len(write.calls))
	}
}

func TestImportListener_RecordsNothingIntoTheCallersContext(t *testing.T) {
	fastImportRetry(t, 10*time.Millisecond)
	outer := &common.WriteRecorder{}
	ctx := common.WithWriteRecorder(context.Background(), outer)
	write := &scriptedWrite{onCall: func(rctx context.Context, _ int, _ []any) bus.Message {
		common.RecordRowFailure(rctx, 0, errors.New("invalid timestamp"))
		common.RecordWriteFailure(rctx, errors.New("cannot write index"))
		return bus.Message{}
	}}
	wantStatus(t, newTestImportListener(write).Rev(ctx, bus.NewMessage(1, streamBatch("a"))), modelv1.Status_STATUS_INTERNAL_ERROR)
	if len(outer.Failures()) != 0 || len(outer.RowFailures()) != 0 {
		t.Fatal("the batch recorded its failures into the caller's recorder")
	}
}

func TestImportListener_IgnoresARowIndexOutsideTheBatch(t *testing.T) {
	fastImportRetry(t, time.Minute)
	write := &scriptedWrite{onCall: func(ctx context.Context, _ int, _ []any) bus.Message {
		common.RecordRowFailure(ctx, 9, errors.New("bogus"))
		common.RecordRowFailure(ctx, -1, errors.New("bogus"))
		return bus.Message{}
	}}
	if reply := newTestImportListener(write).Rev(context.Background(), bus.NewMessage(1, streamBatch("a"))); reply.Data() != nil {
		t.Fatalf("reply = %v, want none", reply.Data())
	}
	if len(write.calls) != 1 {
		t.Fatalf("calls = %d, want no retry of rows the batch does not hold", len(write.calls))
	}
}

func TestNotWrittenNamesAtMostTenRows(t *testing.T) {
	failed := make([]rowFailure, 12)
	for i := range failed {
		failed[i] = rowFailure{row: i * 2, err: errors.New("cannot load tsdb")}
	}
	ce := wantStatus(t, notWritten(1, 30, failed), modelv1.Status_STATUS_INTERNAL_ERROR)
	want := "12 of 30 imported row(s) were not written after retries (rows 0,2,4,6,8,10,12,14,16,18,...), the first: cannot load tsdb"
	if !strings.Contains(ce.Error(), want) {
		t.Fatalf("reply %q, want it to contain %q", ce.Error(), want)
	}
}

func TestImportRowListener_WritesAFailedRowAgainWithTheRowsAfterIt(t *testing.T) {
	fastImportRetry(t, time.Minute)
	var order []uint64
	failedOnce := false
	write := &scriptedWrite{onCall: func(_ context.Context, _ int, rows []any) bus.Message {
		shard := rows[0].(*propertyv1.InternalRepairRequest).GetShardId()
		order = append(order, shard)
		if shard == 2 && !failedOnce {
			failedOnce = true
			return bus.NewMessage(1, common.NewError("fail to repair"))
		}
		return bus.Message{}
	}}
	l := &importListener{write: write, rate: ratelimit.NewNode(), perRow: true}
	frames := []any{initWrite(0), propertyWrite(1), propertyWrite(2), propertyWrite(3)}
	if reply := l.Rev(context.Background(), bus.NewMessage(1, frames)); reply.Data() != nil {
		t.Fatalf("reply = %v, want none once the failed row was repaired", reply.Data())
	}
	if !reflect.DeepEqual(order, []uint64{1, 2, 2, 3}) {
		t.Fatalf("repair order = %v, want row 2 repaired again before row 3", order)
	}
}

func TestImportRowListener_RecordedFailureIsAFailedRow(t *testing.T) {
	fastImportRetry(t, time.Minute)
	write := &scriptedWrite{onCall: func(ctx context.Context, call int, _ []any) bus.Message {
		if call == 0 {
			common.RecordWriteFailure(ctx, errors.New("cannot write"))
		}
		return bus.Message{}
	}}
	l := &importListener{write: write, rate: ratelimit.NewNode(), perRow: true}
	if reply := l.Rev(context.Background(), bus.NewMessage(1, []any{initWrite(0), propertyWrite(1)})); reply.Data() != nil {
		t.Fatalf("reply = %v, want none", reply.Data())
	}
	if len(write.calls) != 2 {
		t.Fatalf("calls = %d, want the row repaired again", len(write.calls))
	}
}

func TestImportRowListener_RowsStillFailingAtTheBudgetAreAnswered(t *testing.T) {
	fastImportRetry(t, 30*time.Millisecond)
	write := &scriptedWrite{onCall: func(_ context.Context, _ int, rows []any) bus.Message {
		if rows[0].(*propertyv1.InternalRepairRequest).GetShardId() == 2 {
			return bus.NewMessage(1, common.NewError("fail to repair: boom"))
		}
		return bus.Message{}
	}}
	l := &importListener{write: write, rate: ratelimit.NewNode(), perRow: true}
	frames := []any{initWrite(0), propertyWrite(1), propertyWrite(2), propertyWrite(3)}
	ce := wantStatus(t, l.Rev(context.Background(), bus.NewMessage(1, frames)), modelv1.Status_STATUS_INTERNAL_ERROR)
	want := "2 of 3 imported row(s) were not written after retries (rows 1,2), the first: code: STATUS_INTERNAL_ERROR, msg: fail to repair: boom"
	if !strings.Contains(ce.Error(), want) {
		t.Fatalf("reply %q, want it to contain %q", ce.Error(), want)
	}
}

func TestNodeRate_OnePerDataNode(t *testing.T) {
	a, b := t.Name()+"-a", t.Name()+"-b"
	first := nodeRate(a)
	if again := nodeRate(a); again != first {
		t.Fatal("the import topics of one data node do not share its rate")
	}
	if nodeRate(a) == nodeRate(b) {
		t.Fatal("two data nodes of one process share a rate")
	}
	ma := ImportListener(a, &recordingListener{}).(*importListener)
	pa := ImportRowListener(a, &recordingListener{}).(*importListener)
	if ma.rate != pa.rate || ma.rate != nodeRate(a) {
		t.Fatal("the batch and row import listeners of one data node do not share its rate")
	}
}

func TestImportListener_DataNodesAdmitAtTheirOwnRate(t *testing.T) {
	a, b := ImportListener(t.Name()+"-a", &recordingListener{}), ImportListener(t.Name()+"-b", &recordingListener{})
	frames := []any{initWrite(2)}
	for range 22 {
		frames = append(frames, streamWrite("x"))
	}
	// 22 rows at 2 rows/s keep node a busy for 10s.
	go a.Rev(context.Background(), bus.NewMessage(1, frames))
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// Node b admits 2 rows at once; on node a they would queue past the slack and be refused busy.
	if reply := b.Rev(ctx, bus.NewMessage(2, []any{initWrite(2), streamWrite("y"), streamWrite("z")})); reply.Data() != nil {
		t.Fatalf("reply of node b = %v, want its rows admitted at its own rate", reply.Data())
	}
	wantStatus(t, a.Rev(ctx, bus.NewMessage(3, []any{initWrite(2), streamWrite("y"), streamWrite("z")})), modelv1.Status_STATUS_SERVER_BUSY)
}
