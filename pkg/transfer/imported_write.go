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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/transfer/ratelimit"
)

// nodeRates holds, per data node id, the import rate every import topic of that node admits its
// rows through. A process runs one data node, except a test harness that runs several.
var nodeRates sync.Map

// nodeRate returns the import rate of the data node nodeID.
func nodeRate(nodeID string) *ratelimit.Node {
	if r, ok := nodeRates.Load(nodeID); ok {
		return r.(*ratelimit.Node)
	}
	r, _ := nodeRates.LoadOrStore(nodeID, ratelimit.NewNode())
	return r.(*ratelimit.Node)
}

// ImportListener returns the listener of an import topic. It reads the frames of an
// ImportWrite batch, admits their rows at the rate its init frame asks for, and hands the rows
// to write, the listener of the catalog's write topic, in one batch message. Every import topic of
// the data node nodeID shares one rate. A batch the node
// cannot admit in time is answered STATUS_SERVER_BUSY before any row is written. What write fails
// to store, which it only logs for a regular write, it records in the context it is given: the
// rows not stored are written again (see writeRows), and what still fails is answered as an error.
func ImportListener(nodeID string, write bus.MessageListener) bus.MessageListener {
	return &importListener{write: write, rate: nodeRate(nodeID)}
}

// ImportRowListener is ImportListener for a write listener that takes one request per message:
// it hands the rows over one at a time, in order; a row that fails is written again, with the
// rows after it.
func ImportRowListener(nodeID string, write bus.MessageListener) bus.MessageListener {
	return &importListener{write: write, rate: nodeRate(nodeID), perRow: true}
}

type importListener struct {
	write  bus.MessageListener
	rate   *ratelimit.Node
	perRow bool
}

func (l *importListener) CheckHealth() *common.Error {
	return l.write.CheckHealth()
}

func (l *importListener) Rev(ctx context.Context, message bus.Message) bus.Message {
	frames, ok := message.Data().([]any)
	if !ok {
		return bus.NewMessage(message.ID(), common.NewError("invalid import message type %T, want a batch", message.Data()))
	}
	var (
		limit *transferv1.RateLimit
		rows  = make([]any, 0, len(frames))
		bytes int
	)
	for _, f := range frames {
		w, encoded, err := decodeImportWrite(f)
		if err != nil {
			return bus.NewMessage(message.ID(), common.NewError("%v", err))
		}
		if limit == nil {
			// The init frame comes first, so the byte rate is known before any row is sized.
			limit = w.GetInit().GetRateLimit()
		}
		row := importedRow(w)
		if row == nil {
			continue
		}
		rows = append(rows, row)
		if limit.GetBytesPerSecond() > 0 {
			if encoded == 0 {
				encoded = proto.Size(w)
			}
			bytes += encoded
		}
	}
	if len(rows) == 0 {
		return bus.Message{}
	}
	if err := l.rate.Pay(ctx, limit.GetBytesPerSecond(), limit.GetRowsPerSecond(), bytes, len(rows)); err != nil {
		if errors.Is(err, ratelimit.ErrBusy) {
			return bus.NewMessage(message.ID(), common.NewErrorWithStatus(modelv1.Status_STATUS_SERVER_BUSY, ImportRateBusy+err.Error()))
		}
		return bus.NewMessage(message.ID(), common.NewError("wait for the import rate: %v", err))
	}
	return l.writeRows(ctx, message.ID(), rows)
}

// The bounds of writing a batch again: a node keeps writing the rows that failed before they
// were stored, and retrying the writes that may be repeated, for at most importRetryBudget,
// well within the liaison's batch timeout.
var (
	importRetryBudget     = 30 * time.Second
	importRetryBackoff    = 100 * time.Millisecond
	importRetryBackoffMax = 2 * time.Second
)

// rowFailure is a row of the batch, by its index, that failed before it was stored.
type rowFailure struct {
	err error
	row int
}

// writeRows hands rows to the write listener and writes again, with back-off, only the rows that
// failed before they were stored, until importRetryBudget is spent. A write listener only logs a
// row it cannot write; the recorder in the context makes it tell the import, and lets it retry
// the writes that may be repeated. The rows the batch could not write are answered as an error.
func (l *importListener) writeRows(ctx context.Context, id bus.MessageID, rows []any) bus.Message {
	deadline := time.Now().Add(importRetryBudget)
	pending := make([]int, len(rows))
	for i := range pending {
		pending[i] = i
	}
	backoff := importRetryBackoff
	for {
		failed, reply := l.attempt(ctx, id, rows, pending, deadline)
		if _, isErr := reply.Data().(*common.Error); isErr || len(failed) == 0 {
			return reply
		}
		wait := min(backoff, time.Until(deadline))
		if wait <= 0 {
			return notWritten(id, len(rows), failed)
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return notWritten(id, len(rows), failed)
		case <-timer.C:
		}
		pending = pending[:0]
		for _, f := range failed {
			pending = append(pending, f.row)
		}
		backoff = min(backoff*2, importRetryBackoffMax)
	}
}

// attempt writes the pending rows once and returns those that failed before they were stored,
// or a reply that ends the batch: the write listener's own error, or the failures that came
// after the rows were stored, which writing the rows again would duplicate.
func (l *importListener) attempt(ctx context.Context, id bus.MessageID, rows []any, pending []int, deadline time.Time) ([]rowFailure, bus.Message) {
	if l.perRow {
		for k, i := range pending {
			recorder := common.NewWriteRecorder(deadline)
			reply := l.write.Rev(common.WithWriteRecorder(ctx, recorder), bus.NewMessage(id, rows[i]))
			if err := firstFailure(reply, recorder); err != nil {
				// Rows are written in order: the failed row and every later one are written again.
				failed := make([]rowFailure, 0, len(pending)-k)
				for _, j := range pending[k:] {
					failed = append(failed, rowFailure{row: j, err: err})
				}
				return failed, bus.Message{}
			}
		}
		return nil, bus.Message{}
	}
	batch := make([]any, len(pending))
	for k, i := range pending {
		batch[k] = rows[i]
	}
	recorder := common.NewWriteRecorder(deadline)
	reply := l.write.Rev(common.WithWriteRecorder(ctx, recorder), bus.NewMessage(id, batch))
	if _, isErr := reply.Data().(*common.Error); isErr {
		return nil, reply
	}
	if failures := recorder.Failures(); len(failures) > 0 {
		return nil, bus.NewMessage(id, common.NewError("%d write(s) after storing imported rows failed, the first: %v; "+
			"the rows are stored, and importing them again would store stream and trace rows twice", len(failures), failures[0]))
	}
	var failed []rowFailure
	for _, f := range recorder.RowFailures() {
		if f.Row >= 0 && f.Row < len(pending) {
			failed = append(failed, rowFailure{row: pending[f.Row], err: f.Err})
		}
	}
	return failed, reply
}

// firstFailure is the failure a single row's write met: the listener's error reply, else the
// first failure it recorded.
func firstFailure(reply bus.Message, recorder *common.WriteRecorder) error {
	if ce, isErr := reply.Data().(*common.Error); isErr {
		return ce
	}
	if f := recorder.RowFailures(); len(f) > 0 {
		return f[0].Err
	}
	if f := recorder.Failures(); len(f) > 0 {
		return f[0]
	}
	return nil
}

// maxNamedRows caps the rows a not-written answer lists.
const maxNamedRows = 10

// notWritten answers a batch of total rows whose failed rows could not be written.
func notWritten(id bus.MessageID, total int, failed []rowFailure) bus.Message {
	named := make([]string, 0, min(len(failed), maxNamedRows))
	for _, f := range failed[:min(len(failed), maxNamedRows)] {
		named = append(named, strconv.Itoa(f.row))
	}
	if len(failed) > maxNamedRows {
		named = append(named, "...")
	}
	return bus.NewMessage(id, common.NewError("%d of %d imported row(s) were not written after retries (rows %s), the first: %v",
		len(failed), total, strings.Join(named, ","), failed[0].err))
}

// decodeImportWrite returns the frame f carries and its encoded size: a frame of a remote
// batch arrives encoded, one written in process as it was published, with no size.
func decodeImportWrite(f any) (*transferv1.ImportWrite, int, error) {
	switch v := f.(type) {
	case *transferv1.ImportWrite:
		return v, 0, nil
	case []byte:
		w := &transferv1.ImportWrite{}
		if err := proto.Unmarshal(v, w); err != nil {
			return nil, 0, fmt.Errorf("unmarshal an import frame: %w", err)
		}
		return w, len(v), nil
	default:
		return nil, 0, fmt.Errorf("invalid import frame type %T", f)
	}
}

// importedRow returns the write request of the row w carries, as the catalog's write listener
// takes it; nil for an init frame.
func importedRow(w *transferv1.ImportWrite) any {
	switch r := w.GetRow().(type) {
	case *transferv1.ImportWrite_Measure:
		return &data.ImportedMeasureWrite{InternalWriteRequest: r.Measure}
	case *transferv1.ImportWrite_Stream:
		return r.Stream
	case *transferv1.ImportWrite_Trace:
		return r.Trace
	case *transferv1.ImportWrite_Property:
		return r.Property
	default:
		return nil
	}
}
