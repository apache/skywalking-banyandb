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

package common

import (
	"context"
	"errors"

	"github.com/apache/skywalking-banyandb/pkg/meter"
)

// DropReason is why a write listener skipped a row of a message. A skipped row is logged and
// not reported to the writer (the acknowledgement covers the batch), so the listener counts it
// under its reason in its <model>_write_dropped_total counter.
type DropReason string

// The reasons a write listener skips a row for.
const (
	// DropReasonDecode is a row whose bytes are not a write request.
	DropReasonDecode DropReason = "decode"
	// DropReasonInvalidType is a row, or a whole message, of a type the listener does not take.
	DropReasonInvalidType DropReason = "invalid_type"
	// DropReasonInvalidTimestamp is a row whose timestamp is out of the supported range.
	DropReasonInvalidTimestamp DropReason = "invalid_timestamp"
	// DropReasonGroupNotFound is a row for a group this node does not hold.
	DropReasonGroupNotFound DropReason = "group_not_found"
	// DropReasonSchemaNotFound is a row for a resource this node has no schema for, in a group
	// it holds.
	DropReasonSchemaNotFound DropReason = "schema_not_found"
	// DropReasonInvalidElement is any other row the listener cannot store, such as one that
	// does not match its schema.
	DropReasonInvalidElement DropReason = "invalid_element"
)

// RowDropError is the error a write listener skips a row for, tagged with the reason the row
// is counted under. Its message is the one of the error it wraps.
type RowDropError struct {
	Err    error
	Reason DropReason
}

// NewRowDropError tags err with reason.
func NewRowDropError(reason DropReason, err error) error {
	return &RowDropError{Reason: reason, Err: err}
}

func (e *RowDropError) Error() string { return e.Err.Error() }

func (e *RowDropError) Unwrap() error { return e.Err }

// ReasonOf returns the reason err was tagged with by NewRowDropError, anywhere in its chain,
// and DropReasonInvalidElement for an error that carries none.
func ReasonOf(err error) DropReason {
	var de *RowDropError
	if errors.As(err, &de) {
		return de.Reason
	}
	return DropReasonInvalidElement
}

// WriteDrops is where a write listener reports the rows it skips. A nil *WriteDrops, a listener
// built without metrics, still records the rows for a caller that asked for them.
type WriteDrops struct {
	counter meter.Counter
	holds   func(group string) bool
}

// NewWriteDrops returns the WriteDrops that counts in counter, labeling a row with its group
// only when groups holds it, so the label is bounded by the groups the node holds whatever a
// row names. A nil counter returns nil.
func NewWriteDrops[G any](counter meter.Counter, groups interface{ LoadGroup(name string) (G, bool) }) *WriteDrops {
	if counter == nil {
		return nil
	}
	return &WriteDrops{counter: counter, holds: func(group string) bool {
		_, ok := groups.LoadGroup(group)
		return ok
	}}
}

// SkipRow reports that row row of the message, of group, was skipped for reason with err.
//
// A write whose ctx carries a WriteRecorder gets err recorded: its caller is told which rows
// failed and may write them again, so the row is neither lost silently nor counted once per
// attempt. Any other write counts the row. Each row is thus counted at most once, by the node
// whose listener skipped it.
func (d *WriteDrops) SkipRow(ctx context.Context, row int, group string, reason DropReason, err error) {
	if recorderOf(ctx) != nil {
		RecordRowFailure(ctx, row, err)
		return
	}
	d.count(group, reason)
}

// SkipMessage reports that a whole message, which is not a batch of rows, was skipped for
// reason. It is counted once, under no group, unless the write's caller records failures.
func (d *WriteDrops) SkipMessage(ctx context.Context, reason DropReason) {
	if recorderOf(ctx) != nil {
		return
	}
	d.count("", reason)
}

func (d *WriteDrops) count(group string, reason DropReason) {
	if d == nil {
		return
	}
	if group != "" && !d.holds(group) {
		group = ""
	}
	d.counter.Inc(1, group, string(reason))
}
