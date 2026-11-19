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
	"errors"

	"github.com/apache/skywalking-banyandb/pkg/meter"
)

// The reasons a write listener skips a row of a message. A skipped row is logged and not
// reported to the writer (the acknowledgement covers the batch), so the listener counts it
// under one of these reasons in its <model>_write_dropped_total counter.
const (
	// DropReasonDecode is a row whose bytes are not a write request.
	DropReasonDecode = "decode"
	// DropReasonInvalidType is a row, or a whole message, of a type the listener does not take.
	DropReasonInvalidType = "invalid_type"
	// DropReasonInvalidTimestamp is a row whose timestamp is out of the supported range.
	DropReasonInvalidTimestamp = "invalid_timestamp"
	// DropReasonGroupNotFound is a row for a group this node does not hold.
	DropReasonGroupNotFound = "group_not_found"
	// DropReasonSchemaNotFound is a row for a resource this node has no schema for.
	DropReasonSchemaNotFound = "schema_not_found"
	// DropReasonInvalidElement is any other row the listener cannot store, such as one that
	// does not match its schema.
	DropReasonInvalidElement = "invalid_element"
)

// RowDropError is the error a write listener skips a row for, tagged with the reason the row
// is counted under. Its message is the one of the error it wraps.
type RowDropError struct {
	Err    error
	Reason string
}

// NewRowDropError tags err with reason.
func NewRowDropError(reason string, err error) error {
	return &RowDropError{Reason: reason, Err: err}
}

func (e *RowDropError) Error() string { return e.Err.Error() }

func (e *RowDropError) Unwrap() error { return e.Err }

// DropReason returns the reason err was tagged with by NewRowDropError, anywhere in its chain,
// and DropReasonInvalidElement for an error that carries none.
func DropReason(err error) string {
	var de *RowDropError
	if errors.As(err, &de) {
		return de.Reason
	}
	return DropReasonInvalidElement
}

// CountRowDrop adds one skipped row of group to dropped, under reason. A nil counter counts
// nothing, so a listener built without metrics needs no guard.
func CountRowDrop(dropped meter.Counter, group, reason string) {
	if dropped == nil {
		return
	}
	dropped.Inc(1, group, reason)
}
