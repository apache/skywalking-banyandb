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
	"encoding/binary"
	"fmt"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// MaxMeasurePatternRequestBytes bounds the encoded envelope independently of
// query memory admission. A request below this cap can still exceed the worker's
// decode reservation or the memory remaining for query execution.
const MaxMeasurePatternRequestBytes = 4 << 20

const (
	measurePatternAllocationSlackBytes = 4 << 10
	measurePatternDecodeExpansion      = 64
)

// MeasurePatternEncodeReservationBytes covers the temporary protobuf buffer and
// the enclosing wire body, plus bounded pattern strings, records and allocation
// overhead. The source request already exists; encoding does not recreate its
// object graph. Reserve before encoding and retain the charge until all broadcast
// futures finish, since transports can still reference the body after Broadcast.
func MeasurePatternEncodeReservationBytes(req *measurev1.InternalQueryRequest, patterns []model.IndexedPattern) int64 {
	return int64(proto.Size(req))*2 + int64(len(patterns)+1)*measurePatternAllocationSlackBytes
}

// MeasurePatternDecodeReservationBytes covers the retained wire body, copied
// strings and the object graph created by protobuf decoding. The conservative
// expansion allowance is intentionally larger than encoding's byte-buffer charge:
// compact repeated/nested messages can allocate much more than their wire size.
// Reserve before decoding and retain the charge through response completion in
// the transport scope. Callers must enforce MaxMeasurePatternRequestBytes first.
// At that wire cap, this policy exceeds a 256 MiB query budget; the cap guarantees
// bounded parsing, not admission under every configured query memory budget.
func MeasurePatternDecodeReservationBytes(body []byte) int64 {
	return int64(len(body))*measurePatternDecodeExpansion + measurePatternAllocationSlackBytes
}

// EncodeMeasurePatternRequest writes a bounded versioned envelope without changing protobuf contracts.
func EncodeMeasurePatternRequest(req *measurev1.InternalQueryRequest, patterns []model.IndexedPattern) ([]byte, error) {
	if req.GetRequest() == nil || len(req.GetRequest().GetGroups()) == 0 || req.GetRequest().GetName() == "" ||
		len(patterns) == 0 || len(patterns) > model.MaxIndexedPatterns {
		return nil, fmt.Errorf("invalid measure pattern request")
	}
	if err := req.GetRequest().ValidateAll(); err != nil {
		return nil, fmt.Errorf("invalid native pattern scan: %w", err)
	}
	if req.GetAggReturnPartial() && req.GetRequest().GetAgg() == nil {
		return nil, fmt.Errorf("partial pattern scan requires aggregation")
	}
	size := 10 + proto.Size(req)
	for _, pattern := range patterns {
		if err := pattern.Validate(); err != nil {
			return nil, err
		}
		size += 17 + len(pattern.TagFamily) + len(pattern.TagName) + len(pattern.Pattern) + len(pattern.Escape)
	}
	if size > MaxMeasurePatternRequestBytes {
		return nil, fmt.Errorf("measure pattern request exceeds message limit: %w", native.ErrQueryLimit)
	}
	body := make([]byte, 9, size)
	copy(body, "MQLP")
	body[4] = 1
	payload, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal measure pattern request: %w", err)
	}
	binary.LittleEndian.PutUint32(body[5:9], uint32(len(payload)))
	body = append(body, payload...)
	body = append(body, byte(len(patterns)))
	for _, pattern := range patterns {
		for _, value := range []string{pattern.TagFamily, pattern.TagName, pattern.Pattern, pattern.Escape} {
			body = binary.LittleEndian.AppendUint32(body, uint32(len(value)))
			body = append(body, value...)
		}
		if pattern.IgnoreCase {
			body = append(body, 1)
		} else {
			body = append(body, 0)
		}
	}
	return body, nil
}

// DecodeMeasurePatternRequest validates all lengths and counts before decoding payloads.
func DecodeMeasurePatternRequest(body []byte) (*measurev1.InternalQueryRequest, []model.IndexedPattern, error) {
	invalid := fmt.Errorf("invalid measure pattern envelope")
	if len(body) > MaxMeasurePatternRequestBytes {
		return nil, nil, fmt.Errorf("measure pattern request exceeds message limit: %w", native.ErrQueryLimit)
	}
	if len(body) < 10 || string(body[:4]) != "MQLP" || body[4] != 1 {
		return nil, nil, invalid
	}
	nativeLen := int(binary.LittleEndian.Uint32(body[5:9]))
	if nativeLen > len(body)-10 {
		return nil, nil, invalid
	}
	cursor := 9 + nativeLen
	count := int(body[cursor])
	cursor++
	if count == 0 || count > model.MaxIndexedPatterns {
		return nil, nil, invalid
	}
	// Validate offsets with a fixed-size table before allocating strings or protos.
	var offsets [model.MaxIndexedPatterns][4][2]int
	var modes [model.MaxIndexedPatterns]bool
	for i := 0; i < count; i++ {
		for j, limit := range []int{model.MaxIndexedPatternIdentityBytes, model.MaxIndexedPatternIdentityBytes, native.MaxSQLPatternBytes, utf8.UTFMax} {
			if len(body)-cursor < 4 {
				return nil, nil, invalid
			}
			n := int(binary.LittleEndian.Uint32(body[cursor : cursor+4]))
			cursor += 4
			if n > limit || n > len(body)-cursor {
				return nil, nil, invalid
			}
			offsets[i][j] = [2]int{cursor, cursor + n}
			cursor += n
		}
		if cursor >= len(body) || body[cursor] > 1 {
			return nil, nil, invalid
		}
		modes[i] = body[cursor] == 1
		cursor++
	}
	if cursor != len(body) {
		return nil, nil, invalid
	}
	patterns := make([]model.IndexedPattern, count)
	for i := range patterns {
		values := [4]string{}
		for j, offset := range offsets[i] {
			values[j] = string(body[offset[0]:offset[1]])
		}
		patterns[i] = model.IndexedPattern{TagFamily: values[0], TagName: values[1], Pattern: values[2], Escape: values[3], IgnoreCase: modes[i]}
		if err := patterns[i].Validate(); err != nil {
			return nil, nil, err
		}
	}
	req := new(measurev1.InternalQueryRequest)
	if err := (proto.UnmarshalOptions{RecursionLimit: 128}).Unmarshal(body[9:9+nativeLen], req); err != nil {
		return nil, nil, fmt.Errorf("decode measure pattern request: %w", err)
	}
	if req.GetRequest() == nil || len(req.GetRequest().GetGroups()) == 0 || len(req.GetRequest().GetGroups()) > 64 || req.GetRequest().GetName() == "" {
		return nil, nil, invalid
	}
	if err := req.GetRequest().ValidateAll(); err != nil {
		return nil, nil, fmt.Errorf("invalid native pattern scan: %w", err)
	}
	if req.GetAggReturnPartial() && req.GetRequest().GetAgg() == nil {
		return nil, nil, invalid
	}
	return req, patterns, nil
}
