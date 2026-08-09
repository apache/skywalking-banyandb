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

package grpc

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// paramMode selects how much of a slow query's bound parameters reaches the top-K log.
type paramMode string

const (
	// paramModeNone omits parameters entirely.
	paramModeNone paramMode = "none"
	// paramModeFingerprint reports each parameter's type and length plus a digest that
	// correlates repeats of the same value.
	paramModeFingerprint paramMode = "fingerprint"
	// paramModeRaw logs string values verbatim.
	paramModeRaw paramMode = "raw"
)

// maxRenderedArrayElems bounds how many elements of an array parameter reach the log.
// Expressing a value set as `IN (?)` bound to a str_array/int_array is the documented
// pattern, so an array parameter can legitimately hold thousands of elements; past this
// many, the total count carries the diagnostic signal and the elements are noise.
const maxRenderedArrayElems = 8

var errParamMode = errors.New("invalid bydbql top-K parameter mode")

// parseParamMode validates a --bydbql-topk-param-mode value.
func parseParamMode(mode string) (paramMode, error) {
	switch parsed := paramMode(mode); parsed {
	case paramModeNone, paramModeFingerprint, paramModeRaw:
		return parsed, nil
	default:
		return "", errors.Wrapf(errParamMode, "got %q, want one of none|fingerprint|raw", mode)
	}
}

// redactParams renders one query's bound parameters for the top-K log under mode, or ""
// when there is nothing to render, which keeps the field out of the log line entirely.
//
// It runs at observe time rather than at dump time, so the tracker never holds a raw
// parameter value in memory: whatever mode forbids does not merely go unprinted, it is
// never retained. The cost is irrelevant because only queries already over the slow
// threshold reach this path.
func redactParams(mode paramMode, params []*modelv1.TagValue) string {
	if mode == paramModeNone || len(params) == 0 {
		return ""
	}
	rendered := make([]string, 0, len(params))
	for _, param := range params {
		rendered = append(rendered, redactParam(mode, param))
	}
	return strings.Join(rendered, ", ")
}

// redactParam renders a single parameter. Numeric, timestamp and null values pass through
// verbatim in every mode above none: they are what explains why a query is slow (window
// width, LIMIT, thresholds) and they carry no user-identifying content. Only str and
// str_array are subject to the mode; binary_data is always digested, never rendered.
func redactParam(mode paramMode, param *modelv1.TagValue) string {
	switch value := param.GetValue().(type) {
	case *modelv1.TagValue_Null:
		return "null"
	case *modelv1.TagValue_Int:
		return strconv.FormatInt(value.Int.GetValue(), 10)
	case *modelv1.TagValue_IntArray:
		elems := value.IntArray.GetValue()
		return fmt.Sprintf("int[n=%d]:[%s]", len(elems), joinCapped(len(elems), func(idx int) string {
			return strconv.FormatInt(elems[idx], 10)
		}))
	case *modelv1.TagValue_Timestamp:
		return value.Timestamp.AsTime().UTC().Format(time.RFC3339Nano)
	case *modelv1.TagValue_Str:
		return redactStr(mode, value.Str.GetValue())
	case *modelv1.TagValue_StrArray:
		return redactStrArray(mode, value.StrArray.GetValue())
	case *modelv1.TagValue_BinaryData:
		// Binary is the one type raw does not render verbatim: it would corrupt the log
		// line and carries no diagnostic value beyond its size.
		return fmt.Sprintf("bytes(len=%d):fp=%s", len(value.BinaryData), fingerprint(string(value.BinaryData)))
	default:
		// The oneof above already covers every modelv1.TagValue variant; this branch only
		// guards against a future field added to the proto without a matching case here.
		return "unrecognized"
	}
}

func redactStr(mode paramMode, value string) string {
	if mode == paramModeRaw {
		return strconv.Quote(value)
	}
	return fmt.Sprintf("str(len=%d):fp=%s", len(value), fingerprint(value))
}

func redactStrArray(mode paramMode, values []string) string {
	if mode == paramModeRaw {
		return fmt.Sprintf("str[n=%d]:[%s]", len(values), joinCapped(len(values), func(idx int) string {
			return strconv.Quote(values[idx])
		}))
	}
	return fmt.Sprintf("str[n=%d]:fp=[%s]", len(values), joinCapped(len(values), func(idx int) string {
		return fingerprint(values[idx])
	}))
}

// fingerprint is a stable, UNSALTED 64-bit digest of a parameter value. Unsalted is a
// deliberate tradeoff: it keeps a value's digest identical across restarts and across
// liaison nodes, which is what lets an operator tell "one particular service is always
// slow" from "every service is slow". The cost is that a low-cardinality value — a
// service name, an instance id — can be recovered by digesting a candidate dictionary.
// This hides values from a casual reader of the log; it is not a cryptographic guarantee,
// and it must not be described as one.
func fingerprint(value string) string {
	return strconv.FormatUint(xxhash.Sum64String(value), 16)
}

// joinCapped renders at most maxRenderedArrayElems of n elements, stating how many were
// dropped so a truncated list never reads as a complete one.
func joinCapped(n int, render func(idx int) string) string {
	capped := n
	if capped > maxRenderedArrayElems {
		capped = maxRenderedArrayElems
	}
	parts := make([]string, 0, capped+1)
	for idx := 0; idx < capped; idx++ {
		parts = append(parts, render(idx))
	}
	if n > capped {
		parts = append(parts, fmt.Sprintf("+%d more", n-capped))
	}
	return strings.Join(parts, " ")
}
