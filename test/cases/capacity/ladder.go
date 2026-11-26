// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package capacity finds, on a node whose query memory is pinned, the largest limit each data model can serve from a data
// set far larger than that limit: a ladder of growing limits, whose top rung is the capacity of the node.
package capacity

import (
	"errors"
	"fmt"
	"strings"
)

// Outcome is what one query returned.
type Outcome struct {
	// Returned is the number of results: traces, elements or data points.
	Returned int
	// Bytes is the size of the payload that came back, as far as the scenario can tell.
	Bytes int
}

// ErrExhausted is what a probe returns when the node refused the query for lack of query memory.
var ErrExhausted = errors.New("query memory exhausted")

// Rung is one probe of a ladder.
type Rung struct {
	Err     error
	Outcome Outcome
	Limit   int
}

// Result is the outcome of a ladder.
type Result struct {
	// Rungs are the probes in the order they ran.
	Rungs []Rung
	// Max is the outcome of the largest limit that succeeded; its Returned is the capacity.
	Max Outcome
	// MaxLimit is the largest limit that succeeded.
	MaxLimit int
	// FirstFail is the smallest limit that was refused, or 0 when none was.
	FirstFail int
	// Saturated reports that even the whole data set was served, so the ladder found no ceiling.
	Saturated bool
}

// Find climbs the limits 1, 2, 4, ... up to total, then bisects between the last limit that succeeded and the first that was
// refused. A probe returns ErrExhausted (possibly wrapped) when the node ran out of query memory; any other error aborts the
// ladder, since a query that fails for another reason says nothing about capacity. total is the number of results the data
// set holds: a limit of total or more asks for all of them.
func Find(total int, probe func(limit int) (Outcome, error)) (*Result, error) {
	if total < 1 {
		return nil, errors.New("capacity: the data set holds no result")
	}
	result := &Result{}
	run := func(limit int) (bool, error) {
		outcome, err := probe(limit)
		result.Rungs = append(result.Rungs, Rung{Limit: limit, Outcome: outcome, Err: err})
		switch {
		case err == nil:
			result.MaxLimit, result.Max = limit, outcome
			return true, nil
		case errors.Is(err, ErrExhausted):
			return false, nil
		default:
			return false, fmt.Errorf("limit %d failed for a reason other than the query memory: %w", limit, err)
		}
	}
	low, high := 0, 0
	for limit := 1; ; limit *= 2 {
		limit = min(limit, total)
		ok, err := run(limit)
		if err != nil {
			return result, err
		}
		if !ok {
			high = limit
			break
		}
		low = limit
		if limit >= total {
			result.Saturated = true
			return result, nil
		}
	}
	for high-low > 1 {
		mid := low + (high-low)/2
		ok, err := run(mid)
		if err != nil {
			return result, err
		}
		if ok {
			low = mid
		} else {
			high = mid
		}
	}
	result.FirstFail = high
	return result, nil
}

// Table renders the rungs of a ladder, one per line.
func (r *Result) Table() string {
	var b strings.Builder
	for _, rung := range r.Rungs {
		status, why := "ok", ""
		if rung.Err != nil {
			status, why = "refused", " "+shorten(rung.Err.Error(), 220)
		}
		fmt.Fprintf(&b, "  limit %-8d %-8s returned %-8d bytes %d%s\n", rung.Limit, status, rung.Outcome.Returned, rung.Outcome.Bytes, why)
	}
	fmt.Fprintf(&b, "  capacity: %d (%d bytes), first refused limit: %d, saturated: %t\n", r.MaxLimit, r.Max.Bytes, r.FirstFail, r.Saturated)
	return b.String()
}

// shorten cuts text to at most n bytes, marking the cut.
func shorten(text string, n int) string {
	if len(text) <= n {
		return text
	}
	return text[:n] + "..."
}
