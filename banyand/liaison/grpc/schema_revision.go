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
	"time"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// clampTimeRangeBegin advances begin to the maximum value in createdAts when begin
// falls before it (Rule 7: query time-range clamp to schema.CreatedAt).
// Returns the (possibly clamped) begin time and empty=true when the clamped begin
// is already after end, which means no data can exist in the resulting range.
// A zero end is treated as unbounded — empty is never true in that case.
// createdAts entries that are zero are ignored.
func clampTimeRangeBegin(begin, end time.Time, createdAts []time.Time) (time.Time, bool) {
	var maxCreatedAt time.Time
	for _, ca := range createdAts {
		if ca.After(maxCreatedAt) {
			maxCreatedAt = ca
		}
	}
	if maxCreatedAt.IsZero() {
		return begin, false
	}
	if begin.Before(maxCreatedAt) {
		begin = maxCreatedAt
	}
	if !end.IsZero() && begin.After(end) {
		return begin, true
	}
	return begin, false
}

// checkQueryGate evaluates the per-group ModRevision gate for a query request.
// It returns the collected group statuses and a shortCircuit flag.
// When shortCircuit is true the caller must not execute the query and should
// return a response populated only with the returned group_statuses map.
// When shortCircuit is false and the returned map is non-empty, all gated
// groups passed (STATUS_SUCCEED) and the caller should attach the map to the
// successful query response.
// An empty groupModRevisions map skips the gate entirely (backward compat).
//
// maxWait is a single overall deadline shared across every "ahead" group rather
// than a per-group budget, so a multi-group query with N gated groups cannot
// exceed maxWait wall-clock time.
func checkQueryGate(
	groups []string,
	name string,
	groupModRevisions map[string]int64,
	getLocatorRevision func(name, group string) (int64, bool),
	maxWait time.Duration,
) (map[string]modelv1.Status, bool) {
	if len(groupModRevisions) == 0 {
		return nil, false
	}
	deadline := time.Now().Add(maxWait)
	groupStatuses := make(map[string]modelv1.Status)
	for _, g := range groups {
		clientRev, revOK := groupModRevisions[g]
		if !revOK || clientRev == 0 {
			continue
		}
		cacheRev, found := getLocatorRevision(name, g)
		if !found {
			groupStatuses[g] = modelv1.Status_STATUS_NOT_FOUND
			continue
		}
		switch {
		case clientRev < cacheRev:
			groupStatuses[g] = modelv1.Status_STATUS_EXPIRED_SCHEMA
		case clientRev > cacheRev:
			currentGroup := g
			reached := awaitRevisionReached(func() int64 {
				rev, _ := getLocatorRevision(name, currentGroup)
				return rev
			}, clientRev, time.Until(deadline))
			if reached {
				groupStatuses[g] = modelv1.Status_STATUS_SUCCEED
			} else {
				groupStatuses[g] = modelv1.Status_STATUS_SCHEMA_NOT_APPLIED
			}
		default:
			groupStatuses[g] = modelv1.Status_STATUS_SUCCEED
		}
	}
	if len(groupStatuses) == 0 {
		return nil, false
	}
	for _, st := range groupStatuses {
		if st != modelv1.Status_STATUS_SUCCEED {
			return groupStatuses, true
		}
	}
	return groupStatuses, false
}

// awaitRevisionReached polls getRevision until the returned value is >= target or maxWait elapses.
// Returns true when the target revision was observed, false on timeout.
// The initial check is performed before any sleep, so a cache that is already at target never
// sleeps. Backoff starts at 10 ms, is multiplied by 1.5 each iteration, and is capped at 1 s.
// If maxWait <= 0 the function returns false immediately without polling.
func awaitRevisionReached(getRevision func() int64, target int64, maxWait time.Duration) bool {
	if getRevision() >= target {
		return true
	}
	if maxWait <= 0 {
		return false
	}
	retryInterval := 10 * time.Millisecond
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		time.Sleep(retryInterval)
		if getRevision() >= target {
			return true
		}
		retryInterval = time.Duration(float64(retryInterval) * 1.5)
		if retryInterval > time.Second {
			retryInterval = time.Second
		}
	}
	return false
}
