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

package rowsrc

import (
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// TraceResource is one trace of a group a TraceResolver tells rows apart by.
type TraceResource struct {
	// stored is the tag set a part of the trace stores: its tags without the id tags.
	stored map[string]struct{}
	// Name is the trace's name.
	Name string
	// Index is the trace's position in the traces the resolver was created with.
	Index int
}

// TraceResolver tells the trace of a row from the row's tag set.
type TraceResolver struct {
	bySignature map[string]*TraceResource
	// fallback caches the superset match of signatures that match no trace exactly.
	fallback map[string]*TraceResource
	// declared holds every tag some trace of the group stores; other stored tags were
	// deleted from the schema and do not tell traces apart.
	declared  map[string]struct{}
	only      *TraceResource
	resources []*TraceResource
	scratch   []string
}

// Resolve returns the trace of a row, or nil. A row whose tag set is no trace's exact set
// was written before tags were added to its trace and comes from an old unmerged part: it
// belongs to the only trace whose stored tag set contains all of the row's tags; with no
// or several such traces the row is ambiguous.
func (r *TraceResolver) Resolve(tags map[string][]byte) *TraceResource {
	if r.only != nil {
		return r.only
	}
	var sig string
	sig, r.scratch = rowSignature(tags, r.declared, r.scratch)
	if res, ok := r.bySignature[sig]; ok {
		return res
	}
	if res, ok := r.fallback[sig]; ok {
		return res
	}
	var match *TraceResource
	for _, res := range r.resources {
		if !containsAll(res.stored, r.scratch) {
			continue
		}
		if match != nil {
			match = nil
			break
		}
		match = res
	}
	r.fallback[sig] = match
	return match
}

func containsAll(set map[string]struct{}, names []string) bool {
	for _, n := range names {
		if _, ok := set[n]; !ok {
			return false
		}
	}
	return true
}

// traceSignature is the sorted tag names of a trace without its trace-id and span-id tags:
// exactly the tag columns a part of that trace stores.
func traceSignature(t *databasev1.Trace) string {
	names := make([]string, 0, len(t.GetTags()))
	for _, tag := range t.GetTags() {
		if tag.GetName() == t.GetTraceIdTagName() || tag.GetName() == t.GetSpanIdTagName() {
			continue
		}
		names = append(names, tag.GetName())
	}
	slices.Sort(names)
	return strings.Join(names, "\x00")
}

// rowSignature is the signature of the tags a trace row's columns hold, by tag name and
// limited to the tags some trace of the group declares.
func rowSignature(tags map[string][]byte, declared map[string]struct{}, scratch []string) (string, []string) {
	scratch = scratch[:0]
	for column := range tags {
		name := StoredTagName(column)
		if _, ok := declared[name]; ok {
			scratch = append(scratch, name)
		}
	}
	slices.Sort(scratch)
	scratch = slices.Compact(scratch)
	return strings.Join(scratch, "\x00"), scratch
}

// NewTraceResolver indexes the traces of a group by tag set. Two traces with the same tag set
// cannot be told apart.
func NewTraceResolver(group string, traces []*databasev1.Trace) (*TraceResolver, error) {
	resolver := &TraceResolver{
		bySignature: make(map[string]*TraceResource, len(traces)),
		fallback:    map[string]*TraceResource{},
		declared:    map[string]struct{}{},
	}
	for i, t := range traces {
		name := t.GetMetadata().GetName()
		res := &TraceResource{Name: name, Index: i, stored: map[string]struct{}{}}
		for _, tag := range t.GetTags() {
			if tag.GetName() != t.GetTraceIdTagName() && tag.GetName() != t.GetSpanIdTagName() {
				res.stored[tag.GetName()] = struct{}{}
				resolver.declared[tag.GetName()] = struct{}{}
			}
		}
		sig := traceSignature(t)
		if other, dup := resolver.bySignature[sig]; dup {
			first, second := min(other.Name, name), max(other.Name, name)
			return nil, status.Errorf(codes.FailedPrecondition,
				"traces %s and %s in group %s have the same tag set; export it in native format", first, second, group)
		}
		resolver.bySignature[sig] = res
		resolver.resources = append(resolver.resources, res)
	}
	if len(traces) == 1 {
		resolver.only = resolver.resources[0]
	}
	return resolver, nil
}
