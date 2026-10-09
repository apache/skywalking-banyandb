// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/index/native/criteria"
	"github.com/apache/skywalking-banyandb/pkg/index/nativeanalysis"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

var (
	nativeMissingSortValue     = bytes.Repeat([]byte{0xff}, 10)
	nativeMissingSortValueDesc = []byte{0}
)

// nativePropertyStore is the Property-specific native adapter. It deliberately
// does not implement index.SeriesStore: Property identifiers are arbitrary
// bytes, whereas the generic adapter's posting API is uint64-oriented.
//
//nolint:govet // callback and owner fields are intentionally kept together.
type nativePropertyStore struct {
	owner   *native.Owner
	wait    bool
	observe func(int64, int64)
}

func newNativePropertyStore(
	path string, lease native.PathRootLease, wait bool, persistInterval time.Duration, observe func(int64, int64),
	prepareMerge native.PrepareMergeCallback,
) (*nativePropertyStore, error) {
	if lease == nil {
		return nil, fmt.Errorf("native property store: root lease is required")
	}
	// NewOwner is a synchronous constructor and has no context-bearing API.
	//nolint:contextcheck // construction does not perform cancellable I/O
	ownerOptions := native.OwnerOptions{Lease: lease, Path: path, PrepareMergeCallback: prepareMerge}
	if !wait {
		// Nobody waits for durability, so space persists like the legacy
		// engine's persister nap and let compaction merge a burst first.
		ownerOptions.PersistInterval = persistInterval
	}
	owner, err := native.NewOwner(ownerOptions)
	if err != nil {
		return nil, err
	}
	return &nativePropertyStore{owner: owner, wait: wait, observe: observe}, nil
}

func (s *nativePropertyStore) collectMetrics() {
	if s == nil || s.owner == nil || s.observe == nil {
		return
	}
	count, size := s.owner.Stats()
	s.observe(count, size)
}

func (s *nativePropertyStore) takeFileSnapshot(destination string) error {
	if s == nil || s.owner == nil {
		return native.ErrOwnerClosed
	}
	return s.owner.TakeFileSnapshot(destination)
}

func (s *nativePropertyStore) close() error {
	if s == nil || s.owner == nil {
		return nil
	}
	return s.owner.Close()
}

func (s *nativePropertyStore) batch(ctx context.Context, docs index.Documents, callback func(error)) error {
	if s == nil || s.owner == nil {
		return native.ErrOwnerClosed
	}
	nativeDocs := make([]native.Document, 0, len(docs))
	for documentIndex := range docs {
		document, err := encodeNativePropertyDocument(docs[documentIndex])
		if err != nil {
			return fmt.Errorf("encode property document %d: %w", documentIndex, err)
		}
		nativeDocs = append(nativeDocs, document)
	}
	done := make(chan error, 1)
	wrapped := func(err error) {
		done <- err
		if callback != nil {
			callback(err)
		}
	}
	if err := s.owner.Batch(ctx, native.Batch{Documents: nativeDocs, PersistentCallback: wrapped}); err != nil {
		return err
	}
	if !s.wait {
		return nil
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func encodeNativePropertyDocument(document index.Document) (native.Document, error) {
	result := native.Document{Identifier: bytes.Clone(document.EntityValues), Timestamp: document.Timestamp}
	if len(result.Identifier) == 0 {
		return native.Document{}, native.ErrInvalidDocument
	}
	result.Fields = make([]native.Field, 0, len(document.Fields))
	for fieldIndex := range document.Fields {
		field := &document.Fields[fieldIndex]
		term, ok := field.GetTerm().(*index.BytesTermValue)
		if !ok {
			return native.Document{}, fmt.Errorf("field %q has unsupported term %T", field.Key.Marshal(), field.GetTerm())
		}
		nativeField := native.Field{
			Name:  field.Key.Marshal(),
			Value: bytes.Clone(term.Value),
			Store: field.Store,
			Index: field.Index,
			Sort:  !field.NoSort,
		}
		if field.Key.Analyzer != index.AnalyzerUnspecified && field.Key.Analyzer != index.AnalyzerKeyword {
			terms, err := nativeanalysis.Analyze(field.Key.Analyzer, term.Value)
			if err != nil {
				return native.Document{}, err
			}
			nativeField.Terms = make([]native.Term, 0, len(terms))
			for _, analyzed := range terms {
				nativeField.Terms = append(nativeField.Terms, native.Term{Value: bytes.Clone(analyzed.Value), Frequency: analyzed.Frequency})
			}
		}
		result.Fields = append(result.Fields, nativeField)
	}
	return result, nil
}

func (s *nativePropertyStore) query(ctx context.Context, request *propertyv1.QueryRequest, order *propertyv1.QueryOrder, limit int) ([]*queryProperty, error) {
	if request == nil {
		return nil, errors.New("property query is nil")
	}
	view, err := s.owner.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer view.Close()
	hits, err := s.matchRequest(ctx, view, request)
	if err != nil {
		return nil, err
	}
	if chargeErr := query.Charge(ctx, uint64(len(hits))*64); chargeErr != nil {
		return nil, chargeErr
	}
	if order == nil && limit > 0 && len(hits) > limit {
		hits = hits[:limit]
	}
	if order != nil && order.TagName != "" {
		hits, err = view.SortHits(ctx, hits, native.SortRequest{
			// Property applies the logical limit after revision/tombstone
			// reconciliation at the API layer. Limiting physical hits here can
			// hide the newest revision or leave a page underfilled.
			Field: propertyTagField(order.TagName), Desc: order.Sort == modelv1.Sort_SORT_DESC, Limit: 0,
		})
		if err != nil {
			return nil, err
		}
	}
	result := make([]*queryProperty, 0, len(hits))
	for _, hit := range hits {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		projected, projectErr := view.ProjectHit(ctx, hit, sourceField, deleteField)
		if projectErr != nil {
			return nil, projectErr
		}
		if order != nil && order.TagName != "" {
			sortValue, missing, sortErr := view.ProjectSortValue(ctx, hit, propertyTagField(order.TagName))
			projectErr = sortErr
			if projectErr != nil {
				return nil, projectErr
			}
			if missing {
				if order.Sort == modelv1.Sort_SORT_DESC {
					sortValue = nativeMissingSortValueDesc
				} else {
					sortValue = nativeMissingSortValue
				}
			}
			if err := query.ChargeResult(ctx, uint64(len(firstProjectedValue(projected, sourceField)))+uint64(len(sortValue))+128); err != nil {
				return nil, err
			}
			property, propertyErr := propertyFromNativeProjection(projected, sortValue, missing)
			if propertyErr != nil {
				return nil, propertyErr
			}
			result = append(result, property)
			continue
		}
		if err := query.ChargeResult(ctx, uint64(len(firstProjectedValue(projected, sourceField)))+128); err != nil {
			return nil, err
		}
		property, propertyErr := propertyFromNativeProjection(projected, nil, false)
		if propertyErr != nil {
			return nil, propertyErr
		}
		result = append(result, property)
	}
	return result, nil
}

func firstProjectedValue(projected native.ProjectedHit, field string) []byte {
	values := projected.Fields[field]
	if len(values) == 0 {
		return nil
	}
	return values[0]
}

func (s *nativePropertyStore) lookup(ctx context.Context, identifiers [][]byte) ([]*queryProperty, error) {
	view, err := s.owner.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer view.Close()
	result := make([]*queryProperty, 0, len(identifiers))
	for _, identifier := range identifiers {
		document, found, lookupErr := view.Lookup(ctx, identifier)
		if lookupErr != nil {
			return nil, lookupErr
		}
		if !found {
			continue
		}
		hit := native.QueryHit{Identifier: bytes.Clone(identifier), Timestamp: document.Timestamp}
		projected := native.ProjectedHit{QueryHit: hit, Fields: make(map[string][][]byte)}
		for _, field := range document.Fields {
			projected.Fields[field.Name] = append(projected.Fields[field.Name], bytes.Clone(field.Value))
		}
		property, propertyErr := propertyFromNativeProjection(projected, nil, false)
		if propertyErr != nil {
			return nil, propertyErr
		}
		result = append(result, property)
	}
	return result, nil
}

func propertyFromNativeProjection(projected native.ProjectedHit, sortedValue []byte, sortMissing bool) (*queryProperty, error) {
	result := &queryProperty{
		id:          bytes.Clone(projected.Identifier),
		timestamp:   projected.Timestamp,
		sortedValue: bytes.Clone(sortedValue),
		sortMissing: sortMissing,
	}
	if source := projected.Fields[sourceField]; len(source) > 0 {
		result.source = bytes.Clone(source[0])
	}
	if deleted := projected.Fields[deleteField]; len(deleted) > 0 {
		if len(deleted[0]) != 8 {
			return nil, fmt.Errorf("invalid deletion timestamp length %d: %w", len(deleted[0]), native.ErrCorrupt)
		}
		result.deleteTime = convert.BytesToInt64(deleted[0])
	}
	return result, nil
}

func propertyTagField(name string) string {
	return index.FieldKey{IndexRuleID: uint32(convert.HashStr(name))}.Marshal()
}

func (s *nativePropertyStore) matchRequest(ctx context.Context, view *native.ReadView, request *propertyv1.QueryRequest) ([]native.QueryHit, error) {
	if len(request.Groups) == 0 {
		return nil, fmt.Errorf("property query requires at least one group: %w", logical.ErrInvalidLogicalExpression)
	}
	groups := make([][]byte, len(request.Groups))
	for i := range request.Groups {
		groups[i] = []byte(request.Groups[i])
	}
	// Every conjunct is intersected at posting level before any stored
	// document is decoded: _group alone can match the whole store (every
	// schema-server entity shares schema.SchemaGroup), so matching it on its
	// own and intersecting decoded hits afterwards made each lookup cost
	// O(store size) even when _entity_id pins it to one document.
	// Most selective conjunct first: a segment that lacks the requested id
	// stops after one dictionary lookup instead of decoding its whole
	// _group posting first.
	requests := make([]native.TermSetRequest, 0, 3)
	if len(request.Ids) > 0 {
		ids := make([][]byte, len(request.Ids))
		for i := range request.Ids {
			ids[i] = []byte(request.Ids[i])
		}
		requests = append(requests, native.TermSetRequest{Field: "_entity_id", Terms: ids, Mode: native.MatchAnyTerm})
	}
	if request.Name != "" {
		requests = append(requests, native.TermSetRequest{Field: index.IndexModeName, Terms: [][]byte{[]byte(request.Name)}, Mode: native.MatchAnyTerm})
	}
	requests = append(requests, native.TermSetRequest{Field: "_group", Terms: groups, Mode: native.MatchAnyTerm})
	result, err := view.MatchAllTermSets(ctx, requests)
	if err != nil {
		return nil, err
	}
	if request.Criteria != nil {
		result, err = criteria.Filter(ctx, view, result, request.Criteria, propertyFieldResolver{})
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// propertyFieldResolver implements criteria.FieldResolver for Property: every
// tag name resolves to its hashed engine field, matching propertyTagField's
// use in the write path (shard.go) and in sort field resolution above. No
// tag is ever rejected -- an unindexed tag simply has no postings, so EQ
// finds nothing and NE finds everything -- and MATCH's analyzer always comes
// from the request, never from a resolved schema value.
type propertyFieldResolver struct{}

func (propertyFieldResolver) Field(tagName string) (string, string, bool) {
	return propertyTagField(tagName), "", true
}
