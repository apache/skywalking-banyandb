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

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
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
	path string, lease native.PathRootLease, wait bool, observe func(int64, int64), prepareMerge native.PrepareMergeCallback,
) (*nativePropertyStore, error) {
	if lease == nil {
		return nil, fmt.Errorf("native property store: root lease is required")
	}
	// NewOwner is a synchronous constructor and has no context-bearing API.
	//nolint:contextcheck // construction does not perform cancellable I/O
	owner, err := native.NewOwner(native.OwnerOptions{Lease: lease, Path: path, PrepareMergeCallback: prepareMerge})
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
	result, err := s.terms(ctx, view, "_group", groups, native.MatchAnyTerm)
	if err != nil {
		return nil, err
	}
	if request.Name != "" {
		result, err = s.filterTerms(ctx, view, result, index.IndexModeName, [][]byte{[]byte(request.Name)}, native.MatchAnyTerm)
		if err != nil {
			return nil, err
		}
	}
	if len(request.Ids) > 0 {
		ids := make([][]byte, len(request.Ids))
		for i := range request.Ids {
			ids[i] = []byte(request.Ids[i])
		}
		result, err = s.filterTerms(ctx, view, result, "_entity_id", ids, native.MatchAnyTerm)
		if err != nil {
			return nil, err
		}
	}
	if request.Criteria != nil {
		result, err = s.matchCriteria(ctx, view, result, request.Criteria)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (s *nativePropertyStore) terms(ctx context.Context, view *native.ReadView, field string, terms [][]byte, mode native.TermSetMode) ([]native.QueryHit, error) {
	hits, err := view.MatchTermsSet(ctx, native.TermSetRequest{Field: field, Terms: terms, Mode: mode})
	if err != nil {
		return nil, err
	}
	return append([]native.QueryHit(nil), hits...), nil
}

func (s *nativePropertyStore) filterTerms(
	ctx context.Context, view *native.ReadView, current []native.QueryHit,
	field string, terms [][]byte, mode native.TermSetMode,
) ([]native.QueryHit, error) {
	hits, err := s.terms(ctx, view, field, terms, mode)
	if err != nil {
		return nil, err
	}
	allowed := make(map[nativeHitKey]struct{}, len(hits))
	for _, hit := range hits {
		allowed[nativeHitKey{hit.Segment, hit.DocumentNumber}] = struct{}{}
	}
	result := make([]native.QueryHit, 0, len(current))
	for _, hit := range current {
		if _, ok := allowed[nativeHitKey{hit.Segment, hit.DocumentNumber}]; ok {
			result = append(result, hit)
		}
	}
	return result, nil
}

type nativeHitKey struct {
	segment  uint64
	document uint64
}

func (s *nativePropertyStore) matchCriteria(
	ctx context.Context, view *native.ReadView, universe []native.QueryHit, criteria *modelv1.Criteria,
) ([]native.QueryHit, error) {
	if criteria == nil {
		return universe, nil
	}
	switch expression := criteria.GetExp().(type) {
	case *modelv1.Criteria_Condition:
		return s.matchCondition(ctx, view, universe, expression.Condition)
	case *modelv1.Criteria_Le:
		if expression.Le == nil || (expression.Le.Left == nil && expression.Le.Right == nil) {
			return nil, fmt.Errorf("logical expression has no operands: %w", logical.ErrInvalidLogicalExpression)
		}
		if expression.Le.Left == nil {
			return s.matchCriteria(ctx, view, universe, expression.Le.Right)
		}
		if expression.Le.Right == nil {
			return s.matchCriteria(ctx, view, universe, expression.Le.Left)
		}
		left, err := s.matchCriteria(ctx, view, universe, expression.Le.Left)
		if err != nil {
			return nil, err
		}
		right, err := s.matchCriteria(ctx, view, universe, expression.Le.Right)
		if err != nil {
			return nil, err
		}
		switch expression.Le.Op {
		case modelv1.LogicalExpression_LOGICAL_OP_OR:
			return unionHits(left, right), nil
		case modelv1.LogicalExpression_LOGICAL_OP_AND:
			return intersectHits(left, right), nil
		default:
			return nil, fmt.Errorf("unsupported logical operator %d: %w", expression.Le.Op, logical.ErrInvalidLogicalExpression)
		}
	default:
		return nil, logical.ErrInvalidCriteriaType
	}
}

func (s *nativePropertyStore) matchCondition(
	ctx context.Context, view *native.ReadView, universe []native.QueryHit, condition *modelv1.Condition,
) ([]native.QueryHit, error) {
	if condition == nil || condition.Value == nil || condition.Value.Value == nil {
		return nil, logical.ErrUnsupportedConditionValue
	}
	if condition.Op == modelv1.Condition_BINARY_OP_MATCH {
		if _, ok := condition.Value.Value.(*modelv1.TagValue_Str); !ok {
			return nil, logical.ErrUnsupportedConditionValue
		}
	}
	if condition.Op == modelv1.Condition_BINARY_OP_IN || condition.Op == modelv1.Condition_BINARY_OP_NOT_IN {
		switch condition.Value.Value.(type) {
		case *modelv1.TagValue_StrArray, *modelv1.TagValue_IntArray:
		default:
			return nil, logical.ErrUnsupportedConditionValue
		}
	}
	expr, err := logical.ParseExpr(condition)
	if err != nil {
		return nil, err
	}
	field := propertyTagField(condition.Name)
	values := expr.Bytes()
	match := func(terms [][]byte, mode native.TermSetMode) ([]native.QueryHit, error) {
		hits, matchErr := view.MatchTermsSet(ctx, native.TermSetRequest{Field: field, Terms: terms, Mode: mode})
		if matchErr != nil {
			return nil, matchErr
		}
		return intersectHits(universe, hits), nil
	}
	switch condition.Op {
	case modelv1.Condition_BINARY_OP_EQ:
		if len(values) != 1 {
			return nil, logical.ErrUnsupportedConditionOp
		}
		return match(values, native.MatchAnyTerm)
	case modelv1.Condition_BINARY_OP_IN:
		return match(values, native.MatchAnyTerm)
	case modelv1.Condition_BINARY_OP_HAVING:
		return match(values, native.MatchAllTerms)
	case modelv1.Condition_BINARY_OP_MATCH:
		if len(values) != 1 {
			return nil, logical.ErrUnsupportedConditionOp
		}
		terms, analyzeErr := nativeanalysis.Analyze(condition.MatchOption.GetAnalyzer(), values[0])
		if analyzeErr != nil {
			return nil, analyzeErr
		}
		analyzed := make([][]byte, 0, len(terms))
		for _, term := range terms {
			analyzed = append(analyzed, term.Value)
		}
		mode := native.MatchAnyTerm
		if condition.MatchOption.GetOperator() == modelv1.Condition_MatchOption_OPERATOR_AND {
			mode = native.MatchAllTerms
		}
		return match(analyzed, mode)
	case modelv1.Condition_BINARY_OP_GT, modelv1.Condition_BINARY_OP_GE,
		modelv1.Condition_BINARY_OP_LT, modelv1.Condition_BINARY_OP_LE:
		if len(values) != 1 {
			return nil, logical.ErrUnsupportedConditionOp
		}
		request := native.RangeRequest{Field: field, MaxTerms: ^uint64(0), Lower: nil, Upper: nil}
		switch condition.Op {
		case modelv1.Condition_BINARY_OP_GT, modelv1.Condition_BINARY_OP_GE:
			request.Lower, request.IncludesLower = values[0], condition.Op == modelv1.Condition_BINARY_OP_GE
		default:
			request.Upper, request.IncludesUpper = values[0], condition.Op == modelv1.Condition_BINARY_OP_LE
		}
		hits, rangeErr := view.MatchRange(ctx, request)
		if rangeErr != nil {
			return nil, rangeErr
		}
		return intersectHits(universe, hits), nil
	case modelv1.Condition_BINARY_OP_NE, modelv1.Condition_BINARY_OP_NOT_IN,
		modelv1.Condition_BINARY_OP_NOT_HAVING:
		if condition.Op == modelv1.Condition_BINARY_OP_NE && len(values) != 1 {
			return nil, logical.ErrUnsupportedConditionOp
		}
		var excluded []native.QueryHit
		mode := native.MatchAnyTerm
		if condition.Op == modelv1.Condition_BINARY_OP_NOT_HAVING {
			mode = native.MatchAllTerms
		}
		excluded, err = match(values, mode)
		if err != nil {
			return nil, err
		}
		return subtractHits(universe, excluded), nil
	default:
		return nil, logical.ErrUnsupportedConditionOp
	}
}

func intersectHits(left, right []native.QueryHit) []native.QueryHit {
	rightSet := make(map[nativeHitKey]native.QueryHit, len(right))
	for _, hit := range right {
		rightSet[nativeHitKey{hit.Segment, hit.DocumentNumber}] = hit
	}
	result := make([]native.QueryHit, 0, len(left))
	for _, hit := range left {
		if _, ok := rightSet[nativeHitKey{hit.Segment, hit.DocumentNumber}]; ok {
			result = append(result, hit)
		}
	}
	return result
}

func unionHits(left, right []native.QueryHit) []native.QueryHit {
	result := make([]native.QueryHit, 0, len(left)+len(right))
	seen := make(map[nativeHitKey]struct{}, len(left)+len(right))
	for _, hits := range [][]native.QueryHit{left, right} {
		for _, hit := range hits {
			key := nativeHitKey{hit.Segment, hit.DocumentNumber}
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				result = append(result, hit)
			}
		}
	}
	return result
}

func subtractHits(universe, excluded []native.QueryHit) []native.QueryHit {
	set := make(map[nativeHitKey]struct{}, len(excluded))
	for _, hit := range excluded {
		set[nativeHitKey{hit.Segment, hit.DocumentNumber}] = struct{}{}
	}
	result := make([]native.QueryHit, 0, len(universe))
	for _, hit := range universe {
		if _, ok := set[nativeHitKey{hit.Segment, hit.DocumentNumber}]; !ok {
			result = append(result, hit)
		}
	}
	return result
}
