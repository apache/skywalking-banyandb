// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
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

package nativeadapter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/index/nativeanalysis"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	postingroaring "github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const nativeSeriesField = "_series_id"

// SearcherOptions supplies the mandatory native budgets for dictionary
// expansion and candidate materialization. A zero MaxTerms is rejected by
// NewSearcher rather than silently turning MatchField into an unbounded walk.
//
//nolint:govet // option fields are grouped by construction-time vs per-query role, not padding.
type SearcherOptions struct {
	MaxTerms      uint64
	MaxCandidates uint64
	// AsyncPersistence permits Batch to return after native publication but
	// before the immutable root has been durably persisted. The default is
	// synchronous, matching the historical writer's safe default.
	AsyncPersistence bool
	// PersistInterval spaces background persists when AsyncPersistence is
	// set; see native.OwnerOptions.PersistInterval.
	PersistInterval time.Duration
	// TimeMetrics receives time-range pruning counts from every query the
	// owner NewStore creates serves; see native.OwnerOptions.TimeMetrics. It
	// is read only by NewStore, not by NewSearcher.
	TimeMetrics native.TimeMetrics
}

// Searcher binds one request context and one immutable native ReadView. The
// view remains pinned until Close; no method opens a legacy search reader.
type Searcher struct {
	ctx           context.Context
	view          *native.ReadView
	maxTerms      uint64
	maxCandidates uint64
}

var _ index.Searcher = (*Searcher)(nil)

// NewSearcher acquires one immutable native view for the request. Callers
// must close the returned searcher after consuming all iterators.
//
//nolint:contextcheck // the context is retained as the request lifetime.
func (a *Adapter) NewSearcher(ctx context.Context, options SearcherOptions) (*Searcher, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if options.MaxTerms == 0 {
		return nil, fmt.Errorf("native adapter: MaxTerms is required: %w", native.ErrQueryLimit)
	}
	view, err := a.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return &Searcher{ctx: ctx, view: view, maxTerms: options.MaxTerms, maxCandidates: options.MaxCandidates}, nil
}

// Close releases the request's immutable view.
func (s *Searcher) Close() error {
	if s == nil || s.view == nil {
		return nil
	}
	err := s.view.Close()
	s.view = nil
	return err
}

// Match performs analyzed any/all term matching under the field's series and
// time constraints. Analysis is done before entering the native package.
func (s *Searcher) Match(fieldKey index.FieldKey, matches []string, options *modelv1.Condition_MatchOption) (posting.List, posting.List, error) {
	if s == nil || s.view == nil {
		return nil, nil, native.ErrViewClosed
	}
	if len(matches) == 0 || fieldKey.Analyzer == index.AnalyzerUnspecified {
		return postingroaring.DummyPostingList, postingroaring.DummyPostingList, nil
	}
	analyzerName := fieldKey.Analyzer
	mode := native.MatchAnyTerm
	if options != nil {
		if options.Analyzer != index.AnalyzerUnspecified {
			analyzerName = options.Analyzer
		}
		if options.Operator == modelv1.Condition_MatchOption_OPERATOR_AND {
			mode = native.MatchAllTerms
		}
	}
	var intersection map[nativeHitKey]native.QueryHit
	for matchIndex, match := range matches {
		analyzed, err := analyzeTerms(analyzerName, []byte(match))
		if err != nil {
			return nil, nil, err
		}
		terms := make([][]byte, 0, len(analyzed))
		for _, term := range analyzed {
			terms = append(terms, term.Value)
		}
		if len(terms) == 0 {
			return postingroaring.DummyPostingList, postingroaring.DummyPostingList, nil
		}
		hits, matchErr := s.view.MatchTermsSet(s.ctx, native.TermSetRequest{
			Field: fieldKey.Marshal(), Terms: terms, Mode: mode, Scope: s.scope(fieldKey), MaxCandidates: s.maxCandidates,
		})
		if matchErr != nil {
			return nil, nil, matchErr
		}
		if matchIndex == 0 {
			intersection = make(map[nativeHitKey]native.QueryHit, len(hits))
			for _, hit := range hits {
				intersection[nativeHitKey{Segment: hit.Segment, DocumentNumber: hit.DocumentNumber}] = hit
			}
			continue
		}
		seen := make(map[nativeHitKey]struct{}, len(hits))
		for _, hit := range hits {
			seen[nativeHitKey{Segment: hit.Segment, DocumentNumber: hit.DocumentNumber}] = struct{}{}
		}
		for key := range intersection {
			if _, ok := seen[key]; !ok {
				delete(intersection, key)
			}
		}
	}
	if len(intersection) == 0 {
		return postingroaring.DummyPostingList, postingroaring.DummyPostingList, nil
	}
	hits := make([]native.QueryHit, 0, len(intersection))
	for _, hit := range intersection {
		hits = append(hits, hit)
	}
	list, err := hitsToPosting(hits)
	if err != nil {
		return nil, nil, err
	}
	return list, hitsToTimestamps(hits), nil
}

type nativeHitKey struct {
	Segment        uint64
	DocumentNumber uint64
}

// matchFieldPageSize bounds one native sort-cursor page for MatchField's full
// scope drain. It is an efficiency knob only: MatchField loops until the
// cursor is exhausted regardless of this value.
const matchFieldPageSize = 1 << 16

// MatchField returns every document in the field's series/time scope,
// matching the legacy engine's Range-with-empty-options contract: it is a
// scope scan, not a field-presence check. A document that never set the
// field is still returned (its sort position is the cursor's "missing"
// sentinel); the stream NOT filter relies on this to treat "field absent" as
// satisfying a negated HAVING/EQ condition, the same way the legacy index
// always did.
func (s *Searcher) MatchField(fieldKey index.FieldKey) (posting.List, posting.List, error) {
	if s == nil || s.view == nil {
		return nil, nil, native.ErrViewClosed
	}
	iter, err := s.Iterator(s.ctx, fieldKey, index.RangeOpts{}, modelv1.Sort_SORT_ASC, matchFieldPageSize)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = iter.Close() }()
	list, timestamps := postingroaring.NewPostingList(), postingroaring.NewPostingList()
	for iter.Next() {
		result := iter.Val()
		list.Insert(result.DocID)
		timestamps.Insert(uint64(result.Timestamp))
	}
	return list, timestamps, nil
}

// MatchTerms returns exact term membership for one field value.
func (s *Searcher) MatchTerms(field index.Field) (posting.List, posting.List, error) {
	if s == nil || s.view == nil {
		return nil, nil, native.ErrViewClosed
	}
	term, err := exactTerm(field)
	if err != nil {
		return nil, nil, err
	}
	if term == nil {
		return postingroaring.DummyPostingList, postingroaring.DummyPostingList, nil
	}
	hits, err := s.view.MatchTermsSet(s.ctx, native.TermSetRequest{
		Field: field.Key.Marshal(), Terms: [][]byte{term}, Mode: native.MatchAnyTerm,
		Scope: s.scope(field.Key), MaxCandidates: s.maxCandidates,
	})
	if err != nil {
		return nil, nil, err
	}
	list, err := hitsToPosting(hits)
	if err != nil {
		return nil, nil, err
	}
	return list, hitsToTimestamps(hits), nil
}

// Range performs bounded encoded range membership. An empty range means
// indexed-field presence, matching the public index contract.
func (s *Searcher) Range(fieldKey index.FieldKey, options index.RangeOpts) (posting.List, posting.List, error) {
	if s == nil || s.view == nil {
		return nil, nil, native.ErrViewClosed
	}
	if options.IsEmpty() {
		return s.MatchField(fieldKey)
	}
	if !validRange(options) {
		return postingroaring.DummyPostingList, postingroaring.DummyPostingList, nil
	}
	rangeRequest, err := nativeRange(fieldKey.Marshal(), options, s.scope(fieldKey), s.maxTerms, s.maxCandidates)
	if err != nil {
		return nil, nil, err
	}
	hits, err := s.view.MatchRange(s.ctx, rangeRequest)
	if err != nil {
		return nil, nil, err
	}
	list, err := hitsToPosting(hits)
	if err != nil {
		return nil, nil, err
	}
	return list, hitsToTimestamps(hits), nil
}

// Iterator creates a page-backed cursor for one series and optional encoded
// field range. It does not materialize all matching documents.
//
//nolint:contextcheck // the context is retained as the request lifetime.
func (s *Searcher) Iterator(
	ctx context.Context, fieldKey index.FieldKey, termRange index.RangeOpts,
	order modelv1.Sort, pageSize int,
) (index.FieldIterator[*index.DocumentResult], error) {
	if s == nil || s.view == nil {
		return nil, native.ErrViewClosed
	}
	if pageSize <= 0 {
		return nil, fmt.Errorf("native adapter: positive iterator page size required: %w", native.ErrQueryLimit)
	}
	if ctx == nil {
		ctx = s.ctx
	}
	scope := s.scope(fieldKey)
	request := native.SortCursorRequest{
		Selection: native.TermSetRequest{Scope: scope}, SortField: fieldKey.Marshal(),
		Desc: order == modelv1.Sort_SORT_DESC, PageSize: pageSize,
	}
	if !termRange.IsEmpty() {
		if !validRange(termRange) {
			return index.DummyFieldIterator, nil
		}
		rangeRequest, err := nativeRange(fieldKey.Marshal(), termRange, scope, s.maxTerms, s.maxCandidates)
		if err != nil {
			return nil, err
		}
		request.Range = &rangeRequest
	}
	//nolint:contextcheck // the searcher owns one immutable request context.
	cursor, err := s.view.NewSortCursor(s.ctx, request)
	if err != nil {
		return nil, err
	}
	return &cursorIterator{ctx: ctx, ownerCtx: s.ctx, cursor: cursor}, nil
}

// Sort returns a page-backed native sort cursor. Series IDs are represented
// as one bounded OR selection and the timestamp range is evaluated inside the
// pinned native view.
//
//nolint:contextcheck // the context is retained as the request lifetime.
func (s *Searcher) Sort(
	ctx context.Context, seriesIDs []common.SeriesID, fieldKey index.FieldKey,
	order modelv1.Sort, timeRange *timestamp.TimeRange, pageSize int,
) (index.FieldIterator[*index.DocumentResult], error) {
	if s == nil || s.view == nil {
		return nil, native.ErrViewClosed
	}
	if pageSize <= 0 {
		return nil, fmt.Errorf("native adapter: positive sort page size required: %w", native.ErrQueryLimit)
	}
	if ctx == nil {
		ctx = s.ctx
	}
	scope := native.QueryScope{TimeRange: timeScope(fieldKey.TimeRange)}
	if timeRange != nil {
		scope.TimeRange = &native.TimeRange{
			Lower: timeRange.Start.UnixNano(), Upper: timeRange.End.UnixNano(),
			IncludesLower: timeRange.IncludeStart, IncludesUpper: timeRange.IncludeEnd,
		}
	}
	if len(seriesIDs) > 0 {
		scope.SeriesIDs = make([][]byte, len(seriesIDs))
		for i := range seriesIDs {
			scope.SeriesIDs[i] = seriesIDs[i].Marshal()
		}
	}
	request := native.SortCursorRequest{
		Selection: native.TermSetRequest{Scope: native.QueryScope{TimeRange: scope.TimeRange}}, SortField: fieldKey.Marshal(),
		Desc: order == modelv1.Sort_SORT_DESC, PageSize: pageSize,
	}
	if len(seriesIDs) > 0 {
		request.Selection.Scope.SeriesField = nativeSeriesField
		request.Selection.Scope.SeriesIDs = scope.SeriesIDs
	}
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	//nolint:contextcheck // the searcher owns one immutable request context.
	cursor, err := s.view.NewSortCursor(s.ctx, request)
	if err != nil {
		return nil, err
	}
	return &cursorIterator{ctx: ctx, ownerCtx: s.ctx, cursor: cursor}, nil
}

//nolint:govet // iterator state is grouped by pinned cursor ownership.
type cursorIterator struct {
	ctx      context.Context
	ownerCtx context.Context
	cursor   *native.SortCursor
	page     []*index.DocumentResult
	position int
	err      error
}

func (i *cursorIterator) Next() bool {
	if i == nil || i.err != nil {
		return false
	}
	if err := i.ctx.Err(); err != nil {
		i.err = err
		return false
	}
	if err := i.ownerCtx.Err(); err != nil {
		i.err = err
		return false
	}
	if i.position < len(i.page) {
		i.position++
		return true
	}
	page, err := i.cursor.NextPage(i.ctx)
	if err != nil {
		i.err = err
		return false
	}
	i.page = make([]*index.DocumentResult, len(page))
	for j := range page {
		result, resultErr := sortedResult(page[j])
		if resultErr != nil {
			i.err = resultErr
			return false
		}
		i.page[j] = result
	}
	i.position = 1
	return len(i.page) > 0
}
func (i *cursorIterator) Val() *index.DocumentResult { return i.page[i.position-1] }
func (i *cursorIterator) Close() error {
	if i == nil {
		return nil
	}
	if i.cursor == nil {
		return i.err
	}
	closeErr := i.cursor.Close()
	i.cursor = nil
	return errors.Join(i.err, closeErr)
}
func (i *cursorIterator) Query() index.Query { return nil }

func sortedResult(hit native.SortedHit) (*index.DocumentResult, error) {
	return hitResult(hit.QueryHit, hit.SortValue)
}

func hitResult(hit native.QueryHit, sorted []byte) (*index.DocumentResult, error) {
	if len(hit.Identifier) != 8 {
		return nil, fmt.Errorf("native adapter: identifier length %d: %w", len(hit.Identifier), native.ErrCorrupt)
	}
	if len(hit.SeriesID) != 0 && len(hit.SeriesID) != 8 {
		return nil, fmt.Errorf("native adapter: series identifier length %d: %w", len(hit.SeriesID), native.ErrCorrupt)
	}
	var seriesID common.SeriesID
	if len(hit.SeriesID) == 8 {
		seriesID = common.SeriesID(convert.BytesToUint64(hit.SeriesID))
	}
	return &index.DocumentResult{
		EntityValues: bytes.Clone(hit.Identifier), Values: nil, SortedValue: bytes.Clone(sorted),
		SeriesID: seriesID, DocID: convert.BytesToUint64(hit.Identifier), Timestamp: hit.Timestamp,
	}, nil
}

func hitsToPosting(hits []native.QueryHit) (posting.List, error) {
	result := postingroaring.NewPostingList()
	for _, hit := range hits {
		if len(hit.Identifier) != 8 {
			return nil, fmt.Errorf("native adapter: identifier length %d: %w", len(hit.Identifier), native.ErrCorrupt)
		}
		result.Insert(convert.BytesToUint64(hit.Identifier))
	}
	return result, nil
}

func hitsToTimestamps(hits []native.QueryHit) posting.List {
	result := postingroaring.NewPostingList()
	for _, hit := range hits {
		result.Insert(uint64(hit.Timestamp))
	}
	return result
}

func (s *Searcher) scope(fieldKey index.FieldKey) native.QueryScope {
	return native.QueryScope{SeriesField: nativeSeriesField, SeriesID: fieldKey.SeriesID.Marshal(), TimeRange: timeScope(fieldKey.TimeRange)}
}

func timeScope(options *index.RangeOpts) *native.TimeRange {
	if options == nil || options.IsEmpty() || !options.Valid() {
		return nil
	}
	lower, lowerOK := intValue(options.Lower)
	upper, upperOK := intValue(options.Upper)
	if !lowerOK || !upperOK {
		return nil
	}
	return &native.TimeRange{Lower: lower, Upper: upper, IncludesLower: options.IncludesLower, IncludesUpper: options.IncludesUpper}
}

func intValue(value index.IsTermValue) (int64, bool) {
	term, ok := value.(*index.FloatTermValue)
	if !ok {
		return 0, false
	}
	return encoding.Float64ToSortableInt64(term.Value), true
}

func exactTerm(field index.Field) ([]byte, error) {
	switch value := field.GetTerm().(type) {
	case *index.BytesTermValue:
		term := make([]byte, len(value.Value))
		copy(term, value.Value)
		return term, nil
	case *index.FloatTermValue:
		return []byte(strconv.FormatFloat(value.Value, 'f', -1, 64)), nil
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("native adapter: unsupported field term %T", field.GetTerm())
	}
}

func analyzeTerms(name string, value []byte) ([]nativeanalysis.Term, error) {
	return nativeanalysis.Analyze(name, value)
}

func nativeRange(field string, options index.RangeOpts, scope native.QueryScope, maxTerms, maxCandidates uint64) (native.RangeRequest, error) {
	request := native.RangeRequest{
		Field: field, IncludesLower: options.IncludesLower, IncludesUpper: options.IncludesUpper,
		Scope: scope, MaxTerms: maxTerms, MaxCandidates: maxCandidates,
	}
	switch lower := options.Lower.(type) {
	case *index.BytesTermValue:
		upper, ok := options.Upper.(*index.BytesTermValue)
		if !ok {
			return native.RangeRequest{}, fmt.Errorf("native adapter: mixed range term types")
		}
		request.Lower, request.Upper = bytes.Clone(lower.Value), bytes.Clone(upper.Value)
	case *index.FloatTermValue:
		upper, ok := options.Upper.(*index.FloatTermValue)
		if !ok {
			return native.RangeRequest{}, fmt.Errorf("native adapter: mixed range term types")
		}
		request.Lower = numericPrefix(encoding.Float64ToSortableInt64(lower.Value), 0)
		request.Upper = numericPrefix(encoding.Float64ToSortableInt64(upper.Value), 0)
	default:
		return native.RangeRequest{}, fmt.Errorf("native adapter: unsupported range type %T", options.Lower)
	}
	return request, nil
}

func validRange(options index.RangeOpts) bool {
	switch lower := options.Lower.(type) {
	case *index.BytesTermValue:
		upper, ok := options.Upper.(*index.BytesTermValue)
		return ok && bytes.Compare(lower.Value, upper.Value) <= 0
	case *index.FloatTermValue:
		upper, ok := options.Upper.(*index.FloatTermValue)
		if !ok {
			return false
		}
		// FloatTermValue is also the carrier for sortable encoded integers.
		// Comparing the decoded float breaks the MinInt64/MaxInt64 open
		// sentinels (which may decode to NaN); compare their sortable keys.
		return encoding.Float64ToSortableInt64(lower.Value) <= encoding.Float64ToSortableInt64(upper.Value)
	default:
		return false
	}
}
