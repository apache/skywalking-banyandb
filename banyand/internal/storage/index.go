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

package storage

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	idxmetrics "github.com/apache/skywalking-banyandb/pkg/index/metrics"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/index/native/criteria"
	"github.com/apache/skywalking-banyandb/pkg/index/nativeanalysis"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	seriesIndexDirName = "sidx"

	// identifierFieldName, timestampFieldName and versionFieldName mirror the
	// reserved field names pkg/index/native keeps private; the series index
	// needs its own copies to name them in TermSetRequest.Field, SortRequest.
	// Field and ProjectHit's requested field list.
	identifierFieldName = "_id"
	timestampFieldName  = "_timestamp"
	versionFieldName    = "_version"

	// legacyExternalSegmentTempDirName is the previous release's
	// external-segment staging directory name, duplicated here so the series
	// index never needs to import the retired index package for series-index
	// work. newSeriesIndex removes it on open (NIDX-03 §6.3): a rolled-back
	// node's staging directory is meaningless once a native owner is opened
	// on the same sidx directory.
	legacyExternalSegmentTempDirName = "external-segment-temp"
)

func (s *segment[T, O]) IndexDB() IndexDB {
	// Snapshot s.index into a local: the field can be flipped to nil by a
	// concurrent reclaim (closeIfIdle / performDelete) at any time, so a second
	// read could see a different value. Returning the local also avoids boxing a nil *seriesIndex
	// into a typed-nil IndexDB interface, which would defeat the
	// `if indexDB == nil` guard on the caller side.
	idx := s.index
	if idx == nil {
		return nil
	}
	return idx
}

func (s *segment[T, O]) Lookup(ctx context.Context, series []*pbv1.Series) (pbv1.SeriesList, error) {
	idx := s.index
	if idx == nil {
		return nil, ErrSegmentClosed
	}
	sl, err := idx.filter(ctx, series)
	return sl.SeriesList, err
}

// seriesIndex is the per-segment series index. It is an application-layer
// adapter over pkg/index/native, the same way the Property store
// (banyand/property/db/native_property.go) is: the engine owns segments,
// admission, persistence, sorting and projection; this type owns only the
// series document mapping (EncodeSeriesDocument), identity matching
// (seriesUniverse / subjectUniverse) and the IndexDB surface.
//
// Every method on this type starts with `if s == nil { ... }` purely to
// absorb the typed-nil leak that `segment.IndexDB()` could expose if a
// caller bypassed the producer guard there. The guards intentionally do NOT
// make the type fully nil-safe -- they assume that on a non-nil receiver,
// owner / l / metrics / p are all populated by newSeriesIndex (the only
// constructor).
type seriesIndex struct {
	owner   *native.Owner
	l       *logger.Logger
	metrics *idxmetrics.Metrics
	p       common.Position
	// wait selects synchronous persistence (SeriesIndexFlushTimeoutSeconds ==
	// 0): Insert/Update blocks for the owner's durability callback before
	// returning. When false, PersistInterval coalesces a write burst in
	// memory before it is written, like the legacy engine's persister nap,
	// and Insert/Update returns as soon as the batch is admitted.
	wait bool
}

func newSeriesIndex(ctx context.Context, root string, flushTimeoutSeconds int64, cacheMaxBytes int,
	metrics *idxmetrics.Metrics, lease RootLease,
) (*seriesIndex, error) {
	si := &seriesIndex{
		l: logger.Fetch(ctx, "series_index"),
		p: common.GetPosition(ctx),
	}
	if metrics != nil {
		si.metrics = metrics
	}
	indexPath := path.Join(root, seriesIndexDirName)
	removeLegacySeriesIndexArtifacts(si.l, root)

	si.wait = flushTimeoutSeconds <= 0
	var persistInterval time.Duration
	if !si.wait {
		persistInterval = time.Duration(flushTimeoutSeconds) * time.Second
	}
	// NewOwner is a synchronous constructor and has no context-bearing API.
	//nolint:contextcheck // construction does not perform cancellable I/O
	owner, err := native.NewOwner(native.OwnerOptions{
		Lease:               lease,
		Path:                indexPath,
		ExternalDedup:       native.ExternalDedupKeepExisting,
		PresenceCacheBytes:  cacheMaxBytes,
		IdentifierDocValues: true,
		PersistInterval:     persistInterval,
		TimeMetrics:         si.metrics.NativeTimeMetrics(si.p.SegLabelValues()...),
	})
	if err != nil {
		return nil, err
	}
	si.owner = owner
	return si, nil
}

// removeLegacySeriesIndexArtifacts best-effort removes the previous
// release's external-segment staging directory (root/external-segment-temp,
// a SIBLING of sidx -- the previous release's index writer rooted its
// external-segment staging directory at the segment directory, not inside
// the series index path). It is meaningless to (and would otherwise linger
// forever under) a native owner opened on the same directory. Writer
// exclusion itself is owned by BanyanDB's database lock, so there is no
// index-local lock file to clean up here. Errors are logged, not panicked or
// returned: a stray legacy artifact that cannot be removed (for example a
// permissions issue) must not block the segment from opening.
func removeLegacySeriesIndexArtifacts(l *logger.Logger, root string) {
	legacyExternalSegmentTempDir := filepath.Join(root, legacyExternalSegmentTempDirName)
	if err := os.RemoveAll(legacyExternalSegmentTempDir); err != nil {
		l.Warn().Err(err).Str("path", legacyExternalSegmentTempDir).
			Msg("failed to remove legacy external-segment staging directory")
	}
}

// EncodeSeriesDocument maps an index.Document -- the series-index write
// contract every product (Measure, Stream, Trace) uses -- onto the native
// engine's physical Document, per NIDX-03 §6.1:
//
//   - EntityValues becomes Identifier (_id); a document with none is rejected.
//   - Timestamp maps straight to the engine's dedicated Timestamp field,
//     which the encoder indexes, stores and gives doc values only when it is
//     positive.
//   - Version, when positive, becomes a stored-only, unindexed "_version"
//     field (convert.Int64ToBytes); zero/negative carries no field at all.
//   - Each index.Field becomes a native.Field: Index=true fields are indexed
//     (with analyzer terms from pkg/index/nativeanalysis attached whenever
//     the field declares a non-keyword analyzer) and sorted unless NoSort;
//     Index=false fields are stored-only.
//
// It is exported for the offline tools that build sidx directories outside a
// live seriesIndex: the union-sidx builder and the index-mode Measure copy.
func EncodeSeriesDocument(doc index.Document) (native.Document, error) {
	if len(doc.EntityValues) == 0 {
		return native.Document{}, fmt.Errorf("series document has no entity values: %w", native.ErrInvalidDocument)
	}
	nd := native.Document{Identifier: bytes.Clone(doc.EntityValues), Timestamp: doc.Timestamp}
	nd.Fields = make([]native.Field, 0, len(doc.Fields)+1)
	for i := range doc.Fields {
		f := &doc.Fields[i]
		name := f.Key.Marshal()
		term, ok := f.GetTerm().(*index.BytesTermValue)
		if !ok {
			return native.Document{}, fmt.Errorf("series field %q has unsupported term %T", name, f.GetTerm())
		}
		value := bytes.Clone(term.Value)
		// An unindexed field is always stored, matching the previous
		// release's stored-only field behavior (NIDX-03 §6.1): Store=false on
		// an Index=false field would carry no field at all, silently
		// dropping data callers expect SortedValue/ProjectHit to return.
		store := f.Store || !f.Index
		nf := native.Field{Name: name, Value: value, Store: store, Index: f.Index, Sort: f.Index && !f.NoSort}
		if f.Numeric && f.Index && !f.NoSort {
			nf.Ladder, nf.LadderKey = true, f.NumericKey
		}
		if f.Index && f.Key.Analyzer != index.AnalyzerUnspecified && f.Key.Analyzer != index.AnalyzerKeyword {
			terms, analyzeErr := nativeanalysis.Analyze(f.Key.Analyzer, value)
			if analyzeErr != nil {
				return native.Document{}, fmt.Errorf("analyze series field %q: %w", name, analyzeErr)
			}
			nf.Terms = make([]native.Term, 0, len(terms))
			for _, t := range terms {
				nf.Terms = append(nf.Terms, native.Term{Value: bytes.Clone(t.Value), Frequency: t.Frequency})
			}
		}
		nd.Fields = append(nd.Fields, nf)
	}
	if doc.Version > 0 {
		nd.Fields = append(nd.Fields, native.Field{Name: versionFieldName, Value: convert.Int64ToBytes(doc.Version), Store: true})
	}
	return nd, nil
}

func (s *seriesIndex) Insert(docs index.Documents) error {
	if s == nil {
		return ErrSegmentClosed
	}
	return s.batch(docs, native.BatchInsertIfAbsent)
}

func (s *seriesIndex) Update(docs index.Documents) error {
	if s == nil {
		return ErrSegmentClosed
	}
	return s.batch(docs, native.BatchUpsert)
}

// batch encodes docs and admits them through the owner under mode, blocking
// for the durability callback when s.wait (SeriesIndexFlushTimeoutSeconds ==
// 0; see newSeriesIndex).
//
// When !s.wait (the production default, PersistInterval > 0), no
// PersistentCallback is registered: the owner only records a background
// persistence failure into the error Close() surfaces when a flush has no
// pending callbacks (drainPersistence's len(callbacks) == 0 branch). A
// callback that nobody drains -- which is what an unconditional callback
// would be here, since the async caller has already returned -- would
// silently swallow that failure instead, leaving admitted series
// unrecoverable with no error anywhere (see closeResourcesLocked / NIDX-03
// §8/§11, "marks the segment failed").
//
// Batch is a synchronous constructor-adjacent admission call and has no
// cancellable context of its own on the IndexDB surface.
//
//nolint:contextcheck // see above.
func (s *seriesIndex) batch(docs index.Documents, mode native.BatchMode) error {
	if len(docs) == 0 {
		return nil
	}
	nativeDocs := make([]native.Document, len(docs))
	for i := range docs {
		nd, err := EncodeSeriesDocument(docs[i])
		if err != nil {
			return fmt.Errorf("encode series document %d: %w", i, err)
		}
		nativeDocs[i] = nd
	}
	if !s.wait {
		return s.owner.Batch(context.Background(), native.Batch{Documents: nativeDocs, Mode: mode})
	}
	done := make(chan error, 1)
	callback := func(err error) { done <- err }
	if err := s.owner.Batch(context.Background(), native.Batch{Documents: nativeDocs, Mode: mode, PersistentCallback: callback}); err != nil {
		return err
	}
	return <-done
}

func (s *seriesIndex) EnableExternalSegments() (index.ExternalSegmentStreamer, error) {
	if s == nil {
		return nil, ErrSegmentClosed
	}
	return s.owner.EnableExternalSegments()
}

// Stats degrades to (0, 0) on a nil receiver as defense-in-depth in case
// a typed-nil interface leaks past segment.IndexDB().
func (s *seriesIndex) Stats() (dataCount int64, dataSizeBytes int64) {
	if s == nil {
		return 0, 0
	}
	return s.owner.Stats()
}

// collectMetrics reports the series index's native-owner stats and activity
// counters under the segment's label values.
func (s *seriesIndex) collectMetrics() {
	if s == nil || s.metrics == nil {
		return
	}
	labelValues := s.p.SegLabelValues()
	dataCount, dataSizeBytes := s.owner.Stats()
	s.metrics.ObserveNative(dataCount, dataSizeBytes, labelValues...)
	admitted, acquired := s.owner.Activity()
	s.metrics.ObserveNativeActivity(admitted, acquired, labelValues...)
}

// ResetCache drops the series index's insert-presence cache. It never
// touches admitted data (pkg/index/native.Owner.ResetPresenceCache).
func (s *seriesIndex) ResetCache() {
	if s == nil {
		return
	}
	s.owner.ResetPresenceCache()
}

// TakeFileSnapshot streams a consistent copy of the committed generation,
// including admitted segments whose asynchronous persistence has not yet
// completed, to destination.
func (s *seriesIndex) TakeFileSnapshot(destination string) error {
	if s == nil {
		return ErrSegmentClosed
	}
	return s.owner.TakeFileSnapshot(destination)
}

var emptySeriesMatcher = index.SeriesMatcher{}

// convertEntityValuesToSeriesMatcher classifies one series template into an
// exact, prefix or wildcard matcher over _id, exactly as the previous
// release did: a template with no pbv1.AnyTagValue entries matches exactly;
// one whose AnyTagValue run is only a tail matches as a prefix of the
// marshaled leading values; and one with any AnyTagValue elsewhere (or more
// than one run) matches as a wildcard, escaping to the same legacy pattern
// language pkg/index/native's Wildcard dictionary expansion expects.
func convertEntityValuesToSeriesMatcher(series *pbv1.Series) (index.SeriesMatcher, error) {
	var hasAny, hasWildcard bool
	var prefixIndex int
	var localSeries pbv1.Series
	series.CopyTo(&localSeries)

	for i, tv := range localSeries.EntityValues {
		if tv == nil {
			return emptySeriesMatcher, errors.New("unexpected nil tag value")
		}
		if tv == pbv1.AnyTagValue {
			if !hasAny {
				hasAny = true
				prefixIndex = i
			}
			continue
		}
		if hasAny {
			hasWildcard = true
			break
		}
	}

	var err error

	if hasAny {
		if hasWildcard {
			if err = localSeries.MarshalWithWildcard(); err != nil {
				return emptySeriesMatcher, err
			}
			return index.SeriesMatcher{
				Type:  index.SeriesMatcherTypeWildcard,
				Match: localSeries.Buffer,
			}, nil
		}
		localSeries.EntityValues = localSeries.EntityValues[:prefixIndex]
		if err = localSeries.Marshal(); err != nil {
			return emptySeriesMatcher, err
		}
		return index.SeriesMatcher{
			Type:  index.SeriesMatcherTypePrefix,
			Match: localSeries.Buffer,
		}, nil
	}
	if err = localSeries.Marshal(); err != nil {
		return emptySeriesMatcher, err
	}
	return index.SeriesMatcher{
		Type:  index.SeriesMatcherTypeExact,
		Match: localSeries.Buffer,
	}, nil
}

// toNativeTimeRange converts the shared timestamp.TimeRange into the native
// engine's QueryScope.TimeRange shape. A nil input keeps the universe
// unbounded in time.
func toNativeTimeRange(tr *timestamp.TimeRange) *native.TimeRange {
	if tr == nil {
		return nil
	}
	return &native.TimeRange{
		Lower: tr.Start.UnixNano(), Upper: tr.End.UnixNano(),
		IncludesLower: tr.IncludeStart, IncludesUpper: tr.IncludeEnd,
	}
}

// seriesRequest builds the _id MatchTermsSet request of a series search (NIDX-03 §6.2 step 1, "with series"): one request
// carrying the exact terms, prefixes and wildcard patterns every series matcher contributes, ORed together exactly as the
// previous release's boolean should-query did.
func seriesRequest(series []*pbv1.Series, timeRange *timestamp.TimeRange) (native.TermSetRequest, []index.SeriesMatcher, error) {
	matchers := make([]index.SeriesMatcher, len(series))
	var exact, prefix, wildcard [][]byte
	for i := range series {
		matcher, err := convertEntityValuesToSeriesMatcher(series[i])
		if err != nil {
			return native.TermSetRequest{}, nil, err
		}
		matchers[i] = matcher
		switch matcher.Type {
		case index.SeriesMatcherTypeExact:
			exact = append(exact, matcher.Match)
		case index.SeriesMatcherTypePrefix:
			prefix = append(prefix, matcher.Match)
		case index.SeriesMatcherTypeWildcard:
			wildcard = append(wildcard, matcher.Match)
		default:
			return native.TermSetRequest{}, nil, errors.Errorf("unsupported series matcher type: %v", matcher.Type)
		}
	}
	return native.TermSetRequest{
		Field: identifierFieldName, Terms: exact, Prefix: prefix, Wildcard: wildcard,
		Mode: native.MatchAnyTerm, MaxTerms: ^uint64(0),
		Scope: native.QueryScope{TimeRange: toNativeTimeRange(timeRange)},
	}, matchers, nil
}

// seriesUniverse builds the _id MatchTermsSet universe of a series search from seriesRequest.
func seriesUniverse(ctx context.Context, view *native.ReadView, series []*pbv1.Series, timeRange *timestamp.TimeRange,
) ([]native.QueryHit, []index.SeriesMatcher, error) {
	request, matchers, err := seriesRequest(series, timeRange)
	if err != nil {
		return nil, nil, err
	}
	hits, err := view.MatchTermsSet(ctx, request)
	return hits, matchers, err
}

// subjectRequest builds the _im_name MatchTermsSet request of an index-mode search: one literal term, the measure's name.
func subjectRequest(subject string, timeRange *timestamp.TimeRange) native.TermSetRequest {
	return native.TermSetRequest{
		Field: index.IndexModeName, Terms: [][]byte{convert.StringToBytes(subject)},
		Mode: native.MatchAnyTerm, MaxTerms: ^uint64(0), Scope: native.QueryScope{TimeRange: toNativeTimeRange(timeRange)},
	}
}

// subjectUniverse builds the _im_name MatchTermsSet universe (NIDX-03 §6.2
// step 1, "without series"): index-mode Measure reads scope their universe to
// one literal term, the measure's name, instead of series matchers. An empty
// subject deliberately yields an empty (not unbounded) universe.
func subjectUniverse(ctx context.Context, view *native.ReadView, subject string, timeRange *timestamp.TimeRange) ([]native.QueryHit, error) {
	if subject == "" {
		return nil, nil
	}
	return view.MatchTermsSet(ctx, subjectRequest(subject, timeRange))
}

// orderFieldName resolves the engine field SortHits/ProjectSortValue sort on
// for a given index.OrderBy, matching the previous release's SeriesSort.
// order.Index is required for OrderByTypeIndex; a nil one is a caller bug,
// not a corrupt-but-tolerable state, so it is reported as an error here
// rather than dereferenced, matching the previous release's own tolerance of
// the combination (SeriesSort was never reached for it: the gate in search
// below only calls into this function when order.Index != nil).
func orderFieldName(order *index.OrderBy) (string, error) {
	switch order.Type {
	case index.OrderByTypeTime:
		return timestampFieldName, nil
	case index.OrderByTypeIndex:
		if order.Index == nil {
			return "", errors.New("order by index requires a non-nil index rule")
		}
		fk := index.FieldKey{IndexRuleID: order.Index.Metadata.Id}
		return fk.Marshal(), nil
	default:
		return "", errors.Errorf("unsupported order by type: %v", order.Type)
	}
}

// search is the shared engine-call sequence behind Search, SearchWithoutSeries
// and the legacy-named filter helper Lookup uses (NIDX-03 §6.2):
//
//  1. Universe: seriesUniverse (series matchers) when series is non-empty,
//     otherwise subjectUniverse (index mode). The universe is always this
//     explicit MatchTermsSet result, even when it is empty, so sorting an
//     empty candidate set below never panics.
//  2. Filter: pkg/index/native/criteria.Filter over opts.Criteria.
//  3. Order: SortHits on the index-rule field for an index order
//     (opts.Order.Index != nil, type index). A time order in index mode
//     (subject != "") orders by the hits' own timestamps instead: the rows
//     are projected in storage order and permuted afterwards, so it reads no
//     doc value per hit and no tag out of storage order. A time order in a
//     series search, or no order at all, keeps the hits in MatchTermsSet's
//     order and leaves sortedValues nil (a sorted series search changes
//     banyand/measure/query.go's needsSorting decision).
//  4. Results: ProjectHit for the timestamp, "_version" and the projection,
//     series from the identifier, and ProjectSortValue for an index order. The
//     unsorted path also charges the query memory budget per the previous
//     release's parseResult/readSeriesDocument; the sorted path never did.
func (s *seriesIndex) search(ctx context.Context, series []*pbv1.Series, subject string, opts IndexSearchOpts,
) (sd SeriesData, sortedValues [][]byte, err error) {
	if s == nil {
		return SeriesData{}, nil, ErrSegmentClosed
	}
	tracer := query.GetTracer(ctx)
	var span *query.Span
	if tracer != nil {
		span, ctx = tracer.StartSpan(ctx, "seriesIndex.search")
		defer func() {
			if err != nil {
				span.Error(err)
			}
			span.Tagf("matched", "%d", len(sd.SeriesList))
			span.Stop()
		}()
	}

	view, acquireErr := s.owner.Acquire(ctx)
	if acquireErr != nil {
		return SeriesData{}, nil, acquireErr
	}
	defer func() {
		err = multierr.Append(err, view.Close())
	}()

	limited := limitedOrder(series, subject, opts)
	seriesLimited := limitedSeriesOrder(series, subject, opts)
	var hits []native.QueryHit
	switch {
	case limited:
		hits, err = limitedOrderHits(ctx, view, subject, opts)
	case seriesLimited:
		hits, sd.MoreSeries, err = limitedSeriesHits(ctx, view, series, opts)
	default:
		hits, err = candidateHits(ctx, view, series, subject, opts)
	}
	if err != nil {
		return SeriesData{}, nil, err
	}

	// sorted selects SortHits/ProjectSortValue for an index order
	// (opts.Order.Index != nil), and for a time order on an index-mode search.
	// An index-mode row is one document whose _timestamp is the row's
	// timestamp, so an explicit ORDER BY must order by it, before LIMIT, and
	// the segments' sorted values then merge by timestamp too
	// (apache/skywalking#14218). A series search (subject == "") keeps the hit
	// order for a time order: the blocks carry the timestamps and the scan
	// orders them, and sorting there would flip banyand/measure/query.go's
	// needsSorting decision. No order at all stays unsorted, as before.
	timeOrderSorts := opts.Order != nil && opts.Order.Type == index.OrderByTypeTime && subject != ""
	sorted := opts.Order != nil && (opts.Order.Index != nil || timeOrderSorts)
	timeOrder := sorted && opts.Order.Type == index.OrderByTypeTime
	var sortField string
	var timeOrderPerm []int
	if sorted {
		if limited || seriesLimited {
			hits, timeOrderPerm, sortField, err = orderLimitedHits(ctx, hits, opts)
			sd.LimitApplied = true
		} else {
			hits, timeOrderPerm, sortField, err = orderSearchHits(ctx, view, hits, opts)
		}
		if err != nil {
			return SeriesData{}, nil, err
		}
		sortedValues = make([][]byte, 0, len(hits))
	}

	// Query memory budget: charged only on the unsorted path, matching the
	// previous release's parseResult/readSeriesDocument (the sorted path's
	// SeriesSort/sortIterator never charged). This one-time charge accounts
	// for parseResult's fixed per-call overhead plus the requested
	// projection; per-document and per-field-value charges follow below.
	_, allocationScoped := query.AllocationScopeFromContext(ctx)
	if !sorted && !allocationScoped {
		if chargeErr := query.Charge(ctx, 1024+uint64(len(opts.Projection))*64); chargeErr != nil {
			return SeriesData{}, nil, chargeErr
		}
	}

	if reserveErr := query.ReserveAllocation(ctx, 1024+uint64(len(opts.Projection)+1)*64+uint64(len(hits))*64); reserveErr != nil {
		return SeriesData{}, nil, reserveErr
	}
	fields := make([]string, 0, len(opts.Projection)+1)
	for i := range opts.Projection {
		fields = append(fields, opts.Projection[i].Marshal())
	}
	fields = append(fields, versionFieldName)
	hasProjection := len(opts.Projection) > 0

	sd.SeriesList = make(pbv1.SeriesList, 0, len(hits))
	sd.Timestamps = make([]int64, 0, len(hits))
	sd.Versions = make([]int64, 0, len(hits))
	sd.TimestampSet = make([]bool, 0, len(hits))
	sd.VersionSet = make([]bool, 0, len(hits))
	if hasProjection {
		sd.Fields = make(FieldResultList, 0, len(hits))
		if len(opts.MultiValueProjection) > 0 {
			if reserveErr := query.ReserveAllocation(ctx, uint64(len(hits))*8); reserveErr != nil {
				return SeriesData{}, nil, reserveErr
			}
			sd.MultiValueFields = make([]map[string][][]byte, 0, len(hits))
		}
	}

	for _, hit := range hits {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return SeriesData{}, nil, ctxErr
		}
		if appendErr := appendSearchHit(ctx, view, hit, fields, opts, sorted, sortField, hasProjection, &sd, &sortedValues); appendErr != nil {
			return SeriesData{}, nil, appendErr
		}
	}
	if timeOrder {
		sd.permute(timeOrderPerm)
		sortedValues = make([][]byte, len(sd.Timestamps))
		for i, ts := range sd.Timestamps {
			sortedValues[i] = native.TimestampSortValue(ts)
		}
	}
	return sd, sortedValues, nil
}

// orderSearchHits is step 3 of search for a sorted search that is not limited. An index order sorts the hits with
// SortHits and returns the field its sort values are read from. A time order sorts by the hits' own timestamps, which the
// universe already decoded in storage order, so it needs no SortHits pass and no doc-value read per hit: it returns the
// hits in storage order (the rows are projected in that order, like the unsorted path, and permuted by the returned
// indexes afterwards) and an empty sort field (the sort values are derived from the timestamps).
func orderSearchHits(ctx context.Context, view *native.ReadView, hits []native.QueryHit, opts IndexSearchOpts,
) (ordered []native.QueryHit, timePerm []int, sortField string, err error) {
	sortField, err = orderFieldName(opts.Order)
	if err != nil {
		return nil, nil, "", err
	}
	desc := opts.Order.Sort == modelv1.Sort_SORT_DESC
	if opts.Order.Type == index.OrderByTypeTime {
		if reserveErr := query.ReserveAllocation(ctx, uint64(len(hits))*24); reserveErr != nil {
			return nil, nil, "", reserveErr
		}
		return hits, timeOrderPermutation(hits, desc), "", nil
	}
	ordered, err = view.SortHits(ctx, hits, native.SortRequest{Field: sortField, Desc: desc})
	if err != nil {
		return nil, nil, "", err
	}
	if reserveErr := query.ReserveAllocation(ctx, uint64(len(ordered))*24); reserveErr != nil {
		return nil, nil, "", reserveErr
	}
	return ordered, nil, sortField, nil
}

// candidateHits is steps 1 and 2 of search: the universe, then the criteria and the indexed LIKE patterns.
func candidateHits(ctx context.Context, view *native.ReadView, series []*pbv1.Series, subject string, opts IndexSearchOpts,
) ([]native.QueryHit, error) {
	var universe []native.QueryHit
	var err error
	if len(series) > 0 {
		universe, _, err = seriesUniverse(ctx, view, series, opts.TimeRange)
	} else {
		universe, err = subjectUniverse(ctx, view, subject, opts.TimeRange)
	}
	if err != nil {
		return nil, err
	}
	return filterHits(ctx, view, universe, opts)
}

// filterHits applies the criteria and the indexed LIKE patterns of opts to hits.
func filterHits(ctx context.Context, view *native.ReadView, hits []native.QueryHit, opts IndexSearchOpts) ([]native.QueryHit, error) {
	hits, err := criteria.Filter(ctx, view, hits, opts.Criteria, opts.Fields)
	if err != nil {
		return nil, errors.WithMessage(err, "filter series index")
	}
	for _, pattern := range opts.PatternRequests {
		hits, err = view.FilterTermsSet(ctx, hits, pattern)
		if err != nil {
			return nil, errors.WithMessage(err, "filter indexed LIKE pattern")
		}
	}
	return hits, nil
}

// limitedOrder reports that search serves an index-mode ordered search with a limit, and a way to tell which series a
// newer segment already returned, so it can return only the first Limit rows that nothing shadows. The order is the time
// order or the order of an index rule.
func limitedOrder(series []*pbv1.Series, subject string, opts IndexSearchOpts) bool {
	if len(series) != 0 || subject == "" || opts.Limit <= 0 || opts.IsShadowed == nil || opts.Order == nil {
		return false
	}
	switch opts.Order.Type {
	case index.OrderByTypeTime:
		return true
	case index.OrderByTypeIndex:
		return opts.Order.Index != nil
	default:
		return false
	}
}

// orderLimitedHits is step 3 of search for a limited search. A time order returns the hits in storage order with the
// indexes that put the projected rows back in time order. An index order keeps the rank order the search produced and
// returns the field its sort values are read from.
func orderLimitedHits(ctx context.Context, hits []native.QueryHit, opts IndexSearchOpts) ([]native.QueryHit, []int, string, error) {
	if opts.Order.Type == index.OrderByTypeTime {
		stored, perm, err := storageOrderForTimeOrder(ctx, hits, opts.Order.Sort == modelv1.Sort_SORT_DESC)
		return stored, perm, "", err
	}
	sortField, err := orderFieldName(opts.Order)
	return hits, nil, sortField, err
}

// topFetchCap bounds how many ranked candidates the descent may be asked for before the search gives up on it
// and ranks the whole filtered universe instead: past it, the criteria or the shadowing drop so many rows that
// reading more of the index costs about as much as reading everything.
const topFetchCap = 1 << 15

// hitRanker is how a limited search ranks its candidates: top asks the index for the first fetch candidates in order
// without reading them all (nil when the index cannot, for example a time order without a time range), and order ranks
// hits that were already read.
type hitRanker struct {
	top   func(ctx context.Context, fetch int) ([]native.QueryHit, error)
	order func(ctx context.Context, hits []native.QueryHit) ([]native.QueryHit, error)
}

// newHitRanker returns the ranker of the order of opts over the candidates of one term-set request: the _timestamp trie
// for a time order with a time range, and the sort ladder of the index rule's field for an index order.
func newHitRanker(view *native.ReadView, candidates native.TermSetRequest, opts IndexSearchOpts) (hitRanker, error) {
	desc := opts.Order.Sort == modelv1.Sort_SORT_DESC
	timeRange := candidates.Scope.TimeRange
	if opts.Order.Type == index.OrderByTypeTime {
		ranker := hitRanker{order: func(_ context.Context, hits []native.QueryHit) ([]native.QueryHit, error) {
			ordered := make([]native.QueryHit, len(hits))
			for i, at := range timeOrderPermutation(hits, desc) {
				ordered[i] = hits[at]
			}
			return ordered, nil
		}}
		if timeRange != nil {
			ranker.top = func(ctx context.Context, fetch int) ([]native.QueryHit, error) {
				return view.TopTimeHits(ctx, native.TopTimeRequest{Candidates: candidates, Limit: fetch, Desc: desc})
			}
		}
		return ranker, nil
	}
	field, err := orderFieldName(opts.Order)
	if err != nil {
		return hitRanker{}, err
	}
	return hitRanker{
		top: func(ctx context.Context, fetch int) ([]native.QueryHit, error) {
			return view.TopFieldHits(ctx, native.TopFieldRequest{Candidates: candidates, Field: field, Limit: fetch, Desc: desc})
		},
		order: func(ctx context.Context, hits []native.QueryHit) ([]native.QueryHit, error) {
			return view.SortHits(ctx, hits, native.SortRequest{Field: field, Desc: desc})
		},
	}, nil
}

// limitedOrderHits returns the first opts.Limit hits in order, among the candidates of the subject that the criteria keep
// and that no newer segment shadows. It asks the native index for the best-ranked candidates in growing chunks (a
// descent that decodes only about the chunk), filters each chunk, and stops at Limit survivors or when the index has no
// more. Past topFetchCap, or when the index cannot rank, it ranks the whole filtered universe instead.
func limitedOrderHits(ctx context.Context, view *native.ReadView, subject string, opts IndexSearchOpts) ([]native.QueryHit, error) {
	universe := func(ctx context.Context) ([]native.QueryHit, error) {
		return subjectUniverse(ctx, view, subject, opts.TimeRange)
	}
	return rankedPrefix(ctx, view, subjectRequest(subject, opts.TimeRange), universe, opts, opts.Limit, opts.IsShadowed)
}

// limitedSeriesOrder reports that search serves a series search ordered by an index rule with a limit of series: the
// series matchers are the candidates, and the first Limit of them in the order of the rule's tag are enough. Shadowing
// is an index-mode concept, so a search that carries IsShadowed is not one of these.
func limitedSeriesOrder(series []*pbv1.Series, subject string, opts IndexSearchOpts) bool {
	return len(series) != 0 && subject == "" && opts.Limit > 0 && opts.IsShadowed == nil && opts.Order != nil &&
		opts.Order.Type == index.OrderByTypeIndex && opts.Order.Index != nil
}

// limitedSeriesHits returns the first opts.Limit series in order that the series matchers select and the criteria and
// patterns keep, and whether the index holds more of them beyond those. It asks for one series more than Limit to
// know. The caller merges the lists of several segments; a segment's first Limit series are enough for that
// because a series is one document of a segment, so no series takes two places of its list, and the merge stops where a
// truncated list ends (see measure.searchSeriesList).
func limitedSeriesHits(ctx context.Context, view *native.ReadView, series []*pbv1.Series, opts IndexSearchOpts,
) (hits []native.QueryHit, more bool, err error) {
	request, _, err := seriesRequest(series, opts.TimeRange)
	if err != nil {
		return nil, false, err
	}
	universe := func(ctx context.Context) ([]native.QueryHit, error) {
		return view.MatchTermsSet(ctx, request)
	}
	hits, err = rankedPrefix(ctx, view, request, universe, opts, opts.Limit+1, nil)
	if err != nil {
		return nil, false, err
	}
	if len(hits) > opts.Limit {
		return hits[:opts.Limit], true, nil
	}
	return hits, false, nil
}

// rankedPrefix returns the first want hits in the order of opts among the candidates of request that the criteria and
// patterns keep and isShadowed (when not nil) does not drop, or all of them when there are fewer. universe lists every
// candidate, for the fallback that ranks the whole filtered universe.
func rankedPrefix(ctx context.Context, view *native.ReadView, request native.TermSetRequest,
	universe func(context.Context) ([]native.QueryHit, error), opts IndexSearchOpts, want int,
	isShadowed func(context.Context, [][]byte) (map[string]struct{}, error),
) ([]native.QueryHit, error) {
	ranker, err := newHitRanker(view, request, opts)
	if err != nil {
		return nil, err
	}
	var ranked []native.QueryHit // the whole filtered universe in order, once the descent is given up
	filtered := false
	rank := func(fetch int) ([]native.QueryHit, bool, error) {
		if !filtered && ranker.top != nil && fetch <= topFetchCap {
			hits, topErr := ranker.top(ctx, fetch)
			return hits, len(hits) < fetch, topErr
		}
		if !filtered {
			all, universeErr := universe(ctx)
			if universeErr != nil {
				return nil, false, universeErr
			}
			kept, filterErr := filterHits(ctx, view, all, opts)
			if filterErr != nil {
				return nil, false, filterErr
			}
			ranked, filterErr = ranker.order(ctx, kept)
			if filterErr != nil {
				return nil, false, filterErr
			}
			filtered = true
		}
		return ranked[:min(fetch, len(ranked))], fetch >= len(ranked), nil
	}
	shadowed := make(map[string]bool)
	fetch := want
	for {
		top, exhausted, rankErr := rank(fetch)
		if rankErr != nil {
			return nil, rankErr
		}
		candidates := top
		if !filtered {
			if candidates, rankErr = filterHits(ctx, view, top, opts); rankErr != nil {
				return nil, rankErr
			}
			if candidates, rankErr = ranker.order(ctx, candidates); rankErr != nil {
				return nil, rankErr
			}
		}
		survivors := candidates
		if isShadowed != nil {
			if survivors, rankErr = dropShadowed(ctx, candidates, shadowed, isShadowed); rankErr != nil {
				return nil, rankErr
			}
		}
		if len(survivors) >= want || exhausted {
			return survivors[:min(want, len(survivors))], nil
		}
		fetch *= 4
		if fetch > topFetchCap && !filtered {
			fetch = topFetchCap + 1 // the next rank call falls back to the whole universe
		}
	}
}

// dropShadowed removes the hits whose series a newer segment already returned. Each identifier is asked about once:
// known keeps the answers across the chunks of one search.
func dropShadowed(ctx context.Context, hits []native.QueryHit, known map[string]bool,
	isShadowed func(context.Context, [][]byte) (map[string]struct{}, error),
) ([]native.QueryHit, error) {
	var unknown [][]byte
	for i := range hits {
		if _, asked := known[string(hits[i].Identifier)]; !asked {
			known[string(hits[i].Identifier)] = false
			unknown = append(unknown, hits[i].Identifier)
		}
	}
	if len(unknown) > 0 {
		found, err := isShadowed(ctx, unknown)
		if err != nil {
			return nil, err
		}
		for id := range found {
			known[id] = true
		}
	}
	kept := make([]native.QueryHit, 0, len(hits))
	for i := range hits {
		if !known[string(hits[i].Identifier)] {
			kept = append(kept, hits[i])
		}
	}
	return kept, nil
}

// storageOrderForTimeOrder takes hits in time order and returns them in storage order (segment, then document), so the
// caller projects them with sequential reads, with the indexes that put the projected rows back in time order.
func storageOrderForTimeOrder(ctx context.Context, hits []native.QueryHit, desc bool) ([]native.QueryHit, []int, error) {
	if err := query.ReserveAllocation(ctx, uint64(len(hits))*24); err != nil {
		return nil, nil, err
	}
	stored := make([]native.QueryHit, len(hits))
	copy(stored, hits)
	sort.SliceStable(stored, func(i, j int) bool {
		if stored[i].Segment != stored[j].Segment {
			return stored[i].Segment < stored[j].Segment
		}
		return stored[i].DocumentNumber < stored[j].DocumentNumber
	})
	return stored, timeOrderPermutation(stored, desc), nil
}

// timeOrderPermutation returns the indexes of hits in timestamp order, ties by identifier, segment and document, the
// order SortHits gives for the "_timestamp" field, without reading any document.
func timeOrderPermutation(hits []native.QueryHit, desc bool) []int {
	perm := make([]int, len(hits))
	for i := range perm {
		perm[i] = i
	}
	sort.SliceStable(perm, func(i, j int) bool {
		a, b := hits[perm[i]], hits[perm[j]]
		if a.Timestamp != b.Timestamp {
			if desc {
				return a.Timestamp > b.Timestamp
			}
			return a.Timestamp < b.Timestamp
		}
		if c := bytes.Compare(a.Identifier, b.Identifier); c != 0 {
			return c < 0
		}
		if a.Segment != b.Segment {
			return a.Segment < b.Segment
		}
		return a.DocumentNumber < b.DocumentNumber
	})
	return perm
}

// permute reorders every parallel row slice so that row i becomes the row that was at perm[i].
func (sd *SeriesData) permute(perm []int) {
	sd.SeriesList = permuteRows(sd.SeriesList, perm)
	sd.Timestamps = permuteRows(sd.Timestamps, perm)
	sd.Versions = permuteRows(sd.Versions, perm)
	sd.TimestampSet = permuteRows(sd.TimestampSet, perm)
	sd.VersionSet = permuteRows(sd.VersionSet, perm)
	sd.Fields = permuteRows(sd.Fields, perm)
	sd.MultiValueFields = permuteRows(sd.MultiValueFields, perm)
}

func permuteRows[E any](rows []E, perm []int) []E {
	if len(rows) != len(perm) {
		return rows
	}
	out := make([]E, len(rows))
	for i, from := range perm {
		out[i] = rows[from]
	}
	return out
}

// appendSearchHit extends sd (and sortedValues, on the sorted path) with one
// hit's projected row: step 4 of search's doc comment, factored out to keep
// search itself within the project's complexity budget. It charges the
// query memory budget per hit and per projected field value on the unsorted
// path only, matching the previous release's readSeriesDocument.
func appendSearchHit(ctx context.Context, view *native.ReadView, hit native.QueryHit, fields []string,
	opts IndexSearchOpts, sorted bool, sortField string, hasProjection bool, sd *SeriesData, sortedValues *[][]byte,
) error {
	projected, projectErr := view.ProjectHit(ctx, hit, fields...)
	if projectErr != nil {
		return projectErr
	}
	_, allocationScoped := query.AllocationScopeFromContext(ctx)
	if !sorted && !allocationScoped {
		if chargeErr := query.ChargeResult(ctx, 256+uint64(len(opts.Projection))*64); chargeErr != nil {
			return chargeErr
		}
		if chargeErr := query.Charge(ctx, uint64(len(projected.Identifier))); chargeErr != nil {
			return chargeErr
		}
	}
	if reserveErr := reserveSeriesDataHit(ctx, projected.Identifier, len(opts.Projection)); reserveErr != nil {
		return reserveErr
	}
	var ser pbv1.Series
	if unmarshalErr := ser.Unmarshal(projected.Identifier); unmarshalErr != nil {
		return errors.WithMessagef(unmarshalErr, "failed to unmarshal series: %s", projected.Identifier)
	}
	sd.SeriesList = append(sd.SeriesList, &ser)

	hasTimestamp := projected.Timestamp > 0
	sd.Timestamps = append(sd.Timestamps, projected.Timestamp)
	sd.TimestampSet = append(sd.TimestampSet, hasTimestamp)

	var version int64
	versionValues := projected.Fields[versionFieldName]
	hasVersion := len(versionValues) > 0
	if hasVersion {
		// Last stored value wins, matching the previous release's
		// visit-and-overwrite projection semantics.
		version = convert.BytesToInt64(versionValues[len(versionValues)-1])
	}
	sd.Versions = append(sd.Versions, version)
	sd.VersionSet = append(sd.VersionSet, hasVersion)

	if hasProjection {
		row := make(map[string][]byte, len(opts.Projection))
		var multiRow map[string][][]byte
		if len(opts.MultiValueProjection) > 0 {
			if reserveErr := query.ReserveAllocation(ctx, 512+uint64(len(opts.MultiValueProjection))*96); reserveErr != nil {
				return reserveErr
			}
			multiRow = make(map[string][][]byte, len(opts.MultiValueProjection))
		}
		for i := range opts.Projection {
			name := opts.Projection[i].Marshal()
			values := projected.Fields[name]
			if len(values) == 0 {
				continue
			}
			value := values[len(values)-1]
			if _, keepValues := opts.MultiValueProjection[name]; keepValues {
				// ProjectHit already owns the bytes. Charge the retained descriptors and
				// the schema-aware array decode before exposing the complete sequence.
				size := uint64(len(values)) * 96
				for _, item := range values {
					size += uint64(len(item)) * 2
				}
				if reserveErr := query.ReserveAllocation(ctx, size); reserveErr != nil {
					return reserveErr
				}
				if !sorted && !allocationScoped {
					if chargeErr := query.Charge(ctx, size); chargeErr != nil {
						return chargeErr
					}
				}
				multiRow[name] = values
			}
			if !sorted && !allocationScoped {
				if chargeErr := query.Charge(ctx, uint64(len(value))); chargeErr != nil {
					return chargeErr
				}
			}
			row[name] = value
		}
		sd.Fields = append(sd.Fields, row)
		if multiRow != nil {
			sd.MultiValueFields = append(sd.MultiValueFields, multiRow)
		}
	}

	if sorted && sortField != "" {
		sortValue, _, sortErr := view.ProjectSortValue(ctx, hit, sortField)
		if sortErr != nil {
			return sortErr
		}
		*sortedValues = append(*sortedValues, sortValue)
	}
	return nil
}

func (s *seriesIndex) Search(ctx context.Context, series []*pbv1.Series, opts IndexSearchOpts,
) (SeriesData, [][]byte, error) {
	if s == nil {
		return SeriesData{}, nil, ErrSegmentClosed
	}
	return s.search(ctx, series, "", opts)
}

// ExistingIdentifiers returns the identifiers this index holds a document for among the candidates that the subject, time
// range, criteria and patterns of opts select: the check a newer segment answers for the rows of an older one.
func (s *seriesIndex) ExistingIdentifiers(ctx context.Context, identifiers [][]byte, opts IndexSearchOpts) (map[string]struct{}, error) {
	if s == nil {
		return nil, ErrSegmentClosed
	}
	if len(identifiers) == 0 {
		return nil, nil
	}
	view, err := s.owner.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = view.Close() }()
	hits, err := view.MatchTermsSet(ctx, native.TermSetRequest{
		Field: identifierFieldName, Terms: identifiers, Mode: native.MatchAnyTerm, MaxTerms: ^uint64(0),
		Scope: native.QueryScope{TimeRange: toNativeTimeRange(opts.TimeRange)},
	})
	if err != nil {
		return nil, err
	}
	hits, err = filterHits(ctx, view, hits, opts)
	if err != nil {
		return nil, err
	}
	found := make(map[string]struct{}, len(hits))
	for i := range hits {
		found[string(hits[i].Identifier)] = struct{}{}
	}
	return found, nil
}

func (s *seriesIndex) SearchWithoutSeries(ctx context.Context, opts IndexSearchOpts) (SeriesData, [][]byte, error) {
	if s == nil {
		return SeriesData{}, nil, ErrSegmentClosed
	}
	return s.search(ctx, nil, opts.IndexModeSubject, opts)
}

// filter is segment.Lookup's narrow entry point: series matchers only, no
// criteria, no projection, no order, no index-mode subject, no time range.
func (s *seriesIndex) filter(ctx context.Context, series []*pbv1.Series) (SeriesData, error) {
	if s == nil {
		return SeriesData{}, ErrSegmentClosed
	}
	sd, _, err := s.search(ctx, series, "", IndexSearchOpts{})
	return sd, err
}

func (s *seriesIndex) Close() error {
	if s == nil {
		return nil
	}
	s.metrics.DeleteAll(s.p.SegLabelValues()...)
	return s.owner.Close()
}

// reserveSeriesDataHit bounds only the selected identifier being decoded. Escaped
// delimiters are payload; each unescaped delimiter ends one subject/tag value.
func reserveSeriesDataHit(ctx context.Context, identifier []byte, projection int) error {
	var values uint64
	for i := 0; i < len(identifier); i++ {
		switch identifier[i] {
		case '\\':
			i++
		case '|':
			values++
		}
	}
	// Buffer growth, copied strings and binary values use at most a conservative
	// multiple of the encoded payload; protobuf objects scale with value count.
	return query.ReserveAllocation(ctx, 512+uint64(len(identifier))*8+values*256+uint64(projection)*96)
}
