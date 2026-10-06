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
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
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

	// legacyLockFilename and legacyExternalSegmentTempDirName are the
	// previous release's on-disk artifact names (pkg/index/inverted.LockFilename
	// and pkg/index/inverted.ExternalSegmentTempDirName), duplicated here so
	// the series index never imports pkg/index/inverted for series-index work.
	// newSeriesIndex removes both on open (NIDX-03 §6.3): a rolled-back node's
	// bluge writer lock and staging directory are meaningless once a native
	// owner is opened on the same sidx directory.
	legacyLockFilename               = "bluge.pid"
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
	metrics *inverted.Metrics
	p       common.Position
	// wait selects synchronous persistence (SeriesIndexFlushTimeoutSeconds ==
	// 0): Insert/Update blocks for the owner's durability callback before
	// returning. When false, PersistInterval coalesces a write burst in
	// memory before it is written, like the legacy engine's persister nap,
	// and Insert/Update returns as soon as the batch is admitted.
	wait bool
}

func newSeriesIndex(ctx context.Context, root string, flushTimeoutSeconds int64, cacheMaxBytes int,
	metrics *inverted.Metrics, lease RootLease,
) (*seriesIndex, error) {
	si := &seriesIndex{
		l: logger.Fetch(ctx, "series_index"),
		p: common.GetPosition(ctx),
	}
	if metrics != nil {
		si.metrics = metrics
	}
	indexPath := path.Join(root, seriesIndexDirName)
	removeLegacySeriesIndexArtifacts(si.l, root, indexPath)

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
	})
	if err != nil {
		return nil, err
	}
	si.owner = owner
	return si, nil
}

// removeLegacySeriesIndexArtifacts best-effort removes the previous
// release's bluge exclusive-lock file (indexPath/bluge.pid, inside the sidx
// directory itself) and external-segment staging directory
// (root/external-segment-temp, a SIBLING of sidx -- the previous release's
// inverted.StoreOpts.ExternalSegmentTempDir was rooted at the segment
// directory, not inside Path). Both are meaningless to (and would otherwise
// linger forever under) a native owner opened on the same directory. Errors
// are logged, not panicked or returned: a stray legacy artifact that cannot
// be removed (for example a permissions issue) must not block the segment
// from opening.
func removeLegacySeriesIndexArtifacts(l *logger.Logger, root, indexPath string) {
	legacyLockPath := filepath.Join(indexPath, legacyLockFilename)
	if _, statErr := os.Stat(legacyLockPath); statErr == nil {
		if err := lfs.DeleteFile(legacyLockPath); err != nil {
			l.Warn().Err(err).Str("path", legacyLockPath).Msg("failed to remove legacy series index lock file")
		}
	}
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
		// release's bluge.NewStoredOnlyField (NIDX-03 §6.1): Store=false on
		// an Index=false field would carry no field at all, silently
		// dropping data callers expect SortedValue/ProjectHit to return.
		store := f.Store || !f.Index
		nf := native.Field{Name: name, Value: value, Store: store, Index: f.Index, Sort: f.Index && !f.NoSort}
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

// seriesUniverse builds the _id MatchTermsSet universe (NIDX-03 §6.2 step 1,
// "with series"): one request carrying the exact terms, prefixes and
// wildcard patterns every series matcher contributes, ORed together exactly
// as the previous release's boolean should-query did.
func seriesUniverse(ctx context.Context, view *native.ReadView, series []*pbv1.Series, timeRange *timestamp.TimeRange,
) ([]native.QueryHit, []index.SeriesMatcher, error) {
	matchers := make([]index.SeriesMatcher, len(series))
	var exact, prefix, wildcard [][]byte
	for i := range series {
		matcher, err := convertEntityValuesToSeriesMatcher(series[i])
		if err != nil {
			return nil, nil, err
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
			return nil, nil, errors.Errorf("unsupported series matcher type: %v", matcher.Type)
		}
	}
	hits, err := view.MatchTermsSet(ctx, native.TermSetRequest{
		Field: identifierFieldName, Terms: exact, Prefix: prefix, Wildcard: wildcard,
		Mode: native.MatchAnyTerm, MaxTerms: ^uint64(0),
		Scope: native.QueryScope{TimeRange: toNativeTimeRange(timeRange)},
	})
	return hits, matchers, err
}

// subjectUniverse builds the _im_name MatchTermsSet universe (NIDX-03 §6.2
// step 1, "without series"): index-mode Measure reads scope their universe to
// one literal term, the measure's name, instead of series matchers. An empty
// subject deliberately yields an empty (not unbounded) universe.
func subjectUniverse(ctx context.Context, view *native.ReadView, subject string, timeRange *timestamp.TimeRange) ([]native.QueryHit, error) {
	if subject == "" {
		return nil, nil
	}
	return view.MatchTermsSet(ctx, native.TermSetRequest{
		Field: index.IndexModeName, Terms: [][]byte{convert.StringToBytes(subject)},
		Mode: native.MatchAnyTerm, MaxTerms: ^uint64(0),
		Scope: native.QueryScope{TimeRange: toNativeTimeRange(timeRange)},
	})
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
//  3. Order: SortHits on the index-rule field only for an index order
//     (opts.Order.Index != nil); a time order or no order at all keeps the
//     hits in MatchTermsSet's order and leaves sortedValues nil, matching
//     the previous release (a sorted index-mode/measure search changes
//     banyand/measure/query.go's needsSorting decision).
//  4. Results: ProjectHit for the timestamp, "_version" and the projection,
//     series from the identifier, and ProjectSortValue when sorted. The
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

	var universe []native.QueryHit
	if len(series) > 0 {
		universe, _, err = seriesUniverse(ctx, view, series, opts.TimeRange)
	} else {
		universe, err = subjectUniverse(ctx, view, subject, opts.TimeRange)
	}
	if err != nil {
		return SeriesData{}, nil, err
	}

	hits, filterErr := criteria.Filter(ctx, view, universe, opts.Criteria, opts.Fields)
	if filterErr != nil {
		return SeriesData{}, nil, errors.WithMessage(filterErr, "filter series index")
	}

	// sorted selects SortHits/ProjectSortValue only for an index order
	// (opts.Order.Index != nil), matching the previous release and NIDX-03
	// §6.2: a time order (resolveOrderBy's {Type: OrderByTypeTime, Index:
	// nil}) takes the unsorted path below, keeping hits in MatchTermsSet
	// order and sortedValues nil. Sorting on every non-nil Order
	// (including time order) would change index-mode output order and flip
	// banyand/measure/query.go's needsSorting decision.
	sorted := opts.Order != nil && opts.Order.Index != nil
	var sortField string
	if sorted {
		sortField, err = orderFieldName(opts.Order)
		if err != nil {
			return SeriesData{}, nil, err
		}
		hits, err = view.SortHits(ctx, hits, native.SortRequest{Field: sortField, Desc: opts.Order.Sort == modelv1.Sort_SORT_DESC})
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
	if !sorted {
		if chargeErr := query.Charge(ctx, 1024+uint64(len(opts.Projection))*64); chargeErr != nil {
			return SeriesData{}, nil, chargeErr
		}
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
	}

	for _, hit := range hits {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return SeriesData{}, nil, ctxErr
		}
		if appendErr := appendSearchHit(ctx, view, hit, fields, opts, sorted, sortField, hasProjection, &sd, &sortedValues); appendErr != nil {
			return SeriesData{}, nil, appendErr
		}
	}
	return sd, sortedValues, nil
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
	if !sorted {
		if chargeErr := query.ChargeResult(ctx, 256+uint64(len(opts.Projection))*64); chargeErr != nil {
			return chargeErr
		}
		if chargeErr := query.Charge(ctx, uint64(len(projected.Identifier))); chargeErr != nil {
			return chargeErr
		}
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
		for i := range opts.Projection {
			name := opts.Projection[i].Marshal()
			values := projected.Fields[name]
			if len(values) == 0 {
				continue
			}
			value := values[len(values)-1]
			if !sorted {
				if chargeErr := query.Charge(ctx, uint64(len(value))); chargeErr != nil {
					return chargeErr
				}
			}
			row[name] = value
		}
		sd.Fields = append(sd.Fields, row)
	}

	if sorted {
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
