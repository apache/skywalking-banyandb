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

// Package inverted implements an inverted index repository.
package inverted

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	roaringpkg "github.com/RoaringBitmap/roaring"
	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/analysis"
	blugeIndex "github.com/blugelabs/bluge/index"
	"github.com/blugelabs/bluge/numeric"
	"github.com/blugelabs/bluge/search"
	segment "github.com/blugelabs/bluge_segment_api"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/analyzer"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	// ExternalSegmentTempDirName is the name of the directory used for temporary external segments.
	ExternalSegmentTempDirName = "external-segment-temp"

	docIDField     = "_id"
	seriesIDField  = "_series_id"
	timestampField = "_timestamp"
	versionField   = "_version"
)

var (
	defaultRangePreloadSize = 1000
	defaultProjection       = []string{docIDField, timestampField}
)

var _ index.Store = (*store)(nil)

// StoreOpts wraps options to create an inverted index repository.
type StoreOpts struct {
	Logger                 *logger.Logger
	Metrics                *Metrics
	PrepareMergeCallback   func(src []*roaringpkg.Bitmap, segments []segment.Segment, id uint64) (dest []*roaringpkg.Bitmap, err error)
	Path                   string
	ExternalSegmentTempDir string
	BatchWaitSec           int64
	CacheMaxBytes          int
	EnableDeduplication    bool
}

type store struct {
	writer  *bluge.Writer
	closer  *run.Closer
	l       *logger.Logger
	metrics *Metrics
}

// ExternalSegmentWrapper wraps Bluge's ExternalSegmentReceiver for BanyanDB usage.
type ExternalSegmentWrapper struct {
	receiver *blugeIndex.ExternalSegmentReceiver
	logger   *logger.Logger
	mutex    sync.RWMutex
}

// NewExternalSegmentWrapper creates a new wrapper for external segment operations.
func NewExternalSegmentWrapper(receiver *blugeIndex.ExternalSegmentReceiver, logger *logger.Logger) *ExternalSegmentWrapper {
	return &ExternalSegmentWrapper{
		receiver: receiver,
		logger:   logger,
	}
}

// StartSegment begins streaming a new segment.
func (esw *ExternalSegmentWrapper) StartSegment() error {
	esw.mutex.Lock()
	defer esw.mutex.Unlock()

	if err := esw.receiver.StartSegment(); err != nil {
		esw.logger.Error().Err(err).Msg("failed to start external segment")
		return errors.Wrap(err, "failed to start external segment")
	}

	esw.logger.Debug().Msg("external segment started")
	return nil
}

// WriteChunk writes a chunk of segment data.
func (esw *ExternalSegmentWrapper) WriteChunk(data []byte) error {
	esw.mutex.Lock()
	defer esw.mutex.Unlock()

	if err := esw.receiver.WriteChunk(data); err != nil {
		esw.logger.Error().Err(err).Int("chunk_size", len(data)).Msg("failed to write external segment chunk")
		return errors.Wrap(err, "failed to write external segment chunk")
	}

	return nil
}

// CompleteSegment signals completion of segment streaming.
func (esw *ExternalSegmentWrapper) CompleteSegment() error {
	esw.mutex.Lock()
	defer esw.mutex.Unlock()

	if err := esw.receiver.CompleteSegment(); err != nil {
		esw.logger.Error().Err(err).Msg("failed to complete external segment")
		return errors.Wrap(err, "failed to complete external segment")
	}

	esw.logger.Info().
		Uint64("bytes_received", esw.receiver.BytesReceived()).
		Str("status", esw.receiver.Status().String()).
		Msg("external segment completed successfully")
	return nil
}

// Status returns the current status of the segment streaming.
func (esw *ExternalSegmentWrapper) Status() string {
	esw.mutex.RLock()
	defer esw.mutex.RUnlock()
	return esw.receiver.Status().String()
}

// BytesReceived returns the number of bytes received so far.
func (esw *ExternalSegmentWrapper) BytesReceived() uint64 {
	esw.mutex.RLock()
	defer esw.mutex.RUnlock()
	return esw.receiver.BytesReceived()
}

var batchPool = pool.Register[*blugeIndex.Batch]("index-bluge-batch")

func generateBatch() *blugeIndex.Batch {
	b := batchPool.Get()
	if b == nil {
		return bluge.NewBatch()
	}
	return b
}

func releaseBatch(b *blugeIndex.Batch) {
	b.Reset()
	batchPool.Put(b)
}

func (s *store) Batch(batch index.Batch) error {
	if !s.closer.AddRunning() {
		return nil
	}
	defer s.closer.Done()
	b := generateBatch()
	defer releaseBatch(b)
	for _, d := range batch.Documents {
		doc := bluge.NewDocument(convert.BytesToString(convert.Uint64ToBytes(d.DocID)))
		for i, f := range d.Fields {
			var tf *bluge.TermField
			switch f.GetTerm().(type) {
			case *index.BytesTermValue:
				tf = bluge.NewKeywordFieldBytes(f.Key.Marshal(), f.GetBytes())
			case *index.FloatTermValue:
				tf = bluge.NewNumericField(f.Key.Marshal(), f.GetFloat())
			default:
				return errors.Errorf("unexpected field type: %T", f.GetTerm())
			}
			if !f.NoSort {
				tf.Sortable()
			}
			if f.Store {
				tf.StoreValue()
			}
			if f.Key.Analyzer != index.AnalyzerUnspecified {
				tf = tf.WithAnalyzer(analyzer.Analyzers[f.Key.Analyzer])
			}
			doc.AddField(tf)
			if i == 0 {
				doc.AddField(bluge.NewKeywordFieldBytes(seriesIDField, f.Key.SeriesID.Marshal()).StoreValue())
			}
		}

		if d.Timestamp > 0 {
			doc.AddField(bluge.NewDateTimeField(timestampField, time.Unix(0, d.Timestamp)).StoreValue())
		}
		b.Insert(doc)
	}
	return s.writer.Batch(b)
}

// NewStore create a new inverted index repository.
func NewStore(opts StoreOpts) (index.SeriesStore, error) {
	if opts.Logger == nil {
		opts.Logger = logger.GetLogger("inverted")
	}
	indexConfig := blugeIndex.DefaultConfig(opts.Path)
	if opts.BatchWaitSec > 0 {
		indexConfig = indexConfig.WithUnsafeBatches().
			WithPersisterNapTimeMSec(int(opts.BatchWaitSec * 1000))
	}
	indexConfig.CacheMaxBytes = opts.CacheMaxBytes
	config := bluge.DefaultConfigWithIndexConfig(indexConfig)
	config.DefaultSearchAnalyzer = analyzer.Analyzers[index.AnalyzerKeyword]
	config.Logger = log.New(opts.Logger, opts.Logger.Module(), 0)
	config = config.WithPrepareMergeCallback(opts.PrepareMergeCallback)
	if opts.ExternalSegmentTempDir != "" {
		absPath, err := filepath.Abs(opts.ExternalSegmentTempDir)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get absolute path for ExternalSegmentTempDir")
		}
		info, err := os.Stat(absPath)
		switch {
		case os.IsNotExist(err):
			if mkErr := os.MkdirAll(absPath, 0o755); mkErr != nil {
				return nil, errors.Wrap(mkErr, "failed to create ExternalSegmentTempDir")
			}
		case err != nil:
			return nil, errors.Wrap(err, "failed to stat ExternalSegmentTempDir")
		case !info.IsDir():
			return nil, errors.Errorf("ExternalSegmentTempDir path exists but is not a directory: %s", absPath)
		}
		opts.ExternalSegmentTempDir = absPath
	}

	config = config.WithExternalSegments(opts.ExternalSegmentTempDir, opts.EnableDeduplication)
	w, err := bluge.OpenWriter(config)
	if err != nil {
		return nil, err
	}
	s := &store{
		writer:  w,
		l:       opts.Logger,
		closer:  run.NewCloser(1),
		metrics: opts.Metrics,
	}
	return s, nil
}

// EnableExternalSegments creates and returns an ExternalSegmentStreamer for streaming external segments.
func (s *store) EnableExternalSegments() (index.ExternalSegmentStreamer, error) {
	if !s.closer.AddRunning() {
		return nil, errors.New("store is closing, cannot enable external segments")
	}
	defer s.closer.Done()

	receiver, err := s.writer.EnableExternalSegments()
	if err != nil {
		s.l.Error().Err(err).Msg("failed to enable external segments on writer")
		return nil, errors.Wrap(err, "failed to enable external segments")
	}

	wrapper := NewExternalSegmentWrapper(receiver, s.l)
	s.l.Info().Msg("external segments enabled successfully")
	return wrapper, nil
}

func (s *store) Close() error {
	s.closer.Done()
	s.closer.CloseThenWait()
	return s.writer.Close()
}

func (s *store) Reset() {
	s.writer.ResetCache()
}

func (s *store) Iterator(ctx context.Context, fieldKey index.FieldKey, termRange index.RangeOpts, order modelv1.Sort,
	preLoadSize int,
) (iter index.FieldIterator[*index.DocumentResult], err error) {
	if !termRange.IsEmpty() && !termRange.Valid() {
		return index.DummyFieldIterator, nil
	}
	if !s.closer.AddRunning() {
		return nil, nil
	}

	reader, err := s.writer.Reader()
	if err != nil {
		return nil, err
	}
	fk := fieldKey.Marshal()
	rangeQuery := bluge.NewBooleanQuery()
	rangeNode := newMustNode()

	rangeQuery = rangeQuery.AddMust(bluge.NewTermQuery(string(fieldKey.SeriesID.Marshal())).
		SetField(seriesIDField))
	rangeNode.Append(newTermNode(string(fieldKey.SeriesID.Marshal()), nil))
	if !termRange.IsEmpty() && termRange.Valid() {
		switch lower := termRange.Lower.(type) {
		case *index.BytesTermValue:
			upper := termRange.Upper.(*index.BytesTermValue)
			rangeQuery.AddMust(bluge.NewTermRangeInclusiveQuery(
				string(lower.Value),
				string(upper.Value),
				termRange.IncludesLower,
				termRange.IncludesUpper,
			).
				SetField(fk))
			rangeNode.Append(newTermRangeInclusiveNode(
				string(lower.Value),
				string(upper.Value),
				termRange.IncludesLower,
				termRange.IncludesUpper, nil,
				false,
			))

		case *index.FloatTermValue:
			upper := termRange.Upper.(*index.FloatTermValue)
			rangeQuery.AddMust(bluge.NewNumericRangeInclusiveQuery(
				lower.Value,
				upper.Value,
				termRange.IncludesLower,
				termRange.IncludesUpper,
			).
				SetField(fk))
			rangeNode.Append(newTermRangeInclusiveNode(
				strconv.FormatFloat(lower.Value, 'f', -1, 64),
				strconv.FormatFloat(upper.Value, 'f', -1, 64),
				termRange.IncludesLower,
				termRange.IncludesUpper,
				nil,
				false,
			))
		default:
			logger.Panicf("unexpected field type: %T", lower)
		}
	}
	if n := appendTimeRangeToQuery(rangeQuery, fieldKey); n != nil {
		rangeNode.Append(n)
	}

	sortedKey := fk
	if order == modelv1.Sort_SORT_DESC {
		sortedKey = "-" + sortedKey
	}
	result := &sortIterator{
		query:       &queryNode{rangeQuery, rangeNode},
		reader:      reader,
		sortedKey:   sortedKey,
		size:        preLoadSize,
		closer:      s.closer,
		ctx:         ctx,
		newIterator: newBlugeMatchIterator,
	}
	return result, nil
}

func appendTimeRangeToQuery(query *bluge.BooleanQuery, fieldKey index.FieldKey) node {
	if fieldKey.TimeRange == nil || !fieldKey.TimeRange.Valid() {
		return nil
	}
	lower := numeric.Float64ToInt64(fieldKey.TimeRange.Lower.(*index.FloatTermValue).Value)
	upper := numeric.Float64ToInt64(fieldKey.TimeRange.Upper.(*index.FloatTermValue).Value)
	query.AddMust(bluge.NewDateRangeInclusiveQuery(
		time.Unix(0, lower),
		time.Unix(0, upper),
		fieldKey.TimeRange.IncludesLower,
		fieldKey.TimeRange.IncludesUpper,
	).SetField(timestampField))
	return newTermRangeInclusiveNode(
		strconv.FormatInt(lower, 10),
		strconv.FormatInt(upper, 10),
		fieldKey.TimeRange.IncludesLower,
		fieldKey.TimeRange.IncludesUpper,
		nil,
		true,
	)
}

func (s *store) MatchField(fieldKey index.FieldKey) (posting.List, posting.List, error) {
	return s.Range(fieldKey, index.RangeOpts{})
}

func (s *store) MatchTerms(field index.Field) (list posting.List, timestamps posting.List, err error) {
	reader, err := s.writer.Reader()
	if err != nil {
		return nil, nil, err
	}

	var query *bluge.BooleanQuery
	switch field.GetTerm().(type) {
	case *index.BytesTermValue:
		query = bluge.NewBooleanQuery()
		query.AddMust(bluge.NewTermQuery(string(field.GetBytes())).SetField(field.Key.Marshal()))
	case *index.FloatTermValue:
		query = bluge.NewBooleanQuery()
		query.AddMust(bluge.NewTermQuery(
			strconv.FormatFloat(field.GetFloat(), 'f', -1, 64)).
			SetField(field.Key.Marshal()))
	case nil:
		return roaring.DummyPostingList, roaring.DummyPostingList, nil
	default:
		return nil, nil, errors.Errorf("unexpected field type: %T", field.GetTerm())
	}
	query.AddMust(bluge.NewTermQuery(string(field.Key.SeriesID.Marshal())).
		SetField(seriesIDField))
	_ = appendTimeRangeToQuery(query, field.Key)

	documentMatchIterator, err := reader.Search(context.Background(), bluge.NewAllMatches(query))
	if err != nil {
		return nil, nil, err
	}
	iter := newBlugeMatchIterator(documentMatchIterator, reader, defaultProjection)
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()
	list, timestamps = roaring.NewPostingList(), roaring.NewPostingList()
	for iter.Next() {
		list.Insert(iter.Val().DocID)
		timestamps.Insert(uint64(iter.Val().Timestamp))
	}
	return list, timestamps, err
}

func (s *store) Match(fieldKey index.FieldKey, matches []string, opts *modelv1.Condition_MatchOption) (posting.List, posting.List, error) {
	if len(matches) == 0 || fieldKey.Analyzer == index.AnalyzerUnspecified {
		return roaring.DummyPostingList, roaring.DummyPostingList, nil
	}
	reader, err := s.writer.Reader()
	if err != nil {
		return nil, nil, err
	}
	analyzer, operator := getMatchOptions(fieldKey.Analyzer, opts)
	fk := fieldKey.Marshal()
	query := bluge.NewBooleanQuery()
	query.AddMust(bluge.NewTermQuery(string(fieldKey.SeriesID.Marshal())).SetField(seriesIDField))
	for _, m := range matches {
		query.AddMust(bluge.NewMatchQuery(m).SetField(fk).
			SetAnalyzer(analyzer).SetOperator(operator))
	}
	_ = appendTimeRangeToQuery(query, fieldKey)
	documentMatchIterator, err := reader.Search(context.Background(), bluge.NewAllMatches(query))
	if err != nil {
		return nil, nil, err
	}
	iter := newBlugeMatchIterator(documentMatchIterator, reader, defaultProjection)
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()
	list, timestamps := roaring.NewPostingList(), roaring.NewPostingList()
	for iter.Next() {
		list.Insert(iter.Val().DocID)
		timestamps.Insert(uint64(iter.Val().Timestamp))
	}
	return list, timestamps, err
}

func getMatchOptions(analyzerOnIndexRule string, opts *modelv1.Condition_MatchOption) (*analysis.Analyzer, bluge.MatchQueryOperator) {
	a := analyzer.Analyzers[analyzerOnIndexRule]
	operator := bluge.MatchQueryOperatorOr
	if opts != nil {
		if opts.Analyzer != index.AnalyzerUnspecified {
			a = analyzer.Analyzers[opts.Analyzer]
		}
		if opts.Operator != modelv1.Condition_MatchOption_OPERATOR_UNSPECIFIED {
			if opts.Operator == modelv1.Condition_MatchOption_OPERATOR_AND {
				operator = bluge.MatchQueryOperatorAnd
			}
		}
	}
	return a, bluge.MatchQueryOperator(operator)
}

func (s *store) Range(fieldKey index.FieldKey, opts index.RangeOpts) (list posting.List, timestamps posting.List, err error) {
	iter, err := s.Iterator(context.TODO(), fieldKey, opts, modelv1.Sort_SORT_ASC, defaultRangePreloadSize)
	if err != nil {
		return roaring.DummyPostingList, roaring.DummyPostingList, err
	}
	list, timestamps = roaring.NewPostingList(), roaring.NewPostingList()
	for iter.Next() {
		list.Insert(iter.Val().DocID)
		timestamps.Insert(uint64(iter.Val().Timestamp))
	}
	err = multierr.Append(err, iter.Close())
	return
}

func (s *store) TakeFileSnapshot(dst string) error {
	reader, err := s.writer.Reader()
	if err != nil {
		return err
	}
	defer reader.Close()
	return reader.Backup(dst, nil)
}

type blugeMatchIterator struct {
	delegated     search.DocumentMatchIterator
	err           error
	closer        io.Closer
	ctx           *search.Context
	loadDocValues []string
	current       index.DocumentResult
	hit           int
}

func newBlugeMatchIterator(delegated search.DocumentMatchIterator, closer io.Closer,
	loadDocValues []string,
) blugeIterator {
	bmi := &blugeMatchIterator{
		delegated:     delegated,
		closer:        closer,
		current:       index.DocumentResult{},
		ctx:           search.NewSearchContext(1, 0),
		loadDocValues: loadDocValues,
	}
	return bmi
}

func (bmi *blugeMatchIterator) Next() bool {
	var match *search.DocumentMatch
	match, bmi.err = bmi.delegated.Next()
	if bmi.err != nil {
		bmi.err = errors.WithMessagef(bmi.err, "failed to get next document, hit: %d", bmi.hit)
		return false
	}
	if match == nil {
		bmi.err = io.EOF
		return false
	}
	bmi.hit = match.HitNumber
	for i := range bmi.current.Values {
		bmi.current.Values[i] = nil
	}
	bmi.current.DocID = 0
	bmi.current.SeriesID = 0
	bmi.current.Timestamp = 0
	bmi.current.SortedValue = nil
	if len(match.SortValue) > 0 {
		bmi.current.SortedValue = match.SortValue[0]
	}
	if len(bmi.loadDocValues) == 0 {
		err := match.VisitStoredFields(bmi.setVal)
		bmi.err = multierr.Combine(bmi.err, err)
		return bmi.err == nil
	}
	if err := match.LoadDocumentValues(bmi.ctx, bmi.loadDocValues); err != nil {
		bmi.err = multierr.Combine(bmi.err, err)
		return false
	}
	for _, dv := range bmi.loadDocValues {
		vv := match.DocValues(dv)
		if len(vv) == 0 {
			continue
		}
		bmi.setVal(dv, vv[0])
	}
	return true
}

func (bmi *blugeMatchIterator) setVal(field string, value []byte) bool {
	switch field {
	case docIDField:
		bmi.current.DocID = convert.BytesToUint64(value)
	case seriesIDField:
		bmi.current.SeriesID = common.SeriesID(convert.BytesToUint64(value))
	case timestampField:
		ts, errTime := bluge.DecodeDateTime(value)
		if errTime != nil {
			bmi.err = errTime
			return false
		}
		bmi.current.Timestamp = ts.UnixNano()
	default:
		bmi.err = errors.Errorf("unexpected field: %s", field)
	}
	return true
}

func (bmi *blugeMatchIterator) Val() index.DocumentResult {
	return bmi.current
}

func (bmi *blugeMatchIterator) Close() error {
	if bmi.closer == nil {
		if errors.Is(bmi.err, io.EOF) {
			return nil
		}
		return bmi.err
	}
	err := bmi.closer.Close()
	if errors.Is(bmi.err, io.EOF) {
		return err
	}
	return multierr.Combine(bmi.err, bmi.closer.Close())
}
