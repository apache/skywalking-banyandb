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

package tsdb

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var (
	entityPrefix    = []byte("entity_")
	entityPrefixLen = len(entityPrefix)
	seriesPrefix    = []byte("series_")
	maxIntBytes     = convert.Uint64ToBytes(math.MaxUint64)
	zeroIntBytes    = convert.Uint64ToBytes(0)
)

// AnyEntry is the `*` for a regular expression. It could match "any" Entry in an Entity.
var AnyEntry = Entry(nil)

// Entry is an element in an Entity.
type Entry []byte

// Entity denotes an identity of a Series.
// It defined by Stream or Measure schema.
type Entity []Entry

// Marshal encodes an Entity to bytes.
func (e Entity) Marshal() []byte {
	data := make([][]byte, len(e))
	for i, entry := range e {
		data[i] = entry
	}
	return bytes.Join(data, nil)
}

// Prepend inserts an Entry before the first Entry as the prefix.
func (e Entity) Prepend(entry Entry) Entity {
	d := e
	d = append(Entity{entry}, d...)
	return d
}

// Copy an Entity deeply.
func (e Entity) Copy() Entity {
	a := make(Entity, len(e))
	copy(a, e)
	return a
}

// NewEntity return an Entity with an fixed length.
func NewEntity(length int) Entity {
	e := make(Entity, length)
	for i := 0; i < length; i++ {
		e[i] = AnyEntry
	}
	return e
}

// EntityValue represents the value of a tag which is a part of an entity.
type EntityValue *modelv1.TagValue

// EntityValueToEntry transforms EntityValue to Entry.
func EntityValueToEntry(ev EntityValue) (Entry, error) {
	return pbv1.MarshalTagValue(ev)
}

// EntityValues is the encoded Entity.
type EntityValues []EntityValue

// Prepend inserts an EntityValue before the first EntityValue as the prefix.
func (evs EntityValues) Prepend(scope EntityValue) EntityValues {
	return append(EntityValues{scope}, evs...)
}

// Encode EntityValues to tag values.
func (evs EntityValues) Encode() (result []*modelv1.TagValue) {
	for _, v := range evs {
		result = append(result, v)
	}
	return
}

// ToEntity transforms EntityValues to Entity.
func (evs EntityValues) ToEntity() (result Entity, err error) {
	for _, v := range evs {
		entry, errMarshal := EntityValueToEntry(v)
		if errMarshal != nil {
			return nil, err
		}
		result = append(result, entry)
	}
	return
}

// String outputs the string represent of an EntityValue.
func (evs EntityValues) String() string {
	var strBuilder strings.Builder
	vv := evs.Encode()
	for i := 0; i < len(vv); i++ {
		strBuilder.WriteString(vv[i].String())
		if i < len(vv)-1 {
			strBuilder.WriteString(".")
		}
	}
	return strBuilder.String()
}

// DecodeEntityValues decodes tag values to EntityValues.
func DecodeEntityValues(tvv []*modelv1.TagValue) (result EntityValues) {
	for _, tv := range tvv {
		result = append(result, tv)
	}
	return
}

// StrValue returns an EntityValue which wraps a string value.
func StrValue(v string) EntityValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
}

// Int64Value returns an EntityValue which wraps a int64 value.
func Int64Value(v int64) EntityValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: v}}}
}

// MarshalEntityValues encodes EntityValues to bytes.
func MarshalEntityValues(evs EntityValues) ([]byte, error) {
	data := &modelv1.TagFamilyForWrite{}
	for _, v := range evs {
		data.Tags = append(data.Tags, v)
	}
	return proto.Marshal(data)
}

// UnmarshalEntityValues decodes EntityValues from bytes.
func UnmarshalEntityValues(evs []byte) (result EntityValues, err error) {
	data := &modelv1.TagFamilyForWrite{}
	result = make(EntityValues, len(data.Tags))
	if err = proto.Unmarshal(evs, data); err != nil {
		return nil, err
	}
	for _, tv := range data.Tags {
		result = append(result, tv)
	}
	return
}

// Path denotes a expression to match a Series.
// It supports the fuzzy matching more than EQ by setting an entry to AnyEntry.
type Path struct {
	prefix   []byte
	seekKey  []byte
	mask     []byte
	template []byte
	isFull   bool
	offset   int
}

// NewPath return a Path with a matching expression.
func NewPath(matchingExpression []Entry) Path {
	p := Path{
		seekKey:  make([]byte, 0),
		mask:     make([]byte, 0),
		template: make([]byte, 0),
	}

	var encounterAny bool
	for _, e := range matchingExpression {
		if e == nil {
			encounterAny = true
			p.mask = append(p.mask, zeroIntBytes...)
			p.template = append(p.template, zeroIntBytes...)
			continue
		}
		entry := Hash(e)
		if !encounterAny {
			p.offset += 8
		}
		p.mask = append(p.mask, maxIntBytes...)
		p.template = append(p.template, entry...)
	}
	if !encounterAny {
		p.isFull = true
	}
	p.extractPrefix()
	return p
}

func (p *Path) extractPrefix() {
	p.prefix = p.template[:p.offset]
	p.seekKey = p.seekKey[:0]
	p.seekKey = append(p.seekKey, p.prefix...)
	for i := 0; i < len(p.template)-p.offset; i++ {
		p.seekKey = append(p.seekKey, 0)
	}
}

func (p Path) prepend(entry Entry) Path {
	e := Hash(entry)
	p.template = prepend(p.template, e)
	p.offset += len(e)
	p.extractPrefix()
	p.mask = prepend(p.mask, maxIntBytes)
	return p
}

func prepend(src []byte, entry []byte) []byte {
	dst := make([]byte, len(src)+len(entry))
	copy(dst, entry)
	copy(dst[len(entry):], src)
	return dst
}

// SeriesDatabase allows retrieving series.
type SeriesDatabase interface {
	observability.Observable
	io.Closer
	GetByID(id common.SeriesID) (Series, error)
	Get(key []byte, entityValues EntityValues) (Series, error)
	List(ctx context.Context, path Path) (SeriesList, error)
}

type blockDatabase interface {
	shardID() common.ShardID
	span(ctx context.Context, timeRange timestamp.TimeRange) ([]blockDelegate, error)
	create(ctx context.Context, ts time.Time) (blockDelegate, error)
	block(ctx context.Context, id GlobalItemID) (blockDelegate, error)
}

var (
	_ SeriesDatabase = (*seriesDB)(nil)
	_ blockDatabase  = (*seriesDB)(nil)

	dummySeriesList = make(SeriesList, 0)
)

type seriesDB struct {
	seriesMetadata kv.Store
	l              *logger.Logger
	segCtrl        *segmentController
	sync.Mutex
	sID common.ShardID
}

func (s *seriesDB) GetByID(id common.SeriesID) (Series, error) {
	var series string
	if e := s.l.Debug(); e.Enabled() {
		var buf bytes.Buffer
		buf.Write(seriesPrefix)
		buf.Write(id.Marshal())
		data, err := s.seriesMetadata.Get(buf.Bytes())
		if err != nil {
			e.Err(err).Msg("failed to get series id's literal")
			return newSeries(s.context(), id, "unknown", s), nil
		}
		entityValues, err := UnmarshalEntityValues(data)
		if err != nil {
			e.Err(err).Msg("malformed series id's literal")
			return newSeries(s.context(), id, "malformed", s), nil
		}
		series = entityValues.String()
	}
	return newSeries(s.context(), id, series, s), nil
}

func (s *seriesDB) block(ctx context.Context, id GlobalItemID) (blockDelegate, error) {
	seg := s.segCtrl.get(id.segID)
	if seg == nil {
		return nil, nil
	}

	return seg.blockController.get(ctx, id.blockID)
}

func (s *seriesDB) shardID() common.ShardID {
	return s.sID
}

func (s *seriesDB) Get(key []byte, entityValues EntityValues) (Series, error) {
	entityKey := prepend(key, entityPrefix)
	data, err := s.seriesMetadata.Get(entityKey)
	if errors.Is(err, kv.ErrKeyNotFound) {
		s.Lock()
		defer s.Unlock()
		seriesID := bytesToSeriesID(Hash(key))
		encodedData, entityValuesBytes, errDecode := encode(seriesID, entityValues)
		if errDecode != nil {
			return nil, errDecode
		}
		errDecode = s.seriesMetadata.Put(entityKey, encodedData)
		if errDecode != nil {
			return nil, errDecode
		}

		var series string
		if e := s.l.Debug(); e.Enabled() {
			errDecode = s.seriesMetadata.Put(prepend(seriesID.Marshal(), seriesPrefix), entityValuesBytes)
			if errDecode != nil {
				return nil, errDecode
			}
			series = entityValues.String()
			e.Str("series", series).
				Uint64("series_id", uint64(seriesID)).
				Msg("create a new series")
		}
		return newSeries(s.context(), seriesID, series, s), nil
	}
	if err != nil {
		return nil, err
	}
	seriesID, entityValues, err := decode(data)
	if err != nil {
		return nil, err
	}

	return newSeries(s.context(), seriesID, entityValues.String(), s), nil
}

func encode(seriesID common.SeriesID, evv EntityValues) ([]byte, []byte, error) {
	data, err := MarshalEntityValues(evv)
	if err != nil {
		return nil, nil, err
	}
	var buf bytes.Buffer
	buf.Write(convert.Uint64ToBytes(uint64(seriesID)))
	buf.Write(data)
	return buf.Bytes(), data, nil
}

func decode(value []byte) (common.SeriesID, EntityValues, error) {
	seriesID := convert.BytesToUint64(value[:8])
	entityValues, err := UnmarshalEntityValues(value[8:])
	if err != nil {
		return 0, nil, err
	}
	return common.SeriesID(seriesID), entityValues, nil
}

func (s *seriesDB) List(ctx context.Context, path Path) (SeriesList, error) {
	prefix := prepend(path.prefix, entityPrefix)
	l := logger.FetchOrDefault(ctx, "series_database", s.l)
	if path.isFull {
		data, err := s.seriesMetadata.Get(prefix)
		if err != nil && errors.Is(err, kv.ErrKeyNotFound) {
			if e := l.Debug(); e.Enabled() {
				e.Hex("path", path.prefix).Msg("doesn't get any series")
			}
			return dummySeriesList, nil
		}
		if err != nil {
			return nil, err
		}
		seriesID, entityValue, err := decode(data)
		if err != nil {
			return nil, err
		}
		var series string
		if e := l.Debug(); e.Enabled() {
			series = entityValue.String()
			e.Int("prefix_len", path.offset/8).
				Str("series", series).
				Uint64("series_id", uint64(seriesID)).
				Msg("got a series with a full path")
		}
		return []Series{newSeries(ctx, seriesID, series, s)}, nil
	}
	result := make([]Series, 0)
	var err error
	errScan := s.seriesMetadata.Scan(prefix, prepend(path.seekKey, entityPrefix), kv.DefaultScanOpts, func(key []byte, getVal func() ([]byte, error)) error {
		key = key[entityPrefixLen:]
		comparableKey := make([]byte, len(key))
		for i, b := range key {
			comparableKey[i] = path.mask[i] & b
		}
		if bytes.Equal(path.template, comparableKey) {
			data, errGetVal := getVal()
			if errGetVal != nil {
				err = multierr.Append(err, errGetVal)
				return nil
			}
			seriesID, entityValue, errDecode := decode(data)
			if errDecode != nil {
				err = multierr.Append(err, errDecode)
				return nil
			}
			series := entityValue.String()
			if e := l.Debug(); e.Enabled() {
				e.Int("prefix_len", path.offset/8).
					Str("series", series).
					Uint64("series_id", uint64(seriesID)).
					Msg("match a series")
			}
			result = append(result, newSeries(s.context(), seriesID, series, s))
		}
		return nil
	})
	if errScan != nil {
		return nil, errScan
	}
	return result, err
}

func (s *seriesDB) span(ctx context.Context, timeRange timestamp.TimeRange) ([]blockDelegate, error) {
	result := make([]blockDelegate, 0)
	for _, s := range s.segCtrl.span(timeRange) {
		dd, err := s.blockController.span(ctx, timeRange)
		if err != nil {
			return nil, err
		}
		if dd == nil {
			continue
		}
		result = append(result, dd...)
	}
	return result, nil
}

func (s *seriesDB) create(ctx context.Context, ts time.Time) (blockDelegate, error) {
	s.Lock()
	defer s.Unlock()
	timeRange := timestamp.NewInclusiveTimeRange(ts, ts)
	ss := s.segCtrl.span(timeRange)
	if len(ss) > 0 {
		s := ss[0]
		dd, err := s.blockController.span(ctx, timeRange)
		if err != nil {
			return nil, err
		}
		if len(dd) > 0 {
			return dd[0], nil
		}
		block, err := s.blockController.create(timeRange.Start)
		if err != nil {
			return nil, err
		}
		return block.delegate(ctx)
	}
	seg, err := s.segCtrl.create(timeRange.Start)
	if err != nil {
		return nil, err
	}
	block, err := seg.blockController.create(timeRange.Start)
	if err != nil {
		return nil, err
	}
	return block.delegate(ctx)
}

func (s *seriesDB) context() context.Context {
	return context.WithValue(context.Background(), logger.ContextKey, s.l)
}

func (s *seriesDB) Stats() observability.Statistics {
	return s.seriesMetadata.Stats()
}

func (s *seriesDB) Close() error {
	return s.seriesMetadata.Close()
}

func newSeriesDataBase(ctx context.Context, shardID common.ShardID, path string, segCtrl *segmentController) (SeriesDatabase, error) {
	sdb := &seriesDB{
		sID:     shardID,
		segCtrl: segCtrl,
		l:       logger.Fetch(ctx, "series_database"),
	}
	o := ctx.Value(optionsKey)
	var memSize int64
	if o != nil {
		options := o.(DatabaseOpts)
		if options.SeriesMemSize > 1 {
			memSize = options.SeriesMemSize
		} else {
			memSize = defaultKVMemorySize
		}
	} else {
		memSize = defaultKVMemorySize
	}
	var err error
	sdb.seriesMetadata, err = kv.OpenStore(path+"/md",
		kv.StoreWithNamedLogger("metadata", sdb.l),
		kv.StoreWithMemTableSize(memSize),
	)
	if err != nil {
		return nil, err
	}
	return sdb, nil
}

// HashEntity runs hash function (e.g. with xxhash algorithm) on each segment of the Entity,
// and concatenates all uint64 in byte array. So the return length of the byte array will be
// 8 (every uint64 has 8 bytes) * length of the input.
func HashEntity(entity Entity) []byte {
	result := make([]byte, 0, len(entity)*8)
	for _, entry := range entity {
		result = append(result, Hash(entry)...)
	}
	return result
}

// SeriesID transforms Entity to common.SeriesID.
func SeriesID(entity Entity) common.SeriesID {
	return common.SeriesID(convert.Hash(HashEntity(entity)))
}

// Hash encode Entry to 8 bytes.
func Hash(entry []byte) []byte {
	return convert.Uint64ToBytes(convert.Hash(entry))
}

func bytesToSeriesID(data []byte) common.SeriesID {
	return common.SeriesID(convert.BytesToUint64(data))
}

// SeriesList is a collection of Series.
type SeriesList []Series

func (a SeriesList) Len() int {
	return len(a)
}

func (a SeriesList) Less(i, j int) bool {
	return a[i].ID() < a[j].ID()
}

func (a SeriesList) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// Merge other SeriesList with this one to create a new SeriesList.
func (a SeriesList) Merge(other SeriesList) SeriesList {
	if len(other) == 0 {
		return a
	}
	sort.Sort(other)
	if len(a) == 0 {
		return other
	}
	final := SeriesList{}
	i := 0
	j := 0
	for i < len(a) && j < len(other) {
		if a[i].ID() < other[j].ID() {
			final = append(final, a[i])
			i++
		} else {
			// deduplication
			if a[i].ID() == other[j].ID() {
				i++
			}
			final = append(final, other[j])
			j++
		}
	}
	for ; i < len(a); i++ {
		final = append(final, a[i])
	}
	for ; j < len(other); j++ {
		final = append(final, other[j])
	}
	return final
}
