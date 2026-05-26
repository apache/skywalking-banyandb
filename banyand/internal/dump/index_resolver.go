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

package dump

import (
	"context"
	"path/filepath"

	lru "github.com/hashicorp/golang-lru"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// DefaultIndexCacheSize is the default number of series whose resolved indexed
// tags an IndexResolver keeps in its frequency-aware cache.
const DefaultIndexCacheSize = 4096

// IndexedTagSpec describes an indexed tag so the resolver can decode its raw
// stored value into a typed TagValue keyed by name. Callers that know the schema
// pass a IndexRuleID -> IndexedTagSpec map to NewIndexResolver.
type IndexedTagSpec struct {
	Family string
	Name   string
	Type   pbv1.ValueType
}

// IndexResolver recovers a measure's indexed (non-entity) tag values, which the
// write path stores only in the segment-level series index (sidx) and never as
// data columns. Lookups are on-demand per series and held in a fixed-size,
// frequency-aware (2Q) cache, so the potentially large indexed values are never
// all held in memory at once.
//
// Resolve returns values keyed by IndexRuleID (no schema needed). When the
// caller supplies a IndexRuleID -> IndexedTagSpec mapping, DecodeTagValues
// additionally decodes a resolved map into typed TagValues keyed by "family.tag".
type IndexResolver struct {
	store     index.SeriesStore
	cache     *lru.TwoQueueCache
	seriesMap map[common.SeriesID]string
	ruleToTag map[uint32]IndexedTagSpec
}

// NewIndexResolver opens the segment's series index under segmentPath/sidx for
// read-only lookups. seriesMap maps SeriesID -> EntityValues (as produced by
// LoadSegmentSeriesMap); it is required because the index document is keyed by
// EntityValues, not by SeriesID. ruleToTag is optional: when the schema is known
// it lets DecodeTagValues decode values by tag name, otherwise pass nil and use
// Resolve. cacheSize bounds the frequency-aware cache; values <= 0 fall back to
// DefaultIndexCacheSize. Call Close when done.
func NewIndexResolver(segmentPath string, seriesMap map[common.SeriesID]string, cacheSize int,
	ruleToTag map[uint32]IndexedTagSpec,
) (*IndexResolver, error) {
	if cacheSize <= 0 {
		cacheSize = DefaultIndexCacheSize
	}
	cache, err := lru.New2Q(cacheSize)
	if err != nil {
		return nil, err
	}
	store, err := inverted.NewStore(inverted.StoreOpts{
		Path:   filepath.Join(segmentPath, dirNameSidx),
		Logger: logger.GetLogger(logName),
	})
	if err != nil {
		return nil, err
	}
	return &IndexResolver{
		store:     store,
		cache:     cache,
		seriesMap: seriesMap,
		ruleToTag: ruleToTag,
	}, nil
}

// Resolve returns the indexed tag values stored for the given series, keyed by
// IndexRuleID. Each rule id maps to one or more raw value byte slices (one for a
// scalar tag, several for an array tag). It returns nil when the series has no
// EntityValues mapping or no indexed tags. Results are cached per series.
func (r *IndexResolver) Resolve(seriesID common.SeriesID) (map[uint32][][]byte, error) {
	if v, ok := r.cache.Get(seriesID); ok {
		return v.(map[uint32][][]byte), nil
	}
	entityValues, ok := r.seriesMap[seriesID]
	if !ok || entityValues == "" {
		r.cache.Add(seriesID, map[uint32][][]byte(nil))
		return nil, nil
	}
	raw, err := r.store.StoredFields(context.Background(), []byte(entityValues))
	if err != nil {
		return nil, err
	}
	result := parseIndexedFields(raw)
	r.cache.Add(seriesID, result)
	return result, nil
}

// DecodeTagValues decodes already-resolved indexed tags (a Row's IndexedTags)
// into typed TagValues keyed by "family.tag" (or just "tag" when the family is
// empty), using the ruleToTag mapping given to NewIndexResolver. It does no I/O.
// It returns nil when that mapping is absent (schema unknown) or there are no
// indexed tags.
func (r *IndexResolver) DecodeTagValues(indexed map[uint32][][]byte) map[string]*modelv1.TagValue {
	if r.ruleToTag == nil || len(indexed) == 0 {
		return nil
	}
	out := make(map[string]*modelv1.TagValue, len(indexed))
	for ruleID, values := range indexed {
		spec, ok := r.ruleToTag[ruleID]
		if !ok {
			continue
		}
		key := spec.Name
		if spec.Family != "" {
			key = spec.Family + "." + spec.Name
		}
		var scalar []byte
		if len(values) > 0 {
			scalar = values[0]
		}
		// DecodeTagValue uses value for scalar types and valueArr for array
		// types; passing both lets the value type pick the right one.
		out[key] = DecodeTagValue(spec.Type, scalar, values)
	}
	return out
}

// Close releases the underlying series index store.
func (r *IndexResolver) Close() error {
	return r.store.Close()
}

// parseIndexedFields keys a document's stored fields by IndexRuleID. StoredFields
// already excludes the store's internal bookkeeping fields, so every remaining
// field name is an index-rule field name (FieldKey.Marshal() = the uint32
// IndexRuleID). A rule id maps to one value for a scalar tag, several for an
// array tag.
func parseIndexedFields(raw map[string][][]byte) map[uint32][][]byte {
	if len(raw) == 0 {
		return nil
	}
	result := make(map[uint32][][]byte, len(raw))
	for name, values := range raw {
		result[convert.BytesToUint32([]byte(name))] = values
	}
	return result
}
