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
	"bytes"
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
	ruleToTag map[uint32]IndexedTagSpec
}

// NewIndexResolver opens the segment's series index under segmentPath/sidx for
// read-only lookups. ruleToTag is optional: when the schema is known it lets
// DecodeTagValues decode values by tag name, otherwise pass nil and use Resolve.
// cacheSize bounds the frequency-aware cache; values <= 0 fall back to
// DefaultIndexCacheSize. Call Close when done.
func NewIndexResolver(segmentPath string, cacheSize int,
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
		ruleToTag: ruleToTag,
	}, nil
}

// Resolve returns the indexed tag values stored for the series identified by
// seriesID, whose EntityValues the caller already knows (the index document is
// keyed by EntityValues). Each rule id maps to one or more raw value byte slices
// (one for a scalar tag, several for an array tag). It returns nil when
// entityValues is empty or the series has no indexed tags. Results are cached
// per seriesID, so a series spanning several blocks or parts resolves once.
func (r *IndexResolver) Resolve(seriesID common.SeriesID, entityValues []byte) (map[uint32][][]byte, error) {
	if v, ok := r.cache.Get(seriesID); ok {
		return v.(map[uint32][][]byte), nil
	}
	if len(entityValues) == 0 {
		return nil, nil
	}
	raw, err := r.store.StoredFields(context.Background(), entityValues)
	if err != nil {
		return nil, err
	}
	result := parseIndexedFields(raw)
	r.cache.Add(seriesID, result)
	return result, nil
}

// PartSeriesMap builds a SeriesID -> EntityValues map scoped to the given set of
// series by scanning the series index once and keeping only the entries whose
// SeriesID (Hash of EntityValues) is in seriesIDs. It is the fallback that
// recovers EntityValues for a part with no part-level series metadata, while
// keeping peak memory bounded by the part's distinct series count. It stops
// early once every requested series has been found.
func (r *IndexResolver) PartSeriesMap(seriesIDs map[common.SeriesID]struct{}) (map[common.SeriesID][]byte, error) {
	if len(seriesIDs) == 0 {
		return nil, nil
	}
	iter, err := r.store.SeriesIterator(context.Background())
	if err != nil {
		return nil, err
	}
	result := make(map[common.SeriesID][]byte, len(seriesIDs))
	for iter.Next() {
		entityValues := iter.Val().EntityValues
		if len(entityValues) == 0 {
			continue
		}
		seriesID := common.SeriesID(convert.Hash(entityValues))
		if _, ok := seriesIDs[seriesID]; !ok {
			continue
		}
		result[seriesID] = bytes.Clone(entityValues)
		if len(result) == len(seriesIDs) {
			break
		}
	}
	if closeErr := iter.Close(); closeErr != nil {
		return nil, closeErr
	}
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

// parseIndexedFields keys a document's stored fields by IndexRuleID. An
// index-rule field name is a marshaled uint32 IndexRuleID (exactly 4 bytes);
// any other-width field is skipped, so a non-rule name (e.g. an index-mode
// tag name) can neither panic BytesToUint32 nor map to a garbage rule id. A
// rule id maps to one value for a scalar tag, several for an array tag.
func parseIndexedFields(raw map[string][][]byte) map[uint32][][]byte {
	if len(raw) == 0 {
		return nil
	}
	result := make(map[uint32][][]byte, len(raw))
	for name, values := range raw {
		if len(name) != 4 {
			continue
		}
		result[convert.BytesToUint32([]byte(name))] = values
	}
	return result
}
