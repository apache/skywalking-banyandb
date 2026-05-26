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

package stream

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

// DumpTag describes one tag of a stream row for the dump test builder. Value is a
// Go-native value (string, int64, []string, []int64 or []byte) encoded through
// the production encodeTagValue path.
type DumpTag struct {
	Value  any
	Family string
	Name   string
}

// DumpRow is one stream element for BuildPartForDump. When Entity is set the
// SeriesID is derived as hash(Entity) and (optionally) written to smeta.bin.
type DumpRow struct {
	Entity    string
	Tags      []DumpTag
	SeriesID  common.SeriesID
	Timestamp int64
	ElementID uint64
}

// BuildPartForDump writes rows into a stream part at root/<partID>, returning the
// part directory, the rows (with resolved SeriesID) for verification and a
// cleanup func. When writeSeriesMeta is true a matching smeta.bin is written for
// every row that carries an Entity.
func BuildPartForDump(tmpPath string, fileSystem fs.FileSystem, partID uint64, rows []DumpRow) (string, []DumpRow, func()) {
	es := &elements{}
	for i := range rows {
		r := &rows[i]
		if r.Entity != "" {
			r.SeriesID = common.SeriesID(convert.Hash([]byte(r.Entity)))
		}
		es.seriesIDs = append(es.seriesIDs, r.SeriesID)
		es.timestamps = append(es.timestamps, r.Timestamp)
		es.elementIDs = append(es.elementIDs, r.ElementID)
		es.tagFamilies = append(es.tagFamilies, buildDumpTagFamilies(r.Tags))
	}

	mp := generateMemPart()
	mp.mustInitFromElements(es)
	path := partPath(tmpPath, partID)
	mp.mustFlush(fileSystem, path)

	return path, rows, func() {
		releaseMemPart(mp)
	}
}

// BuildEntityPartWithSeriesMeta builds an entity-keyed stream part (via
// EntityDumpRows) and writes a matching smeta.bin so the dump tool can resolve
// each row's EntityValues.
func BuildEntityPartWithSeriesMeta(tmpPath string, fileSystem fs.FileSystem, partID uint64, entities []string) (string, []DumpRow, func()) {
	path, rows, cleanup := BuildPartForDump(tmpPath, fileSystem, partID, EntityDumpRows(entities))
	docs := make(index.Documents, 0, len(entities))
	for i, entity := range entities {
		docs = append(docs, index.Document{DocID: uint64(i + 1), EntityValues: []byte(entity)})
	}
	seriesMetadataBytes, err := docs.Marshal()
	if err != nil {
		panic(fmt.Sprintf("failed to marshal series metadata documents: %v", err))
	}
	fs.MustFlush(fileSystem, seriesMetadataBytes, filepath.Join(path, "smeta.bin"), storage.FilePerm)
	return path, rows, cleanup
}

// StandardDumpRows returns the canonical stream fixture: three elements exercising
// string / int64 / string-array / int64-array tags across two tag families, with
// one element carrying no tags.
func StandardDumpRows() []DumpRow {
	now := time.Now().UnixNano()
	return []DumpRow{
		{
			SeriesID:  1,
			Timestamp: now,
			ElementID: 11,
			Tags: []DumpTag{
				{Family: "arrTag", Name: "strArrTag", Value: []string{"value1", "value2"}},
				{Family: "arrTag", Name: "intArrTag", Value: []int64{25, 30}},
				{Family: "singleTag", Name: "strTag", Value: "test-value"},
				{Family: "singleTag", Name: "intTag", Value: int64(100)},
			},
		},
		{
			SeriesID:  2,
			Timestamp: now + 1000,
			ElementID: 21,
			Tags: []DumpTag{
				{Family: "singleTag", Name: "strTag1", Value: "tag1"},
				{Family: "singleTag", Name: "strTag2", Value: "tag2"},
			},
		},
		{
			SeriesID:  3,
			Timestamp: now + 2000,
			ElementID: 31,
		},
	}
}

// EntityDumpRows returns one element per entity whose SeriesID is hash(entity),
// each carrying a single meta.name string tag. Pair with writeSeriesMeta=true to
// also emit smeta.bin.
func EntityDumpRows(entities []string) []DumpRow {
	base := time.Now().UnixNano()
	rows := make([]DumpRow, 0, len(entities))
	for i, entity := range entities {
		rows = append(rows, DumpRow{
			Entity:    entity,
			Timestamp: base + int64(i),
			ElementID: uint64(i + 1),
			Tags:      []DumpTag{{Family: "meta", Name: "name", Value: entity}},
		})
	}
	return rows
}

func buildDumpTagFamilies(tags []DumpTag) []tagValues {
	order := make([]string, 0)
	byFamily := make(map[string][]*tagValue)
	for _, tg := range tags {
		if _, ok := byFamily[tg.Family]; !ok {
			order = append(order, tg.Family)
		}
		byFamily[tg.Family] = append(byFamily[tg.Family], encodeDumpTag(tg.Name, tg.Value))
	}
	out := make([]tagValues, 0, len(order))
	for _, family := range order {
		out = append(out, tagValues{tag: family, values: byFamily[family]})
	}
	return out
}

func encodeDumpTag(name string, value any) *tagValue {
	switch v := value.(type) {
	case string:
		return encodeTagValue(name, databasev1.TagType_TAG_TYPE_STRING, &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}})
	case int64:
		return encodeTagValue(name, databasev1.TagType_TAG_TYPE_INT, &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: v}}})
	case []string:
		return encodeTagValue(name, databasev1.TagType_TAG_TYPE_STRING_ARRAY, &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: v}}})
	case []int64:
		return encodeTagValue(name, databasev1.TagType_TAG_TYPE_INT_ARRAY, &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: v}}})
	case []byte:
		return encodeTagValue(name, databasev1.TagType_TAG_TYPE_DATA_BINARY, &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: v}})
	default:
		panic(fmt.Sprintf("unsupported dump tag value type %T", value))
	}
}
