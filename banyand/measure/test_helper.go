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

package measure

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
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// DumpTag describes one tag of a measure row for the dump test builder. Value is
// a Go-native value (string, int64, []string, []int64 or []byte) encoded through
// the production encodeTagValue path.
type DumpTag struct {
	Value  any
	Family string
	Name   string
}

// DumpField describes one field of a measure row. Value is int64 or float64.
type DumpField struct {
	Value any
	Name  string
}

// DumpRow is one measure data point for BuildPartForDump. When Entity is set the
// SeriesID is derived as hash(Entity) and (optionally) written to smeta.bin.
type DumpRow struct {
	Entity    string
	Tags      []DumpTag
	Fields    []DumpField
	SeriesID  common.SeriesID
	Timestamp int64
	Version   int64
}

// BuildPartForDump writes rows into a measure part at root/<partID>, returning
// the part directory, the rows (with resolved SeriesID) for verification and a
// cleanup func. When writeSeriesMeta is true a matching smeta.bin is written for
// every row that carries an Entity.
func BuildPartForDump(tmpPath string, fileSystem fs.FileSystem, partID uint64, rows []DumpRow) (string, []DumpRow, func()) {
	dps := generateDataPoints()
	for i := range rows {
		r := &rows[i]
		if r.Entity != "" {
			r.SeriesID = common.SeriesID(convert.Hash([]byte(r.Entity)))
		}
		dps.seriesIDs = append(dps.seriesIDs, r.SeriesID)
		dps.timestamps = append(dps.timestamps, r.Timestamp)
		dps.versions = append(dps.versions, r.Version)
		dps.tagFamilies = append(dps.tagFamilies, buildDumpTagFamilies(r.Tags))
		dps.fields = append(dps.fields, buildDumpFields(r.Fields))
	}

	mp := generateMemPart()
	mp.mustInitFromDataPoints(dps)
	path := partPath(tmpPath, partID)
	mp.mustFlush(fileSystem, path)

	return path, rows, func() {
		releaseMemPart(mp)
		releaseDataPoints(dps)
	}
}

// BuildEntityPartWithSeriesMeta builds an entity-keyed measure part (via
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

// StandardDumpRows returns the canonical measure fixture: three series exercising
// string / int64 / string-array / int64-array tags across two tag families plus
// int64 and float64 fields, with one series carrying no tags.
func StandardDumpRows() []DumpRow {
	now := time.Now().UnixNano()
	return []DumpRow{
		{
			SeriesID:  1,
			Timestamp: now,
			Version:   1,
			Tags: []DumpTag{
				{Family: "arrTag", Name: "strArrTag", Value: []string{"value1", "value2"}},
				{Family: "arrTag", Name: "intArrTag", Value: []int64{25, 30}},
				{Family: "singleTag", Name: "strTag", Value: "test-value"},
				{Family: "singleTag", Name: "intTag", Value: int64(100)},
			},
			Fields: []DumpField{{Name: "intField", Value: int64(1000)}},
		},
		{
			SeriesID:  2,
			Timestamp: now + 1000,
			Version:   2,
			Tags: []DumpTag{
				{Family: "singleTag", Name: "strTag1", Value: "tag1"},
				{Family: "singleTag", Name: "strTag2", Value: "tag2"},
			},
			Fields: []DumpField{{Name: "floatField", Value: 3.14}},
		},
		{
			SeriesID:  3,
			Timestamp: now + 2000,
			Version:   3,
			Fields:    []DumpField{{Name: "intField", Value: int64(2000)}},
		},
	}
}

// EntityDumpRows returns one row per entity whose SeriesID is hash(entity), each
// carrying a single meta.name string tag and an int64 "value" field (the row
// index). Pair with writeSeriesMeta=true to also emit smeta.bin.
func EntityDumpRows(entities []string) []DumpRow {
	base := time.Now().UnixNano()
	rows := make([]DumpRow, 0, len(entities))
	for i, entity := range entities {
		rows = append(rows, DumpRow{
			Entity:    entity,
			Timestamp: base + int64(i),
			Version:   1,
			Tags:      []DumpTag{{Family: "meta", Name: "name", Value: entity}},
			Fields:    []DumpField{{Name: "value", Value: int64(i)}},
		})
	}
	return rows
}

func buildDumpTagFamilies(tags []DumpTag) []nameValues {
	order := make([]string, 0)
	byFamily := make(map[string][]*nameValue)
	for _, tg := range tags {
		if _, ok := byFamily[tg.Family]; !ok {
			order = append(order, tg.Family)
		}
		byFamily[tg.Family] = append(byFamily[tg.Family], encodeDumpTag(tg.Name, tg.Value))
	}
	out := make([]nameValues, 0, len(order))
	for _, family := range order {
		out = append(out, nameValues{name: family, values: byFamily[family]})
	}
	return out
}

func encodeDumpTag(name string, value any) *nameValue {
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

func buildDumpFields(fields []DumpField) nameValues {
	nv := nameValues{}
	if len(fields) > 0 {
		nv.name = fields[0].Name
	}
	for _, f := range fields {
		nv.values = append(nv.values, encodeDumpField(f.Name, f.Value))
	}
	return nv
}

func encodeDumpField(name string, value any) *nameValue {
	out := &nameValue{name: name}
	switch v := value.(type) {
	case int64:
		out.valueType = pbv1.ValueTypeInt64
		out.value = convert.Int64ToBytes(v)
	case float64:
		out.valueType = pbv1.ValueTypeFloat64
		out.value = convert.Float64ToBytes(v)
	default:
		panic(fmt.Sprintf("unsupported dump field value type %T", value))
	}
	return out
}
