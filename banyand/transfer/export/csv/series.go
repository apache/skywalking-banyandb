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

package csv

import (
	"bytes"
	"context"
	"strings"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// Stored fields of a property document (banyand/property/db/shard.go buildUpdateDocument).
const (
	// propertySourceField holds the property as protojson.
	propertySourceField = "_source"
	// propertyDeleteField holds the delete time of a tombstone, an 8-byte timestamp.
	propertyDeleteField = "_deleted"
)

// IndexModeReader is implemented by the measure backend. Index-mode measures keep their
// rows only as documents of the segment-level series index; ExportIndexModeRows walks the
// rows of the given index-mode measures under sidxDir read-only and hands each to visit
// with its tags keyed "family.tag" (entity tags included) and the number of stored fields
// the current schema no longer declares. A missing or never-committed index is no rows.
// It lives behind an interface because banyand/measure tests import the export package.
type IndexModeReader interface {
	ExportIndexModeRows(ctx context.Context, sidxDir string, measures []*databasev1.Measure, rules []*databasev1.IndexRule,
		visit func(subject string, timestamp, version int64, tags map[string]*modelv1.TagValue, unknownFields int) error) error
}

// seriesDecoder decodes the EntityValues of a row (pbv1.Series: subject, then the entity
// tag values) and remembers the last one, since rows of a block share it.
type seriesDecoder struct {
	err     error
	subject string
	last    []byte
	values  []*modelv1.TagValue
}

func (d *seriesDecoder) decode(entityValues []byte) (string, []*modelv1.TagValue, error) {
	if d.last != nil && bytes.Equal(d.last, entityValues) {
		return d.subject, d.values, d.err
	}
	d.last = append(d.last[:0], entityValues...)
	var s pbv1.Series
	d.err = s.Unmarshal(entityValues)
	d.subject, d.values = s.Subject, s.EntityValues
	return d.subject, d.values, d.err
}

// subjectOf returns the resource of a row from its EntityValues, counting with run the rows
// whose resource cannot be told.
func subjectOf(run *unitRun, d *seriesDecoder, catalog string, entityValues []byte) (string, []*modelv1.TagValue, bool) {
	if len(entityValues) == 0 {
		run.dropRow(catalog + " rows without series metadata")
		return "", nil, false
	}
	subject, values, err := d.decode(entityValues)
	if err != nil || subject == "" {
		run.dropRow(catalog + " rows with undecodable series metadata")
		return "", nil, false
	}
	return subject, values, true
}

// typedTagSuffixes are the suffixes a stream, measure or trace merge appends, after '#', to a tag whose
// parts hold it with different types: each typed column keeps the values of one type.
var typedTagSuffixes = []string{"#str_arr", "#int_arr", "#str", "#int", "#float", "#bin", "#ts"}

// storedTagName is the tag a stored column holds: its name without a merge's type suffix.
func storedTagName(column string) string {
	if !strings.Contains(column, "#") {
		return column
	}
	for _, suffix := range typedTagSuffixes {
		if strings.HasSuffix(column, suffix) {
			return column[:len(column)-len(suffix)]
		}
	}
	return column
}
