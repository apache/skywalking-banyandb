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
	"strings"

	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

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

// columnTypes maps a stored value type to its column type; an unknown stored type has none.
var columnTypes = map[pbv1.ValueType]csvfmt.ValueType{ //nolint:exhaustive // ValueTypeUnknown has no column type
	pbv1.ValueTypeStr:        csvfmt.ValueString,
	pbv1.ValueTypeInt64:      csvfmt.ValueInt,
	pbv1.ValueTypeFloat64:    csvfmt.ValueFloat,
	pbv1.ValueTypeBinaryData: csvfmt.ValueBinary,
	pbv1.ValueTypeStrArr:     csvfmt.ValueStringArray,
	pbv1.ValueTypeInt64Arr:   csvfmt.ValueIntArray,
	pbv1.ValueTypeTimestamp:  csvfmt.ValueTimestamp,
}

// setStoredTags fills cells with the stored tag columns of a row the way a query reads them:
// a column a merge renamed "<tag>#<type>" serves the tag, else the plain column does, and a
// value whose stored type is not the column's schema type is left null. columns maps a tag to
// its layout column; a stored value no column takes is counted as dropped for resource.
func setStoredTags(cells []string, layout *csvfmt.Layout, tags map[string][]byte, types map[string]pbv1.ValueType,
	columns map[string]int, run *unitRun, resource string,
) {
	for key, raw := range tags {
		if raw == nil {
			continue
		}
		tag := storedTagName(key)
		idx, ok := columns[tag]
		if !ok {
			run.dropTag(resource, key)
			continue
		}
		vt := types[key]
		want, known := columnTypes[vt]
		if !known {
			// A query reads a value of an unknown stored type as null.
			continue
		}
		if want != layout.Columns[idx].Type {
			run.dropTag(resource, key)
			continue
		}
		// A block holds either the plain column or the typed ones, so nothing else competes
		// for the cell.
		cells[idx] = csvfmt.EncodeTagValue(dump.DecodeTagValue(vt, raw, nil))
	}
}
