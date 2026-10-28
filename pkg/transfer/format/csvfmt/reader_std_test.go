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

package csvfmt

import (
	"compress/gzip"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"testing"
)

// stdReadAll is the differential oracle of the field reader: it reads every record with
// encoding/csv, which materializes every cell, and decodes the cells with decodeCell. It
// follows the Reader contract: the prelude is checked by readPrelude and record errors
// carry the record number.
func stdReadAll(t *testing.T, r io.Reader, l *Layout, gz bool) ([][]Value, error) {
	t.Helper()
	if gz {
		zr, err := gzip.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("open gzip stream: %w", err)
		}
		defer zr.Close()
		r = zr
	}
	cr := csv.NewReader(r)
	// The #datatype line has one more cell than the header, so counts are checked here.
	cr.FieldsPerRecord = -1
	cr.ReuseRecord = true
	if err := readPrelude(cr.Read, l); err != nil {
		return nil, err
	}
	var rows [][]Value
	for {
		rec, err := cr.Read()
		if errors.Is(err, io.EOF) {
			return rows, nil
		}
		if err == nil {
			row := make([]Value, len(l.Columns))
			err = stdDecodeRecord(rec, row, l.Columns)
			if err == nil {
				rows = append(rows, row)
				continue
			}
		}
		return rows, fmt.Errorf("record %d: %w", len(rows)+1, err)
	}
}

func stdDecodeRecord(rec []string, dst []Value, cols []Column) error {
	if len(rec) != len(cols) {
		return fmt.Errorf("record has %d columns, the layout has %d", len(rec), len(cols))
	}
	for i := range cols {
		if err := decodeCell(rec[i], cols[i].Type, &dst[i]); err != nil {
			return columnError(&cols[i], err)
		}
	}
	return nil
}
