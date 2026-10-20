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

package vectorized

// SelectRows narrows b's selection to the active rows for which keep reports true,
// preserving their order. keep sees the column and a row of it.
//
// A predicate on a ConstColumn has the same answer for every row, so it is evaluated
// once: a batch it rejects gets an empty selection and a batch it accepts is left
// untouched, whatever its size. Any other column is evaluated row by row.
func SelectRows(b *RecordBatch, colIdx int, keep func(col Column, row int) bool) {
	col := b.Columns[colIdx]
	if _, isConst := col.(*ConstColumn); isConst {
		if b.ActiveLen() == 0 || keep(col, 0) {
			return
		}
		b.Selection = []uint16{}
		return
	}
	if b.Selection == nil {
		out := make([]uint16, 0, b.Len)
		for row := range b.Len {
			if keep(col, row) {
				out = append(out, uint16(row))
			}
		}
		b.Selection = out
		return
	}
	out := make([]uint16, 0, len(b.Selection))
	for _, row := range b.Selection {
		if keep(col, int(row)) {
			out = append(out, row)
		}
	}
	b.Selection = out
}
