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

package measure

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestMergeReusesColumnBuffersAcrossReset(t *testing.T) {
	var target blockPointer
	wide := mergeReuseLayout(8, 2, 2)
	target.copyFrom(wide)
	fieldBuffers := []*[]byte{&target.field.columns[0].values[0], &target.field.columns[1].values[0]}
	tagBuffers := [][]*[]byte{
		{&target.tagFamilies[0].columns[0].values[0], &target.tagFamilies[0].columns[1].values[0]},
		{&target.tagFamilies[1].columns[0].values[0], &target.tagFamilies[1].columns[1].values[0]},
	}
	for iteration := 0; iteration < 3; iteration++ {
		for _, shape := range []struct{ rows, families, columns int }{{3, 1, 1}, {5, 2, 2}} {
			source := mergeReuseLayout(shape.rows, shape.families, shape.columns)
			target.copyFrom(source)
			require.Equal(t, source.block, target.block)
			for index := range source.field.columns {
				require.Same(t, fieldBuffers[index], &target.field.columns[index].values[0], "field buffer %d", index)
				require.NotSame(t, &source.field.columns[index].values[0], &target.field.columns[index].values[0])
			}
			for family := range source.tagFamilies {
				for index := range source.tagFamilies[family].columns {
					require.Same(t, tagBuffers[family][index], &target.tagFamilies[family].columns[index].values[0],
						"tag buffer %d.%d", family, index)
				}
			}
			// Byte payloads may be borrowed, but the destination must own the row list.
			source.field.columns[0].values[0] = []byte("changed source row")
			require.Equal(t, []byte("field-0-0"), target.field.columns[0].values[0])
		}
	}
}

func mergeReuseLayout(rows, families, columns int) *blockPointer {
	b := &blockPointer{}
	for row := 0; row < rows; row++ {
		b.timestamps = append(b.timestamps, int64(row+1))
		b.versions = append(b.versions, 1)
	}
	for index := 0; index < columns; index++ {
		c := column{name: fmt.Sprintf("field-%d", index), valueType: pbv1.ValueTypeStr}
		for row := 0; row < rows; row++ {
			c.values = append(c.values, []byte(fmt.Sprintf("field-%d-%d", index, row)))
		}
		b.field.columns = append(b.field.columns, c)
	}
	for family := 0; family < families; family++ {
		cf := columnFamily{name: fmt.Sprintf("family-%d", family)}
		for index := 0; index < columns; index++ {
			c := column{name: fmt.Sprintf("tag-%d", index), valueType: pbv1.ValueTypeStr}
			for row := 0; row < rows; row++ {
				c.values = append(c.values, []byte(fmt.Sprintf("tag-%d-%d-%d", family, index, row)))
			}
			cf.columns = append(cf.columns, c)
		}
		b.tagFamilies = append(b.tagFamilies, cf)
	}
	b.updateMetadata()
	return b
}

func TestMergeTypedNumericColumnsOwnTheirRows(t *testing.T) {
	left := &blockPointer{block: block{
		timestamps: []int64{1, 3}, versions: []int64{1, 1},
		field: columnFamily{columns: []column{
			{name: "integer", valueType: pbv1.ValueTypeInt64, form: numericFormInt64, int64s: []int64{0, 30}},
			{name: "float", valueType: pbv1.ValueTypeFloat64, form: numericFormFloat64, float64s: []float64{0, 3.25}},
		}},
	}}
	right := &blockPointer{block: block{
		timestamps: []int64{2, 3}, versions: []int64{1, 2},
		field: columnFamily{columns: []column{
			{name: "integer", valueType: pbv1.ValueTypeInt64, form: numericFormInt64, int64s: []int64{-20, 31}},
			{name: "float", valueType: pbv1.ValueTypeFloat64, form: numericFormFloat64, float64s: []float64{-2.5, 3.5}},
		}},
	}}
	left.updateMetadata()
	right.updateMetadata()
	var target blockPointer
	mergeTwoBlocks(&target, left, right)
	require.Equal(t, []int64{1, 2, 3}, target.timestamps)
	require.Equal(t, []int64{1, 1, 2}, target.versions)
	require.Equal(t, numericFormInt64, target.field.columns[0].form)
	require.Equal(t, numericFormFloat64, target.field.columns[1].form)
	left.field.columns[0].int64s[0] = 999
	right.field.columns[0].int64s[1] = 999
	left.field.columns[1].float64s[0] = 999
	right.field.columns[1].float64s[1] = 999
	require.Equal(t, []int64{0, -20, 31}, target.field.columns[0].int64s)
	require.Equal(t, []float64{0, -2.5, 3.5}, target.field.columns[1].float64s)

	integerBuffer := &target.field.columns[0].int64s[0]
	floatBuffer := &target.field.columns[1].float64s[0]
	left.idx = 0
	target.copyFrom(left)
	require.Same(t, integerBuffer, &target.field.columns[0].int64s[0])
	require.Same(t, floatBuffer, &target.field.columns[1].float64s[0])
	require.Equal(t, left.field.columns[0].int64s, target.field.columns[0].int64s)
	require.Equal(t, left.field.columns[1].float64s, target.field.columns[1].float64s)
}

func TestMergeAppendChangedLayoutDoesNotDuplicateLeadingField(t *testing.T) {
	left := &blockPointer{block: block{
		timestamps: []int64{1}, versions: []int64{1},
		field: columnFamily{columns: []column{
			{name: "common", valueType: pbv1.ValueTypeInt64, values: [][]byte{convert.Int64ToBytes(10)}},
			{name: "old", valueType: pbv1.ValueTypeStr, values: [][]byte{[]byte("old value")}},
		}},
	}}
	right := &blockPointer{block: block{
		timestamps: []int64{2}, versions: []int64{2},
		field: columnFamily{columns: []column{
			{name: "common", valueType: pbv1.ValueTypeInt64, values: [][]byte{convert.Int64ToBytes(20)}},
			{name: "new", valueType: pbv1.ValueTypeFloat64, values: [][]byte{convert.Float64ToBytes(2.5)}},
		}},
	}}
	var target blockPointer
	target.copyFrom(left)
	target.appendAll(right)
	require.Equal(t, []int64{1, 2}, target.timestamps)
	require.Len(t, target.field.columns, 3)
	require.Equal(t, [][]byte{convert.Int64ToBytes(10), convert.Int64ToBytes(20)}, target.field.columns[0].values)
	require.Equal(t, [][]byte{[]byte("old value"), nil}, target.field.columns[1].values)
	require.Equal(t, [][]byte{nil, convert.Float64ToBytes(2.5)}, target.field.columns[2].values)
}

func TestMergeTagAppendRollbackKeepsNumericRows(t *testing.T) {
	for _, byteSource := range []bool{false, true} {
		t.Run(fmt.Sprintf("byte_source_%t", byteSource), func(t *testing.T) {
			target := &blockPointer{block: block{
				timestamps: []int64{1}, versions: []int64{1},
				tagFamilies: []columnFamily{{name: "tags", columns: []column{
					{name: "common", valueType: pbv1.ValueTypeInt64, form: numericFormInt64, int64s: []int64{10}},
					{name: "old", valueType: pbv1.ValueTypeStr, values: [][]byte{[]byte("old value")}},
				}}},
			}}
			source := &blockPointer{block: block{
				timestamps: []int64{2}, versions: []int64{2},
				tagFamilies: []columnFamily{{name: "tags", columns: []column{
					{name: "common", valueType: pbv1.ValueTypeInt64, form: numericFormInt64, int64s: []int64{20}},
					{name: "new", valueType: pbv1.ValueTypeStr, values: [][]byte{[]byte("new value")}},
				}}},
			}}
			if byteSource {
				source.tagFamilies[0].columns[0] = column{
					name: "common", valueType: pbv1.ValueTypeInt64, values: [][]byte{convert.Int64ToBytes(20)},
				}
			}
			err := fastTagAppend(target, source, 1)
			require.Error(t, err, "the changed second tag must reject the fast append")
			common := &target.tagFamilies[0].columns[0]
			require.Equal(t, 1, common.rowCount())
			require.Equal(t, convert.Int64ToBytes(10), common.rowBytes(0))
			if byteSource {
				require.Equal(t, numericFormBytes, common.form)
				require.Nil(t, common.values[:cap(common.values)][1], "rollback must clear the discarded byte reference")
			} else {
				require.Equal(t, numericFormInt64, common.form)
			}
			fullTagAppend(target, source, 1)
			columns := target.tagFamilies[0].columns
			require.Len(t, columns, 3)
			require.Equal(t, 2, columns[0].rowCount())
			require.Equal(t, convert.Int64ToBytes(10), columns[0].rowBytes(0))
			require.Equal(t, convert.Int64ToBytes(20), columns[0].rowBytes(1))
			require.Equal(t, [][]byte{[]byte("old value"), nil}, columns[1].values)
			require.Equal(t, [][]byte{nil, []byte("new value")}, columns[2].values)
		})
	}
}

func TestReleaseMergeBlockBoundsRetainedColumnBuffers(t *testing.T) {
	largeColumn := func() column {
		return column{
			values:   make([][]byte, 1, 2*maxBlockLength+1),
			int64s:   make([]int64, 0, 2*maxBlockLength+1),
			float64s: make([]float64, 0, 2*maxBlockLength+1),
		}
	}
	b := generateBlockPointer()
	b.reset()
	b.field.columns = []column{largeColumn(), largeColumn(), {
		values: make([][]byte, 0, 2*maxBlockLength),
	}}
	b.tagFamilies = []columnFamily{
		{columns: []column{largeColumn(), largeColumn()}},
		{columns: []column{largeColumn()}},
	}
	// Leave inactive columns and a family behind, as a narrower layout would.
	b.reset()
	b.field.columns = b.field.columns[:1]
	b.tagFamilies = b.tagFamilies[:1]
	b.tagFamilies[0].columns = b.tagFamilies[0].columns[:1]
	fields := b.field.columns[:cap(b.field.columns)]
	families := b.tagFamilies[:cap(b.tagFamilies)]
	releaseBlockPointer(b)
	for _, c := range fields[:2] {
		require.Zero(t, cap(c.values))
		require.Zero(t, cap(c.int64s))
		require.Zero(t, cap(c.float64s))
	}
	require.Equal(t, 2*maxBlockLength, cap(fields[2].values), "normal working capacity should be retained")
	for _, family := range families {
		for _, c := range family.columns[:cap(family.columns)] {
			require.Zero(t, cap(c.values))
			require.Zero(t, cap(c.int64s))
			require.Zero(t, cap(c.float64s))
		}
	}
}

func TestReleaseMergeIteratorBoundsNumericBuffers(t *testing.T) {
	iterator := generatePartMergeIter()
	iterator.block.field.columns = []column{{
		form: numericFormInt64, int64s: make([]int64, 1, 2*maxBlockLength+1),
	}}
	iterator.block.tagFamilies = []columnFamily{{columns: []column{{
		form: numericFormFloat64, float64s: make([]float64, 1, 2*maxBlockLength+1),
	}}}}
	fields := iterator.block.field.columns
	tags := iterator.block.tagFamilies[0].columns
	releasePartMergeIter(iterator)
	require.Zero(t, cap(fields[0].int64s))
	require.Zero(t, cap(tags[0].float64s))
}

func TestMergeNumericColumnsThroughFileParts(t *testing.T) {
	for _, rows := range []int{32, maxBlockLength/2 + 1} {
		t.Run(fmt.Sprintf("%d_rows_per_input", rows), func(t *testing.T) {
			root := t.TempDir()
			fileSystem := fs.NewLocalFileSystem()
			var inputs []*partWrapper
			defer func() {
				for _, input := range inputs {
					input.decRef()
				}
			}()
			for source := 0; source < 3; source++ {
				partID := uint64(source + 1)
				writer := generateBlockWriter()
				writer.mustInitForFilePart(fileSystem, partPath(root, partID), true)
				data := mergeReuseNumericBlock(rows, source)
				writer.mustWriteBlock(1, &data)
				metadata := &partMetadata{}
				types := make(tagType)
				writer.Flush(metadata, &types)
				releaseBlockWriter(writer)
				types.mustWriteTagType(fileSystem, partPath(root, partID))
				metadata.mustWriteMetadata(fileSystem, partPath(root, partID))
				inputs = append(inputs, newPartWrapper(nil, mustOpenFilePart(partID, root, fileSystem)))
			}
			table := &tsTable{pm: protector.Nop{}}
			merged, err := table.mergeParts(fileSystem, make(chan struct{}), inputs, 4, root)
			require.NoError(t, err)
			merged.decRef()
			output := mustOpenFilePart(4, root, fileSystem)
			defer output.close()
			require.Equal(t, uint64(2*rows), output.partMetadata.TotalCount)
			require.Equal(t, uint64((2*rows+maxBlockLength-1)/maxBlockLength), output.partMetadata.BlocksCount,
				"small inputs must coalesce and large output must split")
			iterator := generatePartMergeIter()
			defer releasePartMergeIter(iterator)
			iterator.mustInitFromPart(output)
			decoder := generateColumnValuesDecoder()
			defer releaseColumnValuesDecoder(decoder)
			count := 0
			for iterator.nextBlockMetadata() {
				iterator.mustLoadBlockData(decoder, &iterator.block)
				b := &iterator.block
				require.LessOrEqual(t, len(b.timestamps), maxBlockLength)
				require.Equal(t, uint64(len(b.timestamps)), b.bm.count)
				require.Equal(t, b.uncompressedSizeBytes(), b.bm.uncompressedSizeBytes)
				var fieldNames, familyNames []string
				for _, c := range b.field.columns {
					fieldNames = append(fieldNames, c.name)
				}
				require.ElementsMatch(t, []string{"integer", "float", "nullable_integer", "nullable_float", "optional"}, fieldNames)
				for _, family := range b.tagFamilies {
					familyNames = append(familyNames, family.name)
					var tagNames []string
					for _, c := range family.columns {
						tagNames = append(tagNames, c.name)
					}
					if family.name == "numeric" {
						require.ElementsMatch(t, []string{"integer", "float", "nullable_integer", "nullable_float", "label"}, tagNames)
					} else {
						require.Equal(t, []string{"optional"}, tagNames)
					}
				}
				require.ElementsMatch(t, []string{"numeric", "optional"}, familyNames)
				for row, timestamp := range b.timestamps {
					count++
					require.Equal(t, int64(count), timestamp)
					source := 2
					if timestamp%2 == 0 {
						source = 1
					}
					require.Equal(t, int64(source+1), b.versions[row])
					for _, c := range b.field.columns {
						require.Equal(t, numericFormBytes, c.form, "persisted output must support the legacy reader")
						require.Equal(t, mergeReuseNumericValue(c.name, timestamp, source), c.values[row],
							"field %s at timestamp %d", c.name, timestamp)
					}
					for _, family := range b.tagFamilies {
						for _, c := range family.columns {
							require.Equal(t, numericFormBytes, c.form)
							require.Equal(t, mergeReuseNumericValue(c.name, timestamp, source), c.values[row],
								"tag %s.%s at timestamp %d", family.name, c.name, timestamp)
						}
					}
				}
				decoder.Reset()
			}
			require.NoError(t, iterator.error())
			require.Equal(t, 2*rows, count)
		})
	}
}

func mergeReuseNumericBlock(rows, source int) block {
	b := block{field: columnFamily{columns: []column{
		{name: "integer", valueType: pbv1.ValueTypeInt64},
		{name: "float", valueType: pbv1.ValueTypeFloat64},
		{name: "nullable_integer", valueType: pbv1.ValueTypeInt64},
		{name: "nullable_float", valueType: pbv1.ValueTypeFloat64},
	}}, tagFamilies: []columnFamily{{name: "numeric", columns: []column{
		{name: "integer", valueType: pbv1.ValueTypeInt64},
		{name: "float", valueType: pbv1.ValueTypeFloat64},
		{name: "nullable_integer", valueType: pbv1.ValueTypeInt64},
		{name: "nullable_float", valueType: pbv1.ValueTypeFloat64},
		{name: "label", valueType: pbv1.ValueTypeStr},
	}}}}
	if source == 1 {
		b.field.columns = append(b.field.columns, column{name: "optional", valueType: pbv1.ValueTypeInt64})
		b.tagFamilies = append(b.tagFamilies, columnFamily{name: "optional", columns: []column{
			{name: "optional", valueType: pbv1.ValueTypeInt64},
		}})
	}
	for row := 0; row < rows; row++ {
		timestamp := int64(2*row + 1 + source%2)
		b.timestamps = append(b.timestamps, timestamp)
		b.versions = append(b.versions, int64(source+1))
		for index := range b.field.columns {
			c := &b.field.columns[index]
			c.values = append(c.values, mergeReuseNumericValue(c.name, timestamp, source))
		}
		for family := range b.tagFamilies {
			for index := range b.tagFamilies[family].columns {
				c := &b.tagFamilies[family].columns[index]
				c.values = append(c.values, mergeReuseNumericValue(c.name, timestamp, source))
			}
		}
	}
	return b
}

func mergeReuseNumericValue(name string, timestamp int64, source int) []byte {
	row := (timestamp - 1) / 2
	switch name {
	case "nullable_integer", "nullable_float":
		if source == 1 {
			switch row % 3 {
			case 0:
				return nil
			case 1:
				return []byte("null")
			}
		}
	case "optional":
		if source != 1 {
			return nil
		}
	case "label":
		return []byte(fmt.Sprintf("row-%d-version-%d", timestamp, source+1))
	}
	if name == "float" || name == "nullable_float" {
		return convert.Float64ToBytes(float64(row*int64(source+1)) / 4)
	}
	return convert.Int64ToBytes(row * int64(source+1))
}
