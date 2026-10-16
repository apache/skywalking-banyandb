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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package ql

import (
	"context"
	"math"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type expressionVector struct {
	column       vectorized.Column
	ints         []int64
	floats       []float64
	bools        []bool
	strings      []string
	bytes        [][]byte
	intArrays    [][]int64
	stringArrays [][]string
	nulls        []bool
	literal      ExpressionScalar
	typ          ExpressionType
	constant     bool
}

func (v *expressionVector) isNull(row int) bool {
	if v.constant {
		return v.literal.Null || v.typ == ExpressionNull
	}
	if v.column != nil {
		return v.column.IsNull(row)
	}
	return v.nulls[row]
}

func (v *expressionVector) intAt(row int) int64 {
	if v.constant {
		return v.literal.Int64
	}
	return v.ints[row]
}

func (v *expressionVector) floatAt(row int) float64 {
	if v.constant {
		return v.literal.Float64
	}
	return v.floats[row]
}

func (v *expressionVector) boolAt(row int) bool {
	if v.constant {
		return v.literal.Bool
	}
	return v.bools[row]
}

func (v *expressionVector) stringAt(row int) string {
	if v.constant {
		return v.literal.String
	}
	return v.strings[row]
}

type expressionNode struct {
	eval        func(context.Context, *vectorized.RecordBatch, []uint16) (*expressionVector, error)
	args        []*expressionNode
	value       expressionVector
	op          *expressionOperator
	mask        []uint16
	matched     []uint16
	scalarValue expressionVector
	bufferValue expressionVector
	index       int
	capacity    int
	scalarReady bool
}

type expressionOperator struct {
	source       vectorized.PullOperator
	tracker      *vectorized.MemoryTracker
	schema       *vectorized.BatchSchema
	nodes        []*expressionNode
	outputs      []*expressionNode
	metadata     []int
	view         vectorized.RecordBatch
	reserved     int64
	payloadBytes int64
	stats        ExpressionStats
	started      bool
	closed       bool
}

// WrapExpressions owns source on every return path and lends projected batches until the next pull.
func WrapExpressions(ctx context.Context, source vectorized.PullOperator, plan *ExpressionPlan) (_ vectorized.PullOperator, err error) {
	defer func() {
		if err != nil && source != nil {
			_ = source.Close()
		}
	}()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if source == nil || source.OutputSchema() == nil || plan == nil {
		return nil, expressionError("source schema and plan are required")
	}
	op := &expressionOperator{source: source, tracker: vectorized.QueryMemoryTracker(ctx, 256<<20)}
	defer func() {
		if err != nil {
			op.release()
		}
	}()
	if err = op.reserve(2048 + int64(len(plan.Outputs)+len(source.OutputSchema().Columns))*1024); err != nil {
		return nil, err
	}
	defs := make([]vectorized.ColumnDef, 0, len(plan.Outputs)+len(source.OutputSchema().Columns))
	for _, output := range plan.Outputs {
		bytes, boundErr := expressionCloneBound(output.Expr, 0)
		if boundErr != nil {
			return nil, boundErr
		}
		if reserveErr := op.reserve(bytes); reserveErr != nil {
			return nil, reserveErr
		}
		resolved, resolveErr := ResolveExpression(output.Expr)
		if resolveErr != nil {
			return nil, resolveErr
		}
		if resolved.Type == ExpressionBool {
			return nil, expressionError("projected BOOLEAN requires an explicit cast")
		}
		node, nodeErr := op.compile(resolved)
		if nodeErr != nil {
			return nil, nodeErr
		}
		op.outputs = append(op.outputs, node)
		def := output.Column
		def.Type = expressionStorageType(resolved.Type)
		defs = append(defs, def)
	}
	for index, def := range source.OutputSchema().Columns {
		if def.Role != vectorized.RoleField && def.Role != vectorized.RoleTag {
			op.metadata = append(op.metadata, index)
			defs = append(defs, def)
		}
	}
	op.schema = vectorized.NewBatchSchema(defs)
	op.view = vectorized.RecordBatch{Schema: op.schema, Columns: make([]vectorized.Column, len(defs)), Lent: true}
	return op, nil
}

func expressionCloneBound(expr *Expression, depth int) (int64, error) {
	if expr == nil || depth > 256 {
		return 0, expressionError("expression nesting exceeds 256")
	}
	bytes := int64(1024)
	for _, part := range []struct{ count, width int64 }{
		{int64(len(expr.Args)), 32},
		{int64(len(expr.Literal.Bytes)), 1},
		{int64(len(expr.Literal.Ints)), 8},
		{int64(len(expr.Literal.Strings)), 16},
	} {
		if part.count > (math.MaxInt64-bytes)/part.width {
			return 0, expressionError("expression size overflows")
		}
		bytes += part.count * part.width
	}
	for _, arg := range expr.Args {
		child, err := expressionCloneBound(arg, depth+1)
		if err != nil {
			return 0, err
		}
		if child > math.MaxInt64-bytes {
			return 0, expressionError("expression size overflows")
		}
		bytes += child
	}
	return bytes, nil
}

func expressionColumnType(def vectorized.ColumnDef) ExpressionType {
	if def.Role == vectorized.RoleTimestamp {
		return ExpressionTimestamp
	}
	switch def.Type {
	case vectorized.ColumnTypeInt64:
		return ExpressionInt
	case vectorized.ColumnTypeFloat64:
		return ExpressionFloat
	case vectorized.ColumnTypeString:
		return ExpressionString
	case vectorized.ColumnTypeBytes:
		return ExpressionBytes
	case vectorized.ColumnTypeInt64Array:
		return ExpressionIntArray
	case vectorized.ColumnTypeStrArray:
		return ExpressionStringArray
	case vectorized.ColumnTypeTagValue, vectorized.ColumnTypeFieldValue:
		return ExpressionNull
	}
	return ExpressionNull
}

func expressionStorageType(typ ExpressionType) vectorized.ColumnType {
	switch typ {
	case ExpressionFloat:
		return vectorized.ColumnTypeFloat64
	case ExpressionString:
		return vectorized.ColumnTypeString
	case ExpressionBytes:
		return vectorized.ColumnTypeBytes
	case ExpressionIntArray:
		return vectorized.ColumnTypeInt64Array
	case ExpressionStringArray:
		return vectorized.ColumnTypeStrArray
	default:
		return vectorized.ColumnTypeInt64
	}
}

func (o *expressionOperator) reserve(bytes int64) error {
	if err := o.tracker.Reserve(bytes); err != nil {
		return err
	}
	o.reserved += bytes
	o.stats.ReservedBytes += uint64(bytes)
	o.stats.LiveBytes += bytes
	recordExpressionReserve(bytes)
	return nil
}

func (o *expressionOperator) unreserve(bytes int64) {
	o.tracker.Release(bytes)
	o.reserved -= bytes
	o.stats.ReleasedBytes += uint64(bytes)
	o.stats.LiveBytes -= bytes
	recordExpressionRelease(bytes)
}

func (o *expressionOperator) compile(expr *Expression) (*expressionNode, error) {
	if err := o.reserve(1536 + int64(len(expr.Args))*16); err != nil {
		return nil, err
	}
	node := &expressionNode{op: o, value: expressionVector{typ: expr.Type}, index: -1}
	o.nodes = append(o.nodes, node)
	for _, arg := range expr.Args {
		child, err := o.compile(arg)
		if err != nil {
			return nil, err
		}
		node.args = append(node.args, child)
	}
	switch expr.Op {
	case "literal":
		node.scalarValue = expressionVector{typ: expr.Type, constant: true, literal: expr.Literal}
		node.eval = node.literal
	case "column":
		node.index = sourceIndex(o.source.OutputSchema(), expr.Source)
		if node.index < 0 {
			return nil, expressionError("source column %q is absent", expr.Source.Name)
		}
		actual := o.source.OutputSchema().Columns[node.index].Type
		if actual != expr.Source.Type && actual != vectorized.ColumnTypeTagValue && actual != vectorized.ColumnTypeFieldValue {
			return nil, expressionError("source column %q has an incompatible type", expr.Source.Name)
		}
		node.eval = node.column
	case "+", "-", "*", "/", "%":
		node.eval = node.arithmetic(expr.Op)
	case "cast":
		node.eval = node.cast(expr.Args[0].Type, expr.Type)
	case "negate", "positive":
		node.eval = node.unary(expr.Op)
	case "=", "!=", "<", "<=", ">", ">=":
		node.eval = node.comparison(expr.Op, expr.Args[0].Type)
	case "in", "not_in":
		node.eval = node.membership(expr.Op == "not_in", expr.Args[0].Type)
	case "and", "or":
		node.eval = node.boolean(expr.Op == "and")
	case "not", "is_null", "is_not_null":
		node.eval = node.unaryBoolean(expr.Op)
	case "case", "simple_case", "coalesce":
		node.eval = node.conditional(expr.Op)
	default:
		return nil, expressionError("unsupported expression %s", expr.Op)
	}
	if expr.Op != "literal" && expr.Op != "column" {
		evaluate := node.eval
		if expressionConstant(expr) {
			node.eval = func(ctx context.Context, _ *vectorized.RecordBatch, _ []uint16) (*expressionVector, error) {
				if node.scalarReady {
					return &node.scalarValue, nil
				}
				payloadBefore := o.payloadBytes
				value, evalErr := evaluate(ctx, &vectorized.RecordBatch{Len: 1}, nil)
				if evalErr != nil {
					return nil, evalErr
				}
				node.scalarValue = expressionVector{typ: expr.Type, constant: true, literal: value.scalarAt(0)}
				node.scalarReady = true
				o.payloadBytes = payloadBefore
				o.stats.ScalarEvaluations++
				recordExpressionWork(ExpressionStats{ScalarEvaluations: 1})
				return &node.scalarValue, nil
			}
		} else {
			node.eval = func(ctx context.Context, batch *vectorized.RecordBatch, mask []uint16) (*expressionVector, error) {
				if evalErr := ctx.Err(); evalErr != nil {
					return nil, evalErr
				}
				rows := uint64(expressionRows(batch, mask))
				o.stats.VectorRows += rows
				recordExpressionWork(ExpressionStats{VectorRows: rows})
				return evaluate(ctx, batch, mask)
			}
		}
	}
	return node, nil
}

func expressionConstant(expr *Expression) bool {
	if expr.Op == "column" {
		return false
	}
	for _, arg := range expr.Args {
		if !expressionConstant(arg) {
			return false
		}
	}
	return true
}

func (n *expressionNode) literal(_ context.Context, _ *vectorized.RecordBatch, _ []uint16) (*expressionVector, error) {
	return &n.scalarValue, nil
}

func (n *expressionNode) output(value *expressionVector, size int) (vectorized.Column, error) {
	if value.constant && !value.isNull(0) && (value.typ == ExpressionIntArray || value.typ == ExpressionStringArray) {
		literal := value.literal
		if err := n.buffer(size); err != nil {
			return nil, err
		}
		if value.typ == ExpressionIntArray {
			for row := 0; row < size; row++ {
				n.value.intArrays[row] = literal.Ints
			}
		} else {
			for row := 0; row < size; row++ {
				n.value.stringArrays[row] = literal.Strings
			}
		}
		return n.value.output(size), nil
	}
	return value.output(size), nil
}

func (n *expressionNode) column(ctx context.Context, batch *vectorized.RecordBatch, mask []uint16) (*expressionVector, error) {
	if n.index >= len(batch.Columns) || batch.Columns[n.index] == nil || batch.Columns[n.index].Len() < batch.Len {
		return nil, expressionError("source column does not cover batch")
	}
	c := batch.Columns[n.index]
	if c.Type() != expressionStorageType(n.value.typ) && c.Type() != vectorized.ColumnTypeTagValue && c.Type() != vectorized.ColumnTypeFieldValue {
		return nil, expressionError("input column type changed")
	}
	n.value.column = c
	n.value.constant = false
	if value, ok := vectorized.AsConst(c); ok {
		n.value.constant = true
		n.value.literal = ExpressionScalar{Int64: value.Int64(), Float64: value.Float64(), String: value.Str(), Bytes: value.Bytes(), Null: value.IsNullValue()}
		return &n.value, nil
	}
	switch value := c.(type) {
	case *vectorized.TypedColumn[int64]:
		n.value.ints = value.Data()
	case *vectorized.TypedColumn[float64]:
		n.value.floats = value.Data()
	case *vectorized.TypedColumn[string]:
		n.value.strings = value.Data()
	case *vectorized.TypedColumn[[]byte]:
		n.value.bytes = value.Data()
	case *vectorized.TypedColumn[[]int64]:
		n.value.intArrays = value.Data()
	case *vectorized.TypedColumn[[]string]:
		n.value.stringArrays = value.Data()
	case *vectorized.TypedColumn[*modelv1.TagValue]:
		if err := n.decodeTag(ctx, batch, mask, value.Data()); err != nil {
			return nil, err
		}
	case *vectorized.TypedColumn[*modelv1.FieldValue]:
		if err := n.decodeField(ctx, batch, mask, value.Data()); err != nil {
			return nil, err
		}
	default:
		return nil, expressionError("source column needs a typed vector")
	}
	return &n.value, nil
}

func (n *expressionNode) buffer(size int) error {
	if n.capacity >= size {
		n.value = n.bufferValue
		clear(n.value.nulls[:size])
		return nil
	}
	width := int64(8)
	switch n.value.typ { //nolint:exhaustive // Numeric types share the default int64 width.
	case ExpressionBool:
		width = 1
	case ExpressionString:
		width = 16
	case ExpressionBytes, ExpressionIntArray, ExpressionStringArray:
		width = 24
	}
	bytes := int64(size)*(width+2) + 256
	if err := n.op.reserve(bytes); err != nil {
		return err
	}
	old := int64(0)
	if n.capacity > 0 {
		old = int64(n.capacity)*(width+2) + 256
	}
	n.capacity = size
	n.op.stats.BufferBytes += uint64(bytes)
	if size > 1 {
		n.op.stats.FullColumnAllocations++
	}
	recordExpressionWork(ExpressionStats{BufferBytes: uint64(bytes), FullColumnAllocations: uint64(boolInt(size > 1))})
	n.value.nulls = make([]bool, size)
	switch n.value.typ {
	case ExpressionFloat:
		n.value.floats = make([]float64, size)
	case ExpressionBool:
		n.value.bools = make([]bool, size)
	case ExpressionString:
		n.value.strings = make([]string, size)
	case ExpressionBytes:
		n.value.bytes = make([][]byte, size)
	case ExpressionIntArray:
		n.value.intArrays = make([][]int64, size)
	case ExpressionStringArray:
		n.value.stringArrays = make([][]string, size)
	default:
		n.value.ints = make([]int64, size)
	}
	if old != 0 {
		n.op.unreserve(old)
	}
	n.value.column = nil
	n.value.constant = false
	n.bufferValue = n.value
	return nil
}

func expressionRow(mask []uint16, pos int) int {
	if mask != nil {
		return int(mask[pos])
	}
	return pos
}

func expressionRows(batch *vectorized.RecordBatch, mask []uint16) int {
	if mask != nil {
		return len(mask)
	}
	return batch.Len
}

func (v *expressionVector) output(size int) vectorized.Column {
	if v.constant {
		if v.literal.Null || v.typ == ExpressionNull {
			return vectorized.NewConstNull(expressionStorageType(v.typ), size)
		}
		switch v.typ { //nolint:exhaustive // Only scalar types supported by ConstColumn are handled here.
		case ExpressionInt, ExpressionTimestamp:
			return vectorized.NewConstInt64(v.literal.Int64, size)
		case ExpressionFloat:
			return vectorized.NewConstFloat64(v.literal.Float64, size)
		case ExpressionString:
			return vectorized.NewConstString(v.literal.String, size)
		case ExpressionBytes:
			return vectorized.NewConstBytes(v.literal.Bytes, size)
		}
	}
	if v.column != nil {
		return v.column
	}
	var output vectorized.Column
	switch v.typ {
	case ExpressionFloat:
		output = vectorized.NewColumnFromSlice(vectorized.ColumnTypeFloat64, v.floats[:size])
	case ExpressionString:
		output = vectorized.NewColumnFromSlice(vectorized.ColumnTypeString, v.strings[:size])
	case ExpressionBytes:
		output = vectorized.NewColumnFromSlice(vectorized.ColumnTypeBytes, v.bytes[:size])
	case ExpressionIntArray:
		output = vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64Array, v.intArrays[:size])
	case ExpressionStringArray:
		output = vectorized.NewColumnFromSlice(vectorized.ColumnTypeStrArray, v.stringArrays[:size])
	default:
		output = vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64, v.ints[:size])
	}
	for row, null := range v.nulls[:size] {
		if null {
			output.MarkNullAt(row)
		}
	}
	return output
}

func (o *expressionOperator) Init(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		_ = o.Close()
		return err
	}
	if err := o.source.Init(ctx); err != nil {
		_ = o.Close()
		return err
	}
	return nil
}
func (o *expressionOperator) OutputSchema() *vectorized.BatchSchema { return o.schema }
func (o *expressionOperator) release() {
	if o.reserved != 0 {
		o.unreserve(o.reserved)
	}
	o.nodes = nil
	o.outputs = nil
	o.view.Columns = nil
	o.view.Selection = nil
}

func (o *expressionOperator) Close() error {
	if o.closed {
		return nil
	}
	o.closed = true
	o.release()
	return o.source.Close()
}

func (o *expressionOperator) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	if err := ctx.Err(); err != nil {
		_ = o.Close()
		return nil, err
	}
	if o.closed {
		return nil, nil
	}
	clear(o.view.Columns)
	for _, node := range o.nodes {
		if node.capacity > 0 {
			clear(node.bufferValue.strings)
			clear(node.bufferValue.bytes)
			clear(node.bufferValue.intArrays)
			clear(node.bufferValue.stringArrays)
		}
	}
	if o.payloadBytes > 0 {
		o.unreserve(o.payloadBytes)
		o.payloadBytes = 0
	}
	for {
		batch, err := o.source.NextBatch(ctx)
		if err != nil {
			_ = o.Close()
			return nil, err
		}
		if batch == nil {
			return nil, nil
		}
		if batch.ActiveLen() == 0 {
			continue
		}
		if batch.Len < 0 || batch.Len > 65536 {
			_ = o.Close()
			return nil, expressionError("batch size exceeds selection range")
		}
		if err = ctx.Err(); err != nil {
			_ = o.Close()
			return nil, err
		}
		for _, entry := range batch.Selection {
			if int(entry) >= batch.Len {
				_ = o.Close()
				return nil, expressionError("selection index exceeds batch rows")
			}
		}
		if !o.started {
			o.started = true
			o.stats.Executions++
			recordExpressionWork(ExpressionStats{Executions: 1})
		}
		rows := uint64(batch.ActiveLen())
		o.stats.Batches++
		o.stats.InputRows += rows
		recordExpressionWork(ExpressionStats{Batches: 1, InputRows: rows})
		for index, node := range o.outputs {
			value, evalErr := node.eval(ctx, batch, batch.Selection)
			if evalErr != nil {
				_ = o.Close()
				return nil, evalErr
			}
			column, outputErr := node.output(value, batch.Len)
			if outputErr != nil {
				_ = o.Close()
				return nil, outputErr
			}
			o.view.Columns[index] = column
		}
		for pos, index := range o.metadata {
			o.view.Columns[len(o.outputs)+pos] = batch.Columns[index]
		}
		o.view.Len = batch.Len
		o.view.Selection = batch.Selection
		o.view.Hints = batch.Hints
		o.stats.OutputRows += rows
		recordExpressionWork(ExpressionStats{OutputRows: rows})
		return &o.view, nil
	}
}
