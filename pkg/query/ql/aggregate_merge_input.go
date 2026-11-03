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
	"fmt"
	"math"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func (o *nativeAggregateOperator) consume(ctx context.Context) error {
	if len(o.groupDefs) == 0 {
		if _, err := o.groupID(); err != nil {
			return err
		}
	}
	for _, branch := range o.branches {
		if err := ctx.Err(); err != nil {
			return err
		}
		source, err := o.open(ctx, branch.leaf)
		o.current = source
		if err != nil {
			return err
		}
		if source == nil || source.OutputSchema() == nil {
			return fmt.Errorf("native aggregate leaf for %q has no schema", branch.leaf.Source.Name)
		}
		o.stats.Branches++
		if err = o.consumeLeaf(ctx, branch); err != nil {
			return err
		}
		clear(o.groupValues)
		if err = o.closeCurrent(); err != nil {
			return err
		}
	}
	if err := o.validateGroups(ctx); err != nil {
		return err
	}
	o.complete = true
	recordNativeAggregateExecution(o.statistics())
	return nil
}

func (o *nativeAggregateOperator) consumeLeaf(ctx context.Context, branch nativeAggregateBranch) error {
	schema := o.current.OutputSchema()
	readers := make([]aggregateReader, len(o.groupDefs))
	for index, def := range o.groupDefs {
		reader, err := newAggregateReader(schema, def)
		if err != nil {
			return err
		}
		readers[index] = reader
	}
	valueDef := vectorized.ColumnDef{Name: branch.leaf.Source.Name, Role: vectorized.RoleField, Type: branch.leaf.Source.Type}
	if branch.leaf.Function == AggregateCount && valueDef.Type != vectorized.ColumnTypeFloat64 {
		valueDef.Type = vectorized.ColumnTypeInt64
	}
	value, err := newAggregateReader(schema, valueDef)
	if err != nil {
		return err
	}
	if err = o.current.Init(ctx); err != nil {
		return err
	}
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		batch, pullErr := o.current.NextBatch(ctx)
		if pullErr != nil {
			return pullErr
		}
		if batch == nil {
			return nil
		}
		if batch.Len < 0 || batch.Len > math.MaxUint16+1 {
			return fmt.Errorf("native aggregate leaf has invalid batch length %d", batch.Len)
		}
		if !nativeAggregateSchemaMatches(schema, batch.Schema) {
			return fmt.Errorf("native aggregate leaf for %q changed its batch schema", branch.leaf.Source.Name)
		}
		for index := range readers {
			if err = readers[index].bind(batch); err != nil {
				return err
			}
		}
		if err = value.bind(batch); err != nil {
			return err
		}
		for position := 0; position < batch.ActiveLen(); position++ {
			if position%1024 == 0 {
				if err = ctx.Err(); err != nil {
					return err
				}
			}
			row := position
			if batch.Selection != nil {
				row = int(batch.Selection[position])
			}
			if row >= batch.Len {
				return fmt.Errorf("native aggregate selection row %d exceeds batch length %d", row, batch.Len)
			}
			for index := range readers {
				cell, readErr := readers[index].value(row)
				if readErr != nil {
					return readErr
				}
				o.groupValues[index] = cell
			}
			group, groupErr := o.groupID()
			if groupErr != nil {
				return groupErr
			}
			cell, readErr := value.value(row)
			if readErr != nil {
				return readErr
			}
			if err = o.acceptValue(branch, group, cell); err != nil {
				return err
			}
			o.stats.InputRows++
			o.stats.MergeVisits++
		}
	}
}

func nativeAggregateSchemaMatches(expected, actual *vectorized.BatchSchema) bool {
	if expected == actual {
		return true
	}
	if actual == nil || len(expected.Columns) != len(actual.Columns) {
		return false
	}
	for index, def := range expected.Columns {
		if def != actual.Columns[index] {
			return false
		}
	}
	return true
}

func (o *nativeAggregateOperator) acceptValue(branch nativeAggregateBranch, group int, value aggregateValue) error {
	state := &o.states[branch.state]
	bit := uint16(1) << branch.leaf.Function
	if state.presence[group]&bit != 0 {
		return fmt.Errorf("native aggregate leaf for %q returned a duplicate finalized group", branch.leaf.Source.Name)
	}
	state.presence[group] |= bit
	if value.null {
		if branch.leaf.Function == AggregateCount {
			return fmt.Errorf("native aggregate COUNT(%s) returned null", branch.leaf.Source.Name)
		}
		return nil
	}
	state.presence[group] |= bit << 4
	switch branch.leaf.Function {
	case AggregateCount:
		count := value.i64
		if value.typ == vectorized.ColumnTypeFloat64 {
			if math.IsNaN(value.f64) || math.IsInf(value.f64, 0) || value.f64 < 0 || value.f64 > float64(o.maxCount) || math.Trunc(value.f64) != value.f64 {
				return fmt.Errorf("native aggregate COUNT(%s) is not an exact bounded integer", branch.leaf.Source.Name)
			}
			count = int64(value.f64)
		}
		if count < 0 || count > o.maxCount {
			return fmt.Errorf("native aggregate COUNT(%s) exceeds its proven bound", branch.leaf.Source.Name)
		}
		state.count[group] = count
	case AggregateSum:
		if len(state.sumInt) != 0 {
			state.sumInt[group] = value.i64
		}
		if len(state.sumFloat) != 0 {
			state.sumFloat[group] = value.f64
			if value.typ == vectorized.ColumnTypeInt64 {
				state.sumFloat[group] = float64(value.i64)
			}
		}
	case AggregateMin:
		if value.typ == vectorized.ColumnTypeInt64 {
			state.minInt[group] = value.i64
		} else {
			state.minFloat[group] = value.f64
		}
	case AggregateMax:
		if value.typ == vectorized.ColumnTypeInt64 {
			state.maxInt[group] = value.i64
		} else {
			state.maxFloat[group] = value.f64
		}
	default:
		return fmt.Errorf("unsupported native aggregate leaf function %d", branch.leaf.Function)
	}
	return nil
}

func (o *nativeAggregateOperator) validateGroups(ctx context.Context) error {
	for group := range o.keys {
		if group%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		for index := range o.states {
			state := &o.states[index]
			presence := state.presence[group]
			if presence&1 == 0 && presence>>4 != 0 {
				return fmt.Errorf("native aggregate group for %q is missing its COUNT", state.source.Name)
			}
			if state.count[group] > 0 && (presence>>4)&state.required != state.required {
				return fmt.Errorf("native aggregate group for %q has a positive COUNT but is missing a required value", state.source.Name)
			}
		}
		o.stats.MergeVisits++
	}
	return nil
}
