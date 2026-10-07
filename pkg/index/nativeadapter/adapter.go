// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses
// this file to you under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may
// obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// Package nativeadapter bridges the public index API to native owner queries.
package nativeadapter

import (
	"context"
	"fmt"
	"strconv"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/index/nativeanalysis"
)

// Adapter translates public index documents to native owner batches.
type Adapter struct {
	Owner *native.Owner
}

// encodeField converts one public field to its native representation.
func encodeField(field index.Field) (native.Field, error) {
	nf := native.Field{Name: field.Key.Marshal(), Store: field.Store, Index: true, Sort: !field.NoSort}
	switch term := field.GetTerm().(type) {
	case *index.BytesTermValue:
		nf.Value = append([]byte(nil), term.Value...)
		if field.Key.Analyzer != index.AnalyzerUnspecified && field.Key.Analyzer != index.AnalyzerKeyword {
			analyzed, err := nativeanalysis.Analyze(field.Key.Analyzer, term.Value)
			if err != nil {
				return native.Field{}, err
			}
			nf.Terms = make([]native.Term, 0, len(analyzed))
			for _, t := range analyzed {
				nf.Terms = append(nf.Terms, native.Term{Value: t.Value, Frequency: t.Frequency})
			}
		}
	case *index.FloatTermValue:
		sortable := encoding.Float64ToSortableInt64(term.Value)
		nf.Value = numericPrefix(sortable, 0)
		nf.Terms = append(nf.Terms, native.Term{Value: []byte(strconv.FormatFloat(term.Value, 'f', -1, 64)), Frequency: 1})
		if field.Key.Analyzer != index.AnalyzerUnspecified {
			analyzed, err := nativeanalysis.Analyze(field.Key.Analyzer, nf.Value)
			if err != nil {
				return native.Field{}, err
			}
			nf.Terms = nf.Terms[:0]
			for _, token := range analyzed {
				nf.Terms = append(nf.Terms, native.Term{Value: token.Value, Frequency: token.Frequency})
			}
		}
		for shift := uint(0); shift <= 60; shift += 4 {
			nf.Terms = append(nf.Terms, native.Term{Value: numericPrefix(sortable, shift), Frequency: 1})
		}
	default:
		return native.Field{}, fmt.Errorf("native adapter: unsupported field %T", field.GetTerm())
	}
	return nf, nil
}

// Batch admits documents through the native owner.
func (a *Adapter) Batch(ctx context.Context, batch index.Batch) error {
	if a == nil || a.Owner == nil {
		return fmt.Errorf("native adapter: nil owner")
	}
	docs := make([]native.Document, 0, len(batch.Documents))
	for _, d := range batch.Documents {
		timestamp := d.Timestamp
		if timestamp < 0 {
			timestamp = 0
		}
		nd := native.Document{Identifier: convert.Uint64ToBytes(d.DocID), Timestamp: timestamp}
		for i, f := range d.Fields {
			nf, err := encodeField(f)
			if err != nil {
				return err
			}
			nd.Fields = append(nd.Fields, nf)
			if i == 0 {
				nd.Fields = append(nd.Fields, native.Field{Name: "_series_id", Value: convert.Uint64ToBytes(uint64(f.Key.SeriesID)), Store: true, Index: true})
			}
		}
		docs = append(docs, nd)
	}
	return a.Owner.Batch(ctx, native.Batch{Documents: docs, Mode: native.BatchInsertOnly, PersistentCallback: batch.PersistentCallback})
}

// Acquire pins one native read view for query operations.
func (a *Adapter) Acquire(ctx context.Context) (*native.ReadView, error) {
	if a == nil || a.Owner == nil {
		return nil, fmt.Errorf("native adapter: nil owner")
	}
	return a.Owner.Acquire(ctx)
}

// MatchTerms executes exact term membership on one pinned view.
func (a *Adapter) MatchTerms(ctx context.Context, view *native.ReadView, request native.TermSetRequest) ([]native.QueryHit, error) {
	if view == nil {
		return nil, fmt.Errorf("native adapter: nil view")
	}
	return view.MatchTermsSet(ctx, request)
}

// MatchField executes bounded indexed-field presence matching.
func (a *Adapter) MatchField(ctx context.Context, view *native.ReadView, request native.FieldRequest) ([]native.QueryHit, error) {
	if view == nil {
		return nil, fmt.Errorf("native adapter: nil view")
	}
	return view.MatchField(ctx, request)
}

// MatchRange executes bounded encoded range matching.
func (a *Adapter) MatchRange(ctx context.Context, view *native.ReadView, request native.RangeRequest) ([]native.QueryHit, error) {
	if view == nil {
		return nil, fmt.Errorf("native adapter: nil view")
	}
	return view.MatchRange(ctx, request)
}

func numericPrefix(value int64, shift uint) []byte {
	n := ((63 - shift) / 7) + 1
	out := make([]byte, n+1)
	out[0] = byte(0x20 + shift)
	bits := uint64(value) ^ 0x8000000000000000
	bits >>= shift
	for i := n; i > 0; i-- {
		out[i] = byte(bits & 0x7f)
		bits >>= 7
	}
	return out
}
