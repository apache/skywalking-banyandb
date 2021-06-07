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

//go:generate flatc --root-type "banyandb.v1.TraceSeries" --binary  ../../../../api/fbs/v1/schema.fbs trace_series.json
//go:generate flatc --root-type "banyandb.v1.IndexRule" --binary  ../../../../api/fbs/v1/schema.fbs index_rule.json
//go:generate flatc --root-type "banyandb.v1.IndexRuleBinding" --binary  ../../../../api/fbs/v1/schema.fbs index_rule_binding.json
package sw

import (
	"context"
	//nolint:golint
	_ "embed"

	"github.com/apache/skywalking-banyandb/api/common"
	v1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	apischema "github.com/apache/skywalking-banyandb/api/schema"
	"github.com/apache/skywalking-banyandb/banyand/series/schema"
)

var (
	_ schema.TraceSeries = (*traceSeriesRepo)(nil)
	_ schema.IndexRule   = (*indexRuleRepo)(nil)

	//go:embed trace_series.bin
	traceSeriesBin []byte
	//go:embed index_rule.bin
	indexRuleBin []byte
	//go:embed index_rule_binding.bin
	indexRuleBindingBin []byte
)

type traceSeriesRepo struct {
}

func NewTraceSeries() schema.TraceSeries {
	return &traceSeriesRepo{}
}

func (l *traceSeriesRepo) Get(_ context.Context, _ common.Metadata) (apischema.TraceSeries, error) {
	return apischema.TraceSeries{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        *v1.GetRootAsTraceSeries(traceSeriesBin, 0),
	}, nil

}

func (l *traceSeriesRepo) List(ctx context.Context, _ schema.ListOpt) ([]apischema.TraceSeries, error) {
	t, err := l.Get(ctx, common.Metadata{})
	if err != nil {
		return nil, err
	}
	return []apischema.TraceSeries{t}, nil
}

type indexRuleRepo struct {
}

func NewIndexRule() schema.IndexRule {
	return &indexRuleRepo{}
}

func (i *indexRuleRepo) Get(ctx context.Context, metadata common.Metadata) (apischema.IndexRule, error) {
	return apischema.IndexRule{
		KindVersion: apischema.IndexRuleKindVersion,
		Spec:        *v1.GetRootAsIndexRule(indexRuleBin, 0),
	}, nil
}

func (i *indexRuleRepo) List(ctx context.Context, opt schema.ListOpt) ([]apischema.IndexRule, error) {
	t, err := i.Get(ctx, common.Metadata{})
	if err != nil {
		return nil, err
	}
	return []apischema.IndexRule{t}, nil
}

type indexRuleBindingRepo struct {
}

func NewIndexRuleBinding() schema.IndexRuleBinding {
	return &indexRuleBindingRepo{}
}

func (i *indexRuleBindingRepo) Get(_ context.Context, _ common.Metadata) (apischema.IndexRuleBinding, error) {
	return apischema.IndexRuleBinding{
		KindVersion: apischema.IndexRuleBindingKindVersion,
		Spec:        *v1.GetRootAsIndexRuleBinding(indexRuleBindingBin, 0),
	}, nil
}

func (i *indexRuleBindingRepo) List(ctx context.Context, _ schema.ListOpt) ([]apischema.IndexRuleBinding, error) {
	t, err := i.Get(ctx, common.Metadata{})
	if err != nil {
		return nil, err
	}
	return []apischema.IndexRuleBinding{t}, nil
}
