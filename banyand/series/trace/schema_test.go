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

package trace

import (
	"context"
	"reflect"
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/apache/skywalking-banyandb/api/common"
	v1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/banyand/series"
	"github.com/apache/skywalking-banyandb/banyand/series/schema/sw"
)

func Test_service_RulesBySubject(t *testing.T) {
	type args struct {
		series v1.Series
		filter series.IndexObjectFilter
	}
	tests := []struct {
		name    string
		args    args
		want    []v1.IndexRule
		wantErr bool
	}{
		{
			name: "golden path",
			args: args{
				series: createSubject("sw", "default"),
			},
			want: getIndexRule("sw-index-rule", "default"),
		},
		{
			name: "filter index object",
			args: args{
				series: createSubject("sw", "default"),
				filter: func(object v1.IndexObject) bool {
					return string(object.Fields(0)) == "trace_id"
				},
			},
			want: getIndexRule("sw-index-rule", "default"),
		},
		{
			name: "got empty result",
			args: args{
				series: createSubject("sw", "default"),
				filter: func(object v1.IndexObject) bool {
					return string(object.Fields(0)) == "invalid"
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{}
			ctx := context.Background()
			got, err := s.IndexRules(ctx, tt.args.series, tt.args.filter)
			if (err != nil) != tt.wantErr {
				t.Errorf("RulesBySubject() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got) < 1 && len(tt.want) < 1 {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RulesBySubject() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func getIndexRule(name, group string) []v1.IndexRule {
	b := flatbuffers.NewBuilder(0)
	b.Finish(createMetadata(b, name, group))
	md := v1.GetRootAsMetadata(b.FinishedBytes(), 0)
	indexRule, _ := sw.NewIndexRule().Get(context.Background(), common.Metadata{KindVersion: common.MetadataKindVersion, Spec: md})
	return []v1.IndexRule{indexRule.Spec}
}

func createMetadata(b *flatbuffers.Builder, name, group string) flatbuffers.UOffsetT {
	namePos := b.CreateString(name)
	groupPos := b.CreateString(group)
	v1.MetadataStart(b)
	v1.MetadataAddName(b, namePos)
	v1.MetadataAddGroup(b, groupPos)
	return v1.MetadataEnd(b)
}

func createSubject(name, group string) v1.Series {
	b := flatbuffers.NewBuilder(0)
	namePos := b.CreateString(name)
	groupPos := b.CreateString(group)
	v1.MetadataStart(b)
	v1.MetadataAddName(b, namePos)
	v1.MetadataAddGroup(b, groupPos)
	s := v1.MetadataEnd(b)
	v1.IndexRuleStart(b)
	v1.SeriesAddCatalog(b, v1.CatalogTrace)
	v1.SeriesAddSeries(b, s)
	b.Finish(v1.IndexRuleEnd(b))
	return *v1.GetRootAsSeries(b.FinishedBytes(), 0)
}
