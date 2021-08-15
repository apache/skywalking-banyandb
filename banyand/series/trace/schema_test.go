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

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/series"
	"github.com/apache/skywalking-banyandb/banyand/series/schema/sw"
)

func Test_service_RulesBySubject(t *testing.T) {
	type args struct {
		series *databasev1.Series
		filter series.IndexObjectFilter
	}
	tests := []struct {
		name    string
		args    args
		want    []*databasev1.IndexRule
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
				filter: func(object *databasev1.IndexObject) bool {
					return object.GetFields()[0] == "trace_id"
				},
			},
			want: getIndexRule("sw-index-rule", "default"),
		},
		{
			name: "got empty idWithShard",
			args: args{
				series: createSubject("sw", "default"),
				filter: func(object *databasev1.IndexObject) bool {
					return object.GetFields()[0] == "invalid"
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

func getIndexRule(name, group string) []*databasev1.IndexRule {
	indexRule, _ := sw.NewIndexRule().Get(context.Background(), common.Metadata{
		KindVersion: common.MetadataKindVersion,
		Spec: &commonv1.Metadata{
			Group: group,
			Name:  name,
		}},
	)
	return []*databasev1.IndexRule{indexRule.Spec}
}

func createSubject(name, group string) *databasev1.Series {
	return &databasev1.Series{
		Series: &commonv1.Metadata{
			Group: group,
			Name:  name,
		},
		Catalog: databasev1.Series_CATALOG_TRACE,
	}
}
