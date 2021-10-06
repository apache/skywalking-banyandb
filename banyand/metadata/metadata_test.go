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

package metadata

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

func Test_service_RulesBySubject(t *testing.T) {
	type args struct {
		subject *commonv1.Metadata
	}
	is := assert.New(t)
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "golden path",
			args: args{
				subject: createSubject("sw", "default"),
			},
			want: []string{
				"trace_id",
				"duration",
				"endpoint_id",
				"status_code",
				"http.method",
				"db.instance",
				"db.type",
				"mq.broker",
				"mq.queue",
				"mq.topic",
			},
		},
		{
			name: "got empty idWithShard",
			args: args{
				subject: createSubject("invalid", "default"),
			},
			want: make([]string, 0),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			s, _ := NewService(ctx)
			err := s.PreRun()
			if err != nil {
				t.Errorf("Service.Serve() error = %v", err)
				return
			}
			defer s.GracefulStop()
			if (err != nil) != tt.wantErr {
				t.Errorf("NewService() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got, err := s.IndexRules(ctx, tt.args.subject)
			if (err != nil) != tt.wantErr {
				t.Errorf("RulesBySubject() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			is.Equal(getIndexRule(s, tt.want...), got)
		})
	}
}

func getIndexRule(s Service, names ...string) []*databasev1.IndexRule {
	ruleRepo := s.IndexRuleRegistry()
	result := make([]*databasev1.IndexRule, 0, len(names))
	for _, name := range names {
		indexRule, _ := ruleRepo.GetIndexRule(context.TODO(), &commonv1.Metadata{
			Group: "default",
			Name:  name,
		})
		result = append(result, indexRule)
	}
	return result
}

func createSubject(name, group string) *commonv1.Metadata {
	return &commonv1.Metadata{
		Group: group,
		Name:  name,
	}
}
