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

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	testhelper "github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	test "github.com/apache/skywalking-banyandb/pkg/test/stream"
)

func Test_service_RulesBySubject(t *testing.T) {
	type args struct {
		subject *commonv1.Metadata
	}
	is := assert.New(t)
	is.NoError(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	}))
	ctx := context.TODO()
	s, _ := NewService(ctx)
	is.NotNil(s)
	rootDir, deferFn, err := testhelper.NewSpace()
	is.NoError(err)
	err = s.FlagSet().Parse([]string{"--metadata-root-path=" + rootDir})
	is.NoError(err)
	is.NoError(s.Validate())
	ctx = context.WithValue(ctx, common.ContextNodeKey, common.Node{NodeID: "test"})
	ctx = context.WithValue(ctx, common.ContextNodeRolesKey, []databasev1.Role{databasev1.Role_ROLE_META})
	err = s.PreRun(ctx)
	is.NoError(err)
	defer func() {
		s.GracefulStop()
		deferFn()
	}()

	err = test.PreloadSchema(ctx, s.SchemaRegistry())
	is.NoError(err)

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
				"extended_tags",
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
			if (err != nil) != tt.wantErr {
				t.Errorf("NewService() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got, err := s.IndexRules(ctx, tt.args.subject)
			if (err != nil) != tt.wantErr {
				t.Errorf("RulesBySubject() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			is.Equal(getIndexRule(ctx, s, tt.want...), got)
		})
	}
}

func getIndexRule(ctx context.Context, s Service, names ...string) []*databasev1.IndexRule {
	ruleRepo := s.IndexRuleRegistry()
	result := make([]*databasev1.IndexRule, 0, len(names))
	for _, name := range names {
		indexRule, _ := ruleRepo.GetIndexRule(ctx, &commonv1.Metadata{
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
