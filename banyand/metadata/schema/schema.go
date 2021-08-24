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

package schema

import (
	"context"

	commonv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v2"
	databasev2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v2"
)

type ListOpt struct {
	Group string
}

type Stream interface {
	Get(ctx context.Context, metadata *commonv2.Metadata) (*databasev2.Stream, error)
	List(ctx context.Context, opt ListOpt) ([]*databasev2.Stream, error)
}

type IndexRule interface {
	Get(ctx context.Context, metadata *commonv2.Metadata) (*databasev2.IndexRule, error)
	List(ctx context.Context, opt ListOpt) ([]*databasev2.IndexRule, error)
}

type IndexRuleBinding interface {
	Get(ctx context.Context, metadata *commonv2.Metadata) (*databasev2.IndexRuleBinding, error)
	List(ctx context.Context, opt ListOpt) ([]*databasev2.IndexRuleBinding, error)
}
