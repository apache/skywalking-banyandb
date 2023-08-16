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
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
)

type equalityChecker func(a, b proto.Message) bool

var checkerMap = map[Kind]equalityChecker{
	KindIndexRuleBinding: func(a, b proto.Message) bool {
		return cmp.Equal(a, b,
			protocmp.IgnoreUnknown(),
			protocmp.IgnoreFields(&databasev1.IndexRuleBinding{}, "updated_at"),
			protocmp.IgnoreFields(&commonv1.Metadata{}, "id", "create_revision", "mod_revision"),
			protocmp.Transform(),
		)
	},
	KindIndexRule: func(a, b proto.Message) bool {
		return cmp.Equal(a, b,
			protocmp.IgnoreUnknown(),
			protocmp.IgnoreFields(&databasev1.IndexRule{}, "updated_at"),
			protocmp.IgnoreFields(&commonv1.Metadata{}, "id", "create_revision", "mod_revision"),
			protocmp.Transform(),
		)
	},
	KindMeasure: func(a, b proto.Message) bool {
		return cmp.Equal(a, b,
			protocmp.IgnoreUnknown(),
			protocmp.IgnoreFields(&databasev1.Measure{}, "updated_at"),
			protocmp.IgnoreFields(&commonv1.Metadata{}, "id", "create_revision", "mod_revision"),
			protocmp.Transform(),
		)
	},
	KindStream: func(a, b proto.Message) bool {
		return cmp.Equal(a, b,
			protocmp.IgnoreUnknown(),
			protocmp.IgnoreFields(&databasev1.Stream{}, "updated_at"),
			protocmp.IgnoreFields(&commonv1.Metadata{}, "id", "create_revision", "mod_revision"),
			protocmp.Transform())
	},
	KindGroup: func(a, b proto.Message) bool {
		return cmp.Equal(a, b,
			protocmp.IgnoreUnknown(),
			protocmp.IgnoreFields(&databasev1.Stream{}, "updated_at"),
			protocmp.IgnoreFields(&commonv1.Metadata{}, "id", "create_revision", "mod_revision"),
			protocmp.Transform())
	},
	KindTopNAggregation: func(a, b proto.Message) bool {
		return cmp.Equal(a, b,
			protocmp.IgnoreUnknown(),
			protocmp.IgnoreFields(&databasev1.TopNAggregation{}, "updated_at"),
			protocmp.IgnoreFields(&commonv1.Metadata{}, "id", "create_revision", "mod_revision"),
			protocmp.Transform())
	},
	KindProperty: func(a, b proto.Message) bool {
		return cmp.Equal(a, b,
			protocmp.IgnoreUnknown(),
			protocmp.IgnoreFields(&propertyv1.Property{}, "updated_at"),
			protocmp.IgnoreFields(&commonv1.Metadata{}, "id", "create_revision", "mod_revision"),
			protocmp.Transform())
	},
	KindNode: func(a, b proto.Message) bool {
		return cmp.Equal(a, b,
			protocmp.IgnoreUnknown(),
			protocmp.IgnoreFields(&databasev1.Node{}, "created_at"),
			protocmp.Transform())
	},
	KindShard: func(a, b proto.Message) bool {
		return cmp.Equal(a, b,
			protocmp.IgnoreUnknown(),
			protocmp.IgnoreFields(&databasev1.Shard{}, "updated_at"),
			protocmp.IgnoreFields(&commonv1.Metadata{}, "id", "create_revision", "mod_revision"),
			protocmp.Transform())
	},
	KindMask: func(a, b proto.Message) bool {
		return false
	},
}
