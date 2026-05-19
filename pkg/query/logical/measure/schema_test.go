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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions
// and limitations under the License.

package measure

import (
	"testing"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

// mergeSchema is called from DistributedAnalyze whenever a measure query
// spans more than one group. The merged schema must keep enough of the
// underlying *databasev1.Measure that downstream consumers (notably the
// IndexMode check in unresolvedDistributed.Analyze) do not nil-deref.
func TestMergeSchema_PropagatesMeasure(t *testing.T) {
	build := func(group string, indexMode bool) *schema {
		md := &databasev1.Measure{
			Metadata: &commonv1.Metadata{Name: "zipkin_service_traffic_minute", Group: group},
			Entity:   &databasev1.Entity{TagNames: []string{"id"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "storage-only", Tags: []*databasev1.TagSpec{{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING}}},
			},
			IndexMode: indexMode,
		}
		s, err := BuildSchema(md, nil)
		if err != nil {
			t.Fatalf("BuildSchema(%s) failed: %v", group, err)
		}
		return s.(*schema)
	}

	t.Run("multi-group merge carries IndexMode=true", func(t *testing.T) {
		a := build("sw_metadata", true)
		b := build("sw_metadata_replica", true)
		merged, err := mergeSchema([]logical.Schema{a, b})
		if err != nil {
			t.Fatalf("mergeSchema: %v", err)
		}
		ms := merged.(*schema)
		if ms.measure == nil {
			t.Fatal("merged schema lost its *databasev1.Measure; downstream IndexMode checks will nil-deref")
		}
		if !ms.measure.IndexMode {
			t.Fatalf("merged IndexMode = false, want true")
		}
	})

	t.Run("multi-group merge carries IndexMode=false", func(t *testing.T) {
		a := build("sw_metricsMinute", false)
		b := build("sw_metricsMinute_replica", false)
		merged, err := mergeSchema([]logical.Schema{a, b})
		if err != nil {
			t.Fatalf("mergeSchema: %v", err)
		}
		ms := merged.(*schema)
		if ms.measure == nil {
			t.Fatal("merged schema lost its *databasev1.Measure")
		}
		if ms.measure.IndexMode {
			t.Fatalf("merged IndexMode = true, want false")
		}
	})

	t.Run("single-group fast path returns input unchanged", func(t *testing.T) {
		a := build("sw_metadata", true)
		merged, err := mergeSchema([]logical.Schema{a})
		if err != nil {
			t.Fatalf("mergeSchema: %v", err)
		}
		if merged.(*schema) != a {
			t.Fatal("single-group mergeSchema returned a new instance; expected pass-through")
		}
	})
}
