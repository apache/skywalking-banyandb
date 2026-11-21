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

package rowsrc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

// MeasureSchemas are the schemas of a measure group the readers need.
type MeasureSchemas struct {
	Measures []*databasev1.Measure
	Rules    []*databasev1.IndexRule
	Bindings []*databasev1.IndexRuleBinding
	// IndexMode holds the index-mode measures of Measures, whose rows live in the series index.
	IndexMode []*databasev1.Measure
}

// LoadMeasureSchemas lists the measures, index rules and index rule bindings of a group.
func LoadMeasureSchemas(ctx context.Context, meta metadata.Repo, group string) (*MeasureSchemas, error) {
	measures, err := meta.MeasureRegistry().ListMeasure(ctx, schema.ListOpt{Group: group})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list measures of group %s: %v", group, err)
	}
	rules, err := meta.IndexRuleRegistry().ListIndexRule(ctx, schema.ListOpt{Group: group})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list index rules of group %s: %v", group, err)
	}
	bindings, err := meta.IndexRuleBindingRegistry().ListIndexRuleBinding(ctx, schema.ListOpt{Group: group})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list index rule bindings of group %s: %v", group, err)
	}
	s := &MeasureSchemas{Measures: measures, Rules: rules, Bindings: bindings}
	for _, m := range measures {
		if m.GetIndexMode() {
			s.IndexMode = append(s.IndexMode, m)
		}
	}
	return s, nil
}
