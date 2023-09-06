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

package topn

import (
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

func BuildSchema(topNSchema measure.Measure, metadata *databasev1.TopNAggregation) (logical.Schema, error) {
	md := topNSchema.GetSchema()
	ms := &schema{
		common: &logical.CommonSchema{
			TagSpecMap: make(map[string]*logical.TagSpec),
			EntityList: metadata.GroupByTagNames,
		},
		measure:  md,
		fieldMap: make(map[string]*logical.FieldSpec),
	}

	tagFamilySpecs := topNSchema.GetSchema().GetTagFamilies()
	tagFamilySpec := tagFamilySpecs[0]
	temp := tagFamilySpec.Tags[1]
	tagFamilySpec.Tags[1] = tagFamilySpec.Tags[2]
	tagFamilySpec.Tags[2] = temp
	ms.common.RegisterTagFamilies(md.GetTagFamilies())
	ms.registerField(0, measure.TopNValueFieldSpec)

	return ms, nil
}
