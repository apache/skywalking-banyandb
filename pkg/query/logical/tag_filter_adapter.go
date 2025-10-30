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

package logical

import (
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

type tagFilterAdapter struct {
	filter  TagFilter
	schema  Schema
	decoder model.TagValueDecoder
}

// NewTagFilterMatcher creates a TagFilterMatcher from a TagFilter and Schema.
func NewTagFilterMatcher(filter TagFilter, schema Schema, decoder model.TagValueDecoder) model.TagFilterMatcher {
	if filter == nil || filter == DummyFilter {
		return nil
	}
	return &tagFilterAdapter{
		filter:  filter,
		schema:  schema,
		decoder: decoder,
	}
}

func (tfa *tagFilterAdapter) Match(tags []*modelv1.Tag) (bool, error) {
	// Convert tags to TagFamily for logical.TagFilter.Match
	family := &modelv1.TagFamily{
		Name: "",
		Tags: tags,
	}

	tagFamilies := []*modelv1.TagFamily{family}
	return tfa.filter.Match(TagFamilies(tagFamilies), tfa.schema)
}

func (tfa *tagFilterAdapter) GetDecoder() model.TagValueDecoder {
	return tfa.decoder
}
