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

package bydbql

import (
	"errors"
	"reflect"
	"strings"
	"time"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// MeasureSource identifies one explicitly named measure schema dependency.
type MeasureSource struct {
	Group string
	Name  string
}

// MeasureSources reports measure sources in statement order, including repeated groups.
// Non-measure statements have no measure dependencies.
func (ps *PreparedStatement) MeasureSources() []MeasureSource {
	if ps == nil || ps.template == nil || ps.template.Select == nil {
		return nil
	}
	source := ps.template.Select.From
	if source == nil || !strings.EqualFold(source.ResourceType, "MEASURE") || source.In == nil {
		return nil
	}
	result := make([]MeasureSource, len(source.In.Groups))
	for i, group := range source.In.Groups {
		result[i] = MeasureSource{Group: group, Name: source.ResourceName}
	}
	return result
}

// BindParams binds against the template's own immutable prepared statement.
// It remains valid when the independent prepared-statement cache evicts or reparses a query.
func (m *MeasureTemplate) BindParams(params []*modelv1.TagValue, now time.Time) (*MeasureExecution, error) {
	if m == nil {
		return nil, invalidMeasure(errors.New("nil measure template"))
	}
	bound, err := m.statement.Bind(params)
	if err != nil {
		return nil, err
	}
	return m.Bind(bound, now)
}

// EstimatedSize estimates the retained Go object graph, including shared syntax.
// Shared pointers and backing arrays are counted once; global protobuf runtime
// descriptors are excluded because the template does not own them.
func (m *MeasureTemplate) EstimatedSize() int {
	return deepSize(reflect.ValueOf(m))
}
