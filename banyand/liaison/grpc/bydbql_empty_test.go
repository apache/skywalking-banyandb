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

package grpc

import (
	"testing"

	"github.com/stretchr/testify/require"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

// A statement that asks for no row is answered with an empty response of its own type, so that the client reads it like any
// other result; a type that has no such response is executed as usual.
func TestEmptyQueryResponseIsOfTheTypeAsked(t *testing.T) {
	stream := emptyQueryResponse(bydbql.QueryTypeStream)
	require.NotNil(t, stream.GetStreamResult())
	require.Empty(t, stream.GetStreamResult().GetElements())

	measure := emptyQueryResponse(bydbql.QueryTypeMeasure)
	require.NotNil(t, measure.GetMeasureResult())
	require.Empty(t, measure.GetMeasureResult().GetDataPoints())

	trace := emptyQueryResponse(bydbql.QueryTypeTrace)
	require.NotNil(t, trace.GetTraceResult())
	require.Empty(t, trace.GetTraceResult().GetTraces())

	property := emptyQueryResponse(bydbql.QueryTypeProperty)
	require.NotNil(t, property.GetPropertyResult())
	require.Empty(t, property.GetPropertyResult().GetProperties())

	require.Nil(t, emptyQueryResponse(bydbql.QueryTypeTopN), "a TOP N query has no LIMIT to be empty")
	var none *bydbqlv1.QueryResponse
	require.Equal(t, none, emptyQueryResponse(bydbql.QueryType(-1)), "an unknown type is not answered as empty")
}

func TestSkipPropertiesDropsTheLeadingOnes(t *testing.T) {
	properties := make([]*propertyv1.Property, 5)
	for i := range properties {
		properties[i] = &propertyv1.Property{Id: string(rune('a' + i)), Metadata: &commonv1.Metadata{Name: "p"}}
	}
	ids := func(kept []*propertyv1.Property) string {
		var out string
		for _, property := range kept {
			out += property.GetId()
		}
		return out
	}
	require.Equal(t, "abcde", ids(skipProperties(properties, 0)))
	require.Equal(t, "cde", ids(skipProperties(properties, 2)))
	require.Equal(t, "e", ids(skipProperties(properties, 4)))
	require.Empty(t, skipProperties(properties, 5), "skipping every row leaves none")
	require.Empty(t, skipProperties(properties, 100), "skipping past the end leaves none")
	require.Empty(t, skipProperties(nil, 3))
	require.Empty(t, skipProperties(properties, 4294967295), "the largest offset does not wrap")
}
