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

package measure

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

func TestMeasurePatternEnvelope(t *testing.T) {
	req := &measurev1.InternalQueryRequest{Request: &measurev1.QueryRequest{
		Groups: []string{"g"}, Name: "m", TimeRange: &modelv1.TimeRange{Begin: timestamppb.New(time.Unix(0, 0)), End: timestamppb.New(time.Unix(1, 0))},
	}}
	patterns := []model.IndexedPattern{{TagFamily: "default", TagName: "name", Pattern: "%a!_%", Escape: "!", IgnoreCase: true}}
	body, err := EncodeMeasurePatternRequest(req, patterns)
	require.NoError(t, err)
	decoded, got, err := DecodeMeasurePatternRequest(body)
	require.NoError(t, err)
	require.True(t, proto.Equal(req, decoded))
	require.Equal(t, patterns, got)
	for n := 0; n < len(body); n++ {
		_, _, err = DecodeMeasurePatternRequest(body[:n])
		require.Error(t, err)
	}
	_, _, err = DecodeMeasurePatternRequest(append(body, 0))
	require.Error(t, err)
	body[4] = 99
	_, _, err = DecodeMeasurePatternRequest(body)
	require.Error(t, err)
}

func TestMeasurePatternEnvelopeSizeLimit(t *testing.T) {
	_, _, err := DecodeMeasurePatternRequest(make([]byte, MaxMeasurePatternRequestBytes+1))
	require.ErrorIs(t, err, native.ErrQueryLimit)
}

func TestMeasurePatternEnvelopeMaximumPredicates(t *testing.T) {
	req := &measurev1.InternalQueryRequest{Request: &measurev1.QueryRequest{
		Groups: []string{"g"}, Name: "m", TimeRange: &modelv1.TimeRange{Begin: timestamppb.New(time.Unix(0, 0)), End: timestamppb.New(time.Unix(1, 0))},
	}}
	patterns := make([]model.IndexedPattern, model.MaxIndexedPatterns)
	for i := range patterns {
		patterns[i] = model.IndexedPattern{
			TagFamily: strings.Repeat("f", model.MaxIndexedPatternIdentityBytes),
			TagName:   strings.Repeat("t", model.MaxIndexedPatternIdentityBytes),
			Pattern:   strings.Repeat("x", native.MaxSQLPatternBytes), Escape: "😀", IgnoreCase: true,
		}
	}
	body, err := EncodeMeasurePatternRequest(req, patterns)
	require.NoError(t, err)
	decoded, got, err := DecodeMeasurePatternRequest(body)
	require.NoError(t, err)
	require.True(t, proto.Equal(req, decoded))
	require.Equal(t, patterns, got)
	require.GreaterOrEqual(t, MeasurePatternEncodeReservationBytes(req, patterns), int64(proto.Size(req)+len(body)),
		"encoding reserves both live byte buffers even with maximum pattern fields")
}
