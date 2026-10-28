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

package data

import (
	"context"
	"fmt"
	"maps"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/testing/protocmp"
	"sigs.k8s.io/yaml"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

// VerifyQL executes a QL-only fixture, including its #!params directive, against the real RPC.
// Expected values are authored independently as measure response YAML; no native request is required.
func VerifyQL(ctx context.Context, conn grpc.ClientConnInterface, content string, wantYAML []byte, ordered bool) (*measurev1.QueryResponse, error) {
	request, err := qlRequest(content)
	if err != nil {
		return nil, err
	}
	expectedJSON, err := yaml.YAMLToJSONStrict(wantYAML)
	if err != nil {
		return nil, fmt.Errorf("decode expected QL YAML: %w", err)
	}
	want := &measurev1.QueryResponse{}
	if err = protojson.Unmarshal(expectedJSON, want); err != nil {
		return nil, fmt.Errorf("decode expected QL response: %w", err)
	}
	response, err := bydbqlv1.NewBydbQLServiceClient(conn).Query(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("query QL: %w", err)
	}
	result, ok := response.GetResult().(*bydbqlv1.QueryResponse_MeasureResult)
	if !ok || result.MeasureResult == nil {
		return nil, fmt.Errorf("QL returned %T instead of a measure result", response.GetResult())
	}
	if err = CompareQLResult(result.MeasureResult, want, ordered); err != nil {
		return nil, err
	}
	return result.MeasureResult, nil
}

// VerifyQLError asserts a gRPC status and optional message fragments for a QL-only fixture.
func VerifyQLError(ctx context.Context, conn grpc.ClientConnInterface, content string, wantCode codes.Code, wantMessages ...string) error {
	if wantCode == codes.OK {
		return fmt.Errorf("expected error status must not be OK")
	}
	request, err := qlRequest(content)
	if err != nil {
		return err
	}
	_, err = bydbqlv1.NewBydbQLServiceClient(conn).Query(ctx, request)
	if err == nil {
		return fmt.Errorf("QL succeeded, want status %s", wantCode)
	}
	if status.Code(err) != wantCode {
		return fmt.Errorf("QL status: got %s, want %s %w", status.Code(err), wantCode, err)
	}
	for _, message := range wantMessages {
		if !strings.Contains(status.Convert(err).Message(), message) {
			return fmt.Errorf("QL error is missing %q: %w", message, err)
		}
	}
	return nil
}

func qlRequest(content string) (*bydbqlv1.QueryRequest, error) {
	query, params, err := helpers.ExtractQL(content)
	if err != nil {
		return nil, fmt.Errorf("extract QL fixture: %w", err)
	}
	return &bydbqlv1.QueryRequest{Query: query, Params: params}, nil
}

// CompareQLResult checks types, nulls, names and timestamps, ignoring only storage identity and trace.
// Unordered rows form a multiset: duplicate rows must occur equally often on both sides.
func CompareQLResult(got, want *measurev1.QueryResponse, ordered bool) error {
	if got == nil || want == nil {
		return fmt.Errorf("QL measure response must not be nil")
	}
	options := []cmp.Option{
		protocmp.Transform(),
		protocmp.IgnoreFields(&measurev1.DataPoint{}, "sid", "version"),
		protocmp.IgnoreFields(&measurev1.QueryResponse{}, "trace"),
		cmpopts.EquateApprox(0, 1e-12),
		protocmp.FilterMessage(&modelv1.Float{}, cmp.Comparer(qlFloatsEqual)),
	}
	if ordered {
		if diff := cmp.Diff(want, got, options...); diff != "" {
			return fmt.Errorf("QL ordered result mismatch (-want +got):\n%s", diff)
		}
		return nil
	}
	// Compare response-level metadata separately; only row ordering is unspecified.
	gotMetadata := proto.Clone(got).(*measurev1.QueryResponse)
	wantMetadata := proto.Clone(want).(*measurev1.QueryResponse)
	gotMetadata.DataPoints, wantMetadata.DataPoints = nil, nil
	if diff := cmp.Diff(wantMetadata, gotMetadata, options...); diff != "" {
		return fmt.Errorf("QL response metadata mismatch (-want +got):\n%s", diff)
	}
	if len(got.DataPoints) != len(want.DataPoints) {
		return fmt.Errorf("QL row count: got %d, want %d", len(got.DataPoints), len(want.DataPoints))
	}
	buckets, exactCandidates := qlCandidateBuckets(got.DataPoints, want.DataPoints)
	// Approximate equality is not transitive. Find a complete one-to-one matching,
	// allowing an earlier pair to move when a later row has fewer candidates.
	candidates := make([][]int, len(want.DataPoints))
	matchedBy := make([]int, len(got.DataPoints))
	for index := range matchedBy {
		matchedBy[index] = -1
	}
	var match func(int, []bool) bool
	match = func(index int, visited []bool) bool {
		if candidates[index] == nil {
			candidates[index] = make([]int, 0)
			var possible []int
			if exactCandidates {
				possible = buckets[index]
			} else {
				possible = make([]int, len(got.DataPoints))
				for ordinal := range possible {
					possible[ordinal] = ordinal
				}
			}
			for _, candidate := range possible {
				if cmp.Equal(want.DataPoints[index], got.DataPoints[candidate], options...) {
					candidates[index] = append(candidates[index], candidate)
				}
			}
		}
		for _, candidate := range candidates[index] {
			if visited[candidate] {
				continue
			}
			visited[candidate] = true
			if matchedBy[candidate] == -1 || match(matchedBy[candidate], visited) {
				matchedBy[candidate] = index
				return true
			}
		}
		return false
	}
	for index, expected := range want.DataPoints {
		if !match(index, make([]bool, len(got.DataPoints))) {
			return fmt.Errorf("QL multiset cannot match expected row %d: %s; actual: %s", index, expected, got)
		}
	}
	return nil
}

func qlFloatsEqual(left, right protocmp.Message) bool {
	// Transform omits default scalar fields. Restore zero so the tolerance also
	// applies around zero, while retaining message presence and unknown fields.
	left, right = maps.Clone(left), maps.Clone(right)
	if _, present := left["value"]; !present {
		left["value"] = float64(0)
	}
	if _, present := right["value"]; !present {
		right["value"] = float64(0)
	}
	return cmp.Equal(left, right, cmpopts.EquateApprox(0, 1e-12), cmpopts.EquateNaNs())
}

// qlCandidateBuckets narrows exact rows by wire structure. Keys only select
// candidates: the full comparison and one-to-one matching remain authoritative.
// Float-bearing responses and unknown fields retain the original global matcher.
func qlCandidateBuckets(got, want []*measurev1.DataPoint) ([][]int, bool) {
	keys := make([][]string, 2)
	for side, rows := range [][]*measurev1.DataPoint{got, want} {
		for _, row := range rows {
			if !qlExactKeyEligible(row.ProtoReflect()) {
				return nil, false
			}
			normalized := proto.Clone(row).(*measurev1.DataPoint)
			if normalized != nil {
				normalized.Sid = 0
				normalized.Version = 0
			}
			data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(normalized)
			if err != nil {
				return nil, false
			}
			keys[side] = append(keys[side], string(data))
		}
	}
	byKey := make(map[string][]int, len(got))
	for ordinal, key := range keys[0] {
		byKey[key] = append(byKey[key], ordinal)
	}
	candidates := make([][]int, len(want))
	for ordinal, key := range keys[1] {
		candidates[ordinal] = byKey[key]
	}
	return candidates, true
}

func qlExactKeyEligible(message protoreflect.Message) bool {
	if !message.IsValid() {
		return true
	}
	if len(message.GetUnknown()) > 0 {
		return false
	}
	fields := message.Descriptor().Fields()
	for ordinal := range fields.Len() {
		field := fields.Get(ordinal)
		if field.Kind() == protoreflect.FloatKind || field.Kind() == protoreflect.DoubleKind {
			return false
		}
	}
	eligible := true
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsMap():
			value.Map().Range(func(_ protoreflect.MapKey, item protoreflect.Value) bool {
				kind := field.MapValue().Kind()
				switch kind {
				case protoreflect.DoubleKind, protoreflect.FloatKind:
					eligible = false
				case protoreflect.MessageKind, protoreflect.GroupKind:
					eligible = qlExactKeyEligible(item.Message())
				default: // Exact scalar map values remain in the wire key.
				}
				return eligible
			})
		case field.Kind() == protoreflect.MessageKind && field.IsList():
			list := value.List()
			for ordinal := 0; ordinal < list.Len() && eligible; ordinal++ {
				eligible = qlExactKeyEligible(list.Get(ordinal).Message())
			}
		case field.Kind() == protoreflect.MessageKind:
			eligible = qlExactKeyEligible(value.Message())
		}
		return eligible
	})
	return eligible
}
