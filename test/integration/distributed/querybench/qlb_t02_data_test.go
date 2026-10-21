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

package querybench

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	testmeasure "github.com/apache/skywalking-banyandb/pkg/test/measure"
)

// Read every service partition before timing. A readiness check must not require
// one 100,000-row protobuf response that exceeds the execution memory allowance.
func waitForQLBT02Visibility(ctx context.Context, conn *grpc.ClientConn, cardinality int, base time.Time, wide bool) error {
	request, err := buildScenarioQuery(ScenarioScanAll, cardinality, base)
	if err != nil {
		return err
	}
	request.TagProjection = tagProjection(benchTagEntityID)
	field := benchFieldValue
	if wide {
		request.Name = qlbT02WideMeasure
		field = qlbT02WideFields()[0]
	}
	request.FieldProjection = &measurev1.QueryRequest_FieldProjection{Names: []string{field}}
	entities, points := splitCardinality(cardinality)
	for service := 0; service < min(benchServiceMod, entities); service++ {
		request.Criteria = &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
			Name: benchTagID, Op: modelv1.Condition_BINARY_OP_EQ, Value: strTagValue(fmt.Sprintf("svc%d", service)),
		}}}
		rows := ((entities-1-service)/benchServiceMod + 1) * points
		if err = waitForScenarioVisibility(ctx, conn, request, ScenarioScanAll, rows); err != nil {
			return fmt.Errorf("%s service %d visibility: %w", request.Name, service, err)
		}
	}
	return nil
}

func preloadQLBT02Schema(ctx context.Context, registry schema.Registry) error {
	if err := testmeasure.PreloadSchema(ctx, registry); err != nil {
		return err
	}
	original, err := registry.GetMeasure(ctx, &commonv1.Metadata{Name: benchMeasureName, Group: benchGroupName})
	if err != nil {
		return err
	}
	original = proto.Clone(original).(*databasev1.Measure)
	original.Metadata = &commonv1.Metadata{Name: qlbT02WideMeasure, Group: benchGroupName}
	original.Fields = nil
	for _, name := range qlbT02WideFields() {
		original.Fields = append(original.Fields, &databasev1.FieldSpec{
			Name: name, FieldType: databasev1.FieldType_FIELD_TYPE_INT,
			EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		})
	}
	if _, err = registry.CreateMeasure(ctx, original); err != nil {
		return err
	}
	binding, err := registry.GetIndexRuleBinding(ctx, &commonv1.Metadata{Name: benchMeasureName, Group: benchGroupName})
	if err != nil {
		return err
	}
	binding = proto.Clone(binding).(*databasev1.IndexRuleBinding)
	binding.Metadata = &commonv1.Metadata{Name: qlbT02WideMeasure, Group: benchGroupName}
	binding.Subject.Name = qlbT02WideMeasure
	_, err = registry.CreateIndexRuleBinding(ctx, binding)
	return err
}
