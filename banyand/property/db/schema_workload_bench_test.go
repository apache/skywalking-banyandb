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

package db

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// The schema server stores every schema object as a property in one group,
// tagged like banyand/metadata/schema/property.SchemaToProperty does.
const (
	benchSchemaGroup = "_schema"
	benchSchemaKind  = "measure"
	benchPreload     = 3000
)

var benchMetricGroups = []string{"sw_metricsMinute", "sw_metricsHour", "sw_metricsDay"}

func benchSchemaProperty(index int) *propertyv1.Property {
	group := benchMetricGroups[index%len(benchMetricGroups)]
	name := fmt.Sprintf("service_metric_%06d", index)
	source := fmt.Sprintf(`{"metadata":{"group":%q,"name":%q},"tagFamilies":[{"name":"default","tags":[{"name":"entity_id","type":"TAG_TYPE_STRING"}]}],`+
		`"fields":[{"name":"value","fieldType":"FIELD_TYPE_INT","encodingMethod":"ENCODING_METHOD_GORILLA"}],"entity":{"tagNames":["entity_id"]},"interval":"1m","%s"}`,
		group, name, strings.Repeat("x", 256))
	str := func(value string) *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}}
	}
	revision := int64(index + 1)
	return &propertyv1.Property{
		Metadata: &commonv1.Metadata{Group: benchSchemaGroup, Name: benchSchemaKind, ModRevision: revision},
		Id:       benchSchemaKind + "_" + group + "/" + name,
		Tags: []*modelv1.Tag{
			{Key: "group", Value: str(group)},
			{Key: "name", Value: str(name)},
			{Key: "source", Value: str(source)},
			{Key: "kind", Value: str(benchSchemaKind)},
			{Key: "updated_at", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: revision}}}},
		},
	}
}

func openBenchSchemaDB(b *testing.B, nativeWriter bool) (Database, func()) {
	location, cleanup, err := test.NewSpace()
	if err != nil {
		b.Fatal(err)
	}
	opened, err := OpenDB(context.Background(), Config{
		Location: location, MetricsScopeName: fmt.Sprintf("bench_schema_%t_%d", nativeWriter, time.Now().UnixNano()),
		FlushInterval: 5 * time.Second, ExpireToDeleteDuration: time.Hour,
		Repair: RepairConfig{
			Enabled: true, Location: filepath.Join(location, "repair"), TreeSlotCount: 32,
			BuildTreeCron: "@every 1h", QuickBuildTreeTime: 10 * time.Minute,
		},
		Index: IndexConfig{BatchWaitSec: 5, NativeWriter: nativeWriter},
	}, observability.BypassRegistry, fs.NewLocalFileSystem())
	if err != nil {
		cleanup()
		b.Fatal(err)
	}
	return opened, func() {
		_ = opened.Close()
		cleanup()
	}
}

// insertLikeSchemaServer mirrors schemaManagementServer.InsertSchema: an
// existence check by id, then a single-document update.
func insertLikeSchemaServer(ctx context.Context, database Database, property *propertyv1.Property) error {
	existing, err := database.Query(ctx, &propertyv1.QueryRequest{
		Groups: []string{benchSchemaGroup}, Name: property.Metadata.Name, Ids: []string{property.Id},
	})
	if err != nil {
		return err
	}
	for _, result := range existing {
		if result.DeleteTime() == 0 {
			return fmt.Errorf("schema %s already exists", property.Id)
		}
	}
	return database.Update(ctx, 0, GetPropertyID(property), property)
}

func benchEngines(b *testing.B, run func(b *testing.B, database Database)) {
	for _, engine := range []struct {
		name   string
		native bool
	}{{"legacy", false}, {"native", true}} {
		b.Run(engine.name, func(b *testing.B) {
			database, cleanup := openBenchSchemaDB(b, engine.native)
			defer cleanup()
			ctx := context.Background()
			for index := 0; index < benchPreload; index++ {
				if err := insertLikeSchemaServer(ctx, database, benchSchemaProperty(index)); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			run(b, database)
		})
	}
}

func BenchmarkSchemaWorkloadInsert(b *testing.B) {
	benchEngines(b, func(b *testing.B, database Database) {
		ctx := context.Background()
		for index := 0; index < b.N; index++ {
			if err := insertLikeSchemaServer(ctx, database, benchSchemaProperty(benchPreload+index)); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSchemaWorkloadGetByID(b *testing.B) {
	benchEngines(b, func(b *testing.B, database Database) {
		// Let background merging of the preload finish so this measures
		// lookups against a settled store, not against a compaction backlog.
		b.StopTimer()
		time.Sleep(3 * time.Second)
		b.StartTimer()
		ctx := context.Background()
		for index := 0; index < b.N; index++ {
			property := benchSchemaProperty(index % benchPreload)
			results, err := database.Query(ctx, &propertyv1.QueryRequest{
				Groups: []string{benchSchemaGroup}, Name: benchSchemaKind, Ids: []string{property.Id},
			})
			if err != nil || len(results) != 1 {
				b.Fatalf("get %s = %d results, %v", property.Id, len(results), err)
			}
		}
	})
}
