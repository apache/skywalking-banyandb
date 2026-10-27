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

package importer

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

// writeSchemalessArtifact writes the data fixture as an export without its schema: no schema
// files, only the groups in the manifest.
func writeSchemalessArtifact(t *testing.T) string {
	t.Helper()
	dir := writeDataArtifact(t)
	path := filepath.Join(dir, manifest.FileName)
	m, err := manifest.Read(path)
	require.NoError(t, err)
	m.SchemaFiles = nil
	m.Groups = []manifest.Group{{Name: "sw_metric", Catalog: "MEASURE"}, {Name: "sw_prop", Catalog: "PROPERTY"}, {Name: "sw_stream", Catalog: "STREAM"}}
	require.NoError(t, os.RemoveAll(filepath.Join(dir, schemasync.Dir)))
	require.NoError(t, manifest.Write(path, m))
	return dir
}

func TestOpenArtifactWithoutSchemaKnowsOnlyItsGroups(t *testing.T) {
	a, err := OpenArtifact(context.Background(), writeSchemalessArtifact(t), true)
	require.NoError(t, err)
	assert.False(t, a.HasSchema())
	assert.Equal(t, commonv1.Catalog_CATALOG_MEASURE, a.Groups["sw_metric"].GetCatalog())
	assert.Equal(t, commonv1.Catalog_CATALOG_STREAM, a.Groups["sw_stream"].GetCatalog())
	assert.Empty(t, a.Measures)
}

func TestImportWithoutArtifactSchemaReadsColumnsWithTheTargetSchema(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	mirrorFake(f, dataSchemas())
	run := runImport(t, f, li, defaultImport(t, writeSchemalessArtifact(t)))
	require.NoError(t, run.err, run.out+run.log)
	assert.Contains(t, run.log, "carries no schema; its CSV columns are read with the target's schema")
	assert.Empty(t, f.writeLog(), "an artifact without schema writes no schema")
	var stream, measure, prop int
	for _, b := range f.receivedBatches() {
		stream += len(b.GetStream().GetRows())
		measure += len(b.GetMeasure().GetRows())
		prop += len(b.GetProperty().GetRows())
	}
	assert.Equal(t, []int{3, 2, 2}, []int{stream, measure, prop})
}

func TestImportReportsWhereTheSchemaIsReadFrom(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	run := dryRun(t, f, writeDataArtifact(t), li)
	require.NoError(t, run.err, run.out)
	assert.Contains(t, run.log, "info: the schema is read from the 8 schema file(s) of artifact")
}

func TestImportWithoutArtifactSchemaRefusesColumnsTheTargetLacks(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	target := dataSchemas()
	cpm := target.Measures[ResourceKey{"sw_metric", "cpm"}]
	cpm.TagFamilies[0].Tags = append(cpm.TagFamilies[0].Tags, &databasev1.TagSpec{Name: "added", Type: databasev1.TagType_TAG_TYPE_STRING})
	mirrorFake(f, target)
	run := dryRun(t, f, writeSchemalessArtifact(t), li)
	assert.Equal(t, exitcode.ExitPreflight, run.code(), "%v\n%s", run.err, run.out)
	assert.Contains(t, run.out, "its columns differ from the target's schema, which reads an artifact exported without its schema")
	assert.Empty(t, f.receivedBatches())
}

func TestImportWithoutArtifactSchemaRefusesAGroupTheTargetLacks(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	run := dryRun(t, f, writeSchemalessArtifact(t), li)
	assertRefusedUnwritten(t, f, run)
	assert.Contains(t, run.out, "of group(s) sw_metric, sw_prop, sw_stream do not exist on the target, and the import does not create them")
}

func TestSchemaOnlyImportNeedsAnArtifactSchema(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := defaultImport(t, writeSchemalessArtifact(t))
	opts.SchemaOnly = true
	run := runImport(t, f, li, opts)
	assert.Equal(t, exitcode.ExitUsage, run.code(), "%v", run.err)
	assert.ErrorContains(t, run.err, "--schema-only needs a schema")
}
