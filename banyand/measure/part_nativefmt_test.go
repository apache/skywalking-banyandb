// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package measure

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/test"
	transfertest "github.com/apache/skywalking-banyandb/pkg/test/transfer"
)

// TestNativeSyncFileNameMatchesReceiver pins nativefmt's disk-to-wire table to the names this
// engine writes on disk and streams to its chunked sync receiver.
func TestNativeSyncFileNameMatchesReceiver(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	partDir := filepath.Join(tmpPath, "part")
	buildAndFlushMeasureMemPart(t, fileSystem, makeTestDataPoints(1_000_000_000), partDir)
	files, release := CreatePartFileReaderFromPath(partDir, fileSystem)
	defer release()
	transfertest.RequireNativePartNames(t, commonv1.Catalog_CATALOG_MEASURE, partDir, files, metadataFilename, seriesMetadataFilename)
}
