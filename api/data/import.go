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
	"github.com/apache/skywalking-banyandb/api/common"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

// The import topics carry the transfer.v1.ImportWrite frames of a data import, one topic per
// catalog. The receiving node admits their rows at the rate the import asks for, then writes
// them as the catalog's write topic would.
var (
	// StreamImportKindVersion is the version tag of stream import kind.
	StreamImportKindVersion = common.KindVersion{Version: "v1", Kind: "stream-import"}
	// TopicStreamImport is the stream import topic.
	TopicStreamImport = bus.BiTopic(StreamImportKindVersion.String())
	// MeasureImportKindVersion is the version tag of measure import kind.
	MeasureImportKindVersion = common.KindVersion{Version: "v1", Kind: "measure-import"}
	// TopicMeasureImport is the measure import topic.
	TopicMeasureImport = bus.BiTopic(MeasureImportKindVersion.String())
	// TraceImportKindVersion is the version tag of trace import kind.
	TraceImportKindVersion = common.KindVersion{Version: "v1", Kind: "trace-import"}
	// TopicTraceImport is the trace import topic.
	TopicTraceImport = bus.BiTopic(TraceImportKindVersion.String())
	// PropertyImportKindVersion is the version tag of property import kind.
	PropertyImportKindVersion = common.KindVersion{Version: "v1", Kind: "property-import"}
	// TopicPropertyImport is the property import topic.
	TopicPropertyImport = bus.BiTopic(PropertyImportKindVersion.String())
)

// ImportedMeasureWrite marks a measure write of a data import. The write path stores the point
// but keeps it out of Top-N aggregation: the import writes _top_n_result rows verbatim, and
// recomputing them would overwrite them.
type ImportedMeasureWrite struct {
	*measurev1.InternalWriteRequest
}
