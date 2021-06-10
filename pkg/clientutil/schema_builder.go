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

package clientutil

import (
	flatbuffers "github.com/google/flatbuffers/go"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

func BuildTraceSeries(group, name string) *apiv1.Series {
	builder := flatbuffers.NewBuilder(0)
	g, n := builder.CreateString(group), builder.CreateString(name)
	apiv1.MetadataStart(builder)
	apiv1.MetadataAddGroup(builder, g)
	apiv1.MetadataAddName(builder, n)
	meta := apiv1.MetadataEnd(builder)

	apiv1.SeriesStart(builder)
	apiv1.SeriesAddCatalog(builder, apiv1.CatalogTrace)
	apiv1.SeriesAddSeries(builder, meta)
	seriesOffset := apiv1.SeriesEnd(builder)
	builder.Finish(seriesOffset)
	buf := builder.Bytes[builder.Head():]
	return apiv1.GetRootAsSeries(buf, 0)
}
