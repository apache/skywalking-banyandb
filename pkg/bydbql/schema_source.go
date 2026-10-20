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
	"context"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
)

// SchemaSource resolves resource schemas from a local, watch-fed cache, such as
// the liaison's entity repository. A miss reports false and the Transformer falls
// back to the schema registry, which covers the window before a newly created
// resource's watch event arrives.
//
// Returned schemas are shared with the cache and must be treated as read-only.
type SchemaSource interface {
	Measure(group, name string) (*databasev1.Measure, bool)
	Stream(group, name string) (*databasev1.Stream, bool)
	Trace(group, name string) (*databasev1.Trace, bool)
}

// NewTransformerWithSource creates a Transformer that reads measure, stream and
// trace schemas from source first and falls back to registry on a miss. Reading
// locally avoids the registry's per-call fan-out to every metadata node.
func NewTransformerWithSource(registry metadata.Repo, source SchemaSource) *Transformer {
	return &Transformer{
		schemaRegistry: registry,
		schemaSource:   source,
	}
}

func (t *Transformer) getMeasure(ctx context.Context, md *commonv1.Metadata) (*databasev1.Measure, error) {
	if t.schemaSource != nil {
		if m, ok := t.schemaSource.Measure(md.GetGroup(), md.GetName()); ok {
			return m, nil
		}
	}
	return t.schemaRegistry.MeasureRegistry().GetMeasure(ctx, md)
}

func (t *Transformer) getStream(ctx context.Context, md *commonv1.Metadata) (*databasev1.Stream, error) {
	if t.schemaSource != nil {
		if s, ok := t.schemaSource.Stream(md.GetGroup(), md.GetName()); ok {
			return s, nil
		}
	}
	return t.schemaRegistry.StreamRegistry().GetStream(ctx, md)
}

func (t *Transformer) getTrace(ctx context.Context, md *commonv1.Metadata) (*databasev1.Trace, error) {
	if t.schemaSource != nil {
		if tr, ok := t.schemaSource.Trace(md.GetGroup(), md.GetName()); ok {
			return tr, nil
		}
	}
	return t.schemaRegistry.TraceRegistry().GetTrace(ctx, md)
}
