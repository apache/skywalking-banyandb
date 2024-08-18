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
	"context"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/meter/native"
)

var measureScope = observability.RootScope.SubScope("measure")

type metrics struct {
	totalWritten meter.Counter
}

func (m *metrics) DeleteAll() {
	m.totalWritten.Delete()
}

func (s *supplier) newMetrics(p common.Position) storage.Metrics {
	factory := s.omr.With(measureScope.ConstLabels(meter.LabelPairs{"group": p.Database}))
	return &metrics{
		totalWritten: factory.NewCounter("total_written"),
	}
}

func (s *service) createNativeObservabilityGroup(ctx context.Context) error {
	if !s.omr.NativeEnabled() {
		return nil
	}
	g := &commonv1.Group{
		Metadata: &commonv1.Metadata{
			Name: native.ObservabilityGroupName,
		},
		Catalog: commonv1.Catalog_CATALOG_MEASURE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum: 1,
			SegmentInterval: &commonv1.IntervalRule{
				Unit: commonv1.IntervalRule_UNIT_DAY,
				Num:  1,
			},
			Ttl: &commonv1.IntervalRule{
				Unit: commonv1.IntervalRule_UNIT_DAY,
				Num:  1,
			},
		},
	}
	if err := s.metadata.GroupRegistry().CreateGroup(ctx, g); err != nil &&
		!errors.Is(err, schema.ErrGRPCAlreadyExists) {
		return errors.WithMessage(err, "failed to create native observability group")
	}
	return nil
}
