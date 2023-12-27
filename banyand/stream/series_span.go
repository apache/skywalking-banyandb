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

package stream

import (
	"context"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type seriesSpan struct {
	l             *logger.Logger
	timeRange     *timestamp.TimeRange
	series        string
	tableWrappers []storage.TSTableWrapper[*tsTable]
	seriesID      common.SeriesID
}

func (s *seriesSpan) Close() {
	for _, tw := range s.tableWrappers {
		tw.DecRef()
	}
}

func newSeriesSpan(ctx context.Context, timeRange *timestamp.TimeRange, tableWrappers []storage.TSTableWrapper[*tsTable], id common.SeriesID, series string) *seriesSpan {
	s := &seriesSpan{
		tableWrappers: tableWrappers,
		seriesID:      id,
		series:        series,
		timeRange:     timeRange,
	}
	parentLogger := ctx.Value(logger.ContextKey)
	if pl, ok := parentLogger.(*logger.Logger); ok {
		s.l = pl.Named("series_span")
	} else {
		s.l = logger.GetLogger("series_span")
	}
	return s
}
