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
	"path"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type elementIndex struct {
	store index.Store
	l     *logger.Logger
}

func newElementIndex(ctx context.Context, root string, flushTimeoutSeconds int64, metrics *inverted.Metrics) (*elementIndex, error) {
	ei := &elementIndex{
		l: logger.Fetch(ctx, "element_index"),
	}
	var err error
	if ei.store, err = inverted.NewStore(inverted.StoreOpts{
		Path:         path.Join(root, elementIndexFilename),
		Logger:       ei.l,
		BatchWaitSec: flushTimeoutSeconds,
		Metrics:      metrics,
	}); err != nil {
		return nil, err
	}
	return ei, nil
}

func (e *elementIndex) Sort(ctx context.Context, sids []common.SeriesID, fieldKey index.FieldKey, order modelv1.Sort,
	timeRange *timestamp.TimeRange, preloadSize int,
) (index.FieldIterator[*index.DocumentResult], error) {
	iter, err := e.store.Sort(ctx, sids, fieldKey, order, timeRange, preloadSize)
	if err != nil {
		return nil, err
	}
	return iter, nil
}

func (e *elementIndex) Write(docs index.Documents) error {
	return e.store.Batch(index.Batch{
		Documents: docs,
	})
}

func (e *elementIndex) Search(ctx context.Context, seriesList pbv1.SeriesList, filter index.Filter) (posting.List, error) {
	var result posting.List
	for i, series := range seriesList {
		select {
		case <-ctx.Done():
			return nil, errors.WithMessagef(ctx.Err(), "search series %d/%d", i, len(seriesList))
		default:
		}
		pl, err := filter.Execute(func(_ databasev1.IndexRule_Type) (index.Searcher, error) {
			return e.store, nil
		}, series.ID)
		if err != nil {
			return nil, err
		}
		if pl == nil || pl.IsEmpty() {
			continue
		}
		if result == nil {
			result = pl
		} else {
			if err := result.Union(pl); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func (e *elementIndex) Close() error {
	return e.store.Close()
}

func (e *elementIndex) collectMetrics(labelValues ...string) {
	e.store.CollectMetrics(labelValues...)
}
