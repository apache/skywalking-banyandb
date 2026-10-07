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
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/index"
	idxmetrics "github.com/apache/skywalking-banyandb/pkg/index/metrics"
	"github.com/apache/skywalking-banyandb/pkg/index/nativeadapter"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type elementIndex struct {
	store    *nativeadapter.Store
	l        *logger.Logger
	metrics  *idxmetrics.Metrics
	location string
}

func newElementIndex(ctx context.Context, root string, flushTimeoutSeconds int64, idxMetrics *idxmetrics.Metrics, leases ...storage.RootLease) (*elementIndex, error) {
	if len(leases) != 1 || leases[0] == nil {
		return nil, errors.New("element index: exactly one root lease is required")
	}
	if err := leases[0].Validate(); err != nil {
		return nil, errors.WithMessage(err, "element index: validate root lease")
	}
	if err := leases[0].ValidatePath(path.Join(root, elementIndexFilename)); err != nil {
		return nil, errors.WithMessage(err, "element index: validate root lease path")
	}
	location := path.Join(root, elementIndexFilename)
	ei := &elementIndex{
		l:        logger.Fetch(ctx, "element_index"),
		location: location,
		metrics:  idxMetrics,
	}
	var err error
	// Native owner workers are database-lifetime goroutines; they are drained
	// by Close rather than inheriting this constructor's request context.
	//nolint:contextcheck // NewStore intentionally establishes owner lifetime.
	if ei.store, err = nativeadapter.NewStore(ei.location, leases[0], nativeadapter.SearcherOptions{
		MaxTerms:         nativeadapter.DefaultMaxTerms,
		AsyncPersistence: flushTimeoutSeconds > 0,
		PersistInterval:  time.Duration(flushTimeoutSeconds) * time.Second,
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
	return e.WriteContext(context.Background(), docs)
}

// WriteContext admits documents with ctx. Callers that have already stored
// the raw elements must pass a context without cancellation, so a canceled
// request cannot leave stored rows missing from the index; the owner still
// rejects writes once it is closing.
func (e *elementIndex) WriteContext(ctx context.Context, docs index.Documents) error {
	return e.store.Batch(ctx, index.Batch{
		Documents: docs,
	})
}

func (e *elementIndex) Search(ctx context.Context, seriesList []uint64, filter index.Filter, tr *index.RangeOpts) (posting.List, posting.List, error) {
	var result, resultTS posting.List
	searcher, err := e.store.NewSearcher(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = searcher.Close() }()
	for i, id := range seriesList {
		select {
		case <-ctx.Done():
			return nil, nil, errors.WithMessagef(ctx.Err(), "search series %d/%d", i, len(seriesList))
		default:
		}
		pl, plTS, err := filter.Execute(func(_ databasev1.IndexRule_Type) (index.Searcher, error) {
			return searcher, nil
		}, common.SeriesID(id), tr)
		if err != nil {
			return nil, nil, err
		}
		if pl == nil || pl.IsEmpty() {
			continue
		}
		if result == nil {
			result = pl
		} else {
			if err := result.Union(pl); err != nil {
				return nil, nil, err
			}
		}
		if resultTS == nil {
			resultTS = plTS
		} else {
			if err := resultTS.Union(plTS); err != nil {
				return nil, nil, err
			}
		}
	}
	return result, resultTS, nil
}

func (e *elementIndex) EnableExternalSegments() (index.ExternalSegmentStreamer, error) {
	return e.store.EnableExternalSegments()
}

func (e *elementIndex) Close() error {
	return e.store.Close()
}

func (e *elementIndex) collectMetrics(labelValues ...string) {
	if e == nil || e.metrics == nil || e.store == nil {
		return
	}
	dataCount, dataBytes := e.store.Stats()
	e.metrics.ObserveNative(dataCount, dataBytes, labelValues...)
}
