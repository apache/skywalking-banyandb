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

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

type elementIndex struct {
	store index.ElementStore
	l     *logger.Logger
}

func newElementIndex(ctx context.Context, root string) (*elementIndex, error) {
	ei := &elementIndex{
		l: logger.Fetch(ctx, "element_index"),
	}
	var err error
	if ei.store, err = inverted.NewStore(inverted.StoreOpts{
		Path:   path.Join(root, "element_idx"),
		Logger: ei.l,
	}); err != nil {
		return nil, err
	}
	return ei, nil
}

func (e *elementIndex) Iterator(fieldKey index.FieldKey, termRange index.RangeOpts, order modelv1.Sort) (index.FieldIterator, error) {
	iter, err := e.store.Iterator(fieldKey, termRange, order)
	if err != nil {
		return nil, err
	}
	return iter, nil
}

func (e *elementIndex) Write(docs index.Documents) error {
	return e.store.Batch(docs)
}

func (e *elementIndex) Search(_ context.Context, seriesList pbv1.SeriesList, filter index.Filter) (posting.List, error) {
	var pls posting.List
	for _, series := range seriesList {
		pl, err := filter.Execute(func(ruleType databasev1.IndexRule_Type) (index.Searcher, error) {
			return e.store, nil
		}, series.ID)
		if err != nil {
			return nil, err
		}
		err = pls.Union(pl)
		if err != nil {
			return nil, err
		}
	}
	return pls, nil
}

func (e *elementIndex) Close() error {
	return e.store.Close()
}
