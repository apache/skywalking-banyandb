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

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// IndexDB is the interface of index database.
type IndexDB interface {
	Write(docs index.Documents) error
	Search(ctx context.Context, seriesList *pbv1.SeriesList, filter index.Filter) (posting.List, error)
}

type elementIndex struct {
	store index.SeriesStore
	// l     *logger.Logger
}

func (s *elementIndex) Write(docs index.Documents) error {
	return s.store.Batch(docs)
}

func (s *elementIndex) Search(_ context.Context, _ *pbv1.SeriesList, filter index.Filter) (posting.List, error) {
	plFilter, err := filter.Execute(func(ruleType databasev1.IndexRule_Type) (index.Searcher, error) {
		return s.store, nil
	}, 0)
	if err != nil {
		return nil, err
	}
	return plFilter, nil
}

func (s *elementIndex) Close() error {
	return s.store.Close()
}
