// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses
// this file to you under the Apache License, Version 2.0 (the "License"); you
// may not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package nativeadapter

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// DefaultMaxTerms preserves the existing product contract for fields whose
// dictionaries exceed a caller-selected budget. Callers that need a bounded
// expansion must set SearcherOptions.MaxTerms explicitly; the production
// default does not silently reject valid high-cardinality fields.
const DefaultMaxTerms uint64 = ^uint64(0)

// RootLease is the database-owned capability required for native publication.
type RootLease interface {
	Validate() error
	ValidatePath(string) error
}

// Store is the narrow production owner seam. It intentionally does not
// implement index.Store: query ownership is explicit through NewSearcher and
// iterators retain their pinned view until Close.
type Store struct {
	Owner   *native.Owner
	options SearcherOptions
}

// NewStore creates a native owner under a database-owned lease.
func NewStore(path string, lease RootLease, options SearcherOptions) (*Store, error) {
	if lease == nil {
		return nil, fmt.Errorf("native adapter: root lease is required")
	}
	if options.MaxTerms == 0 {
		return nil, fmt.Errorf("native adapter: MaxTerms is required: %w", native.ErrQueryLimit)
	}
	ownerOptions := native.OwnerOptions{Lease: lease, Path: path}
	if options.AsyncPersistence {
		ownerOptions.PersistInterval = options.PersistInterval
	}
	owner, err := native.NewOwner(ownerOptions)
	if err != nil {
		return nil, err
	}
	return &Store{Owner: owner, options: options}, nil
}

// Batch writes one native batch while preserving product document semantics.
func (s *Store) Batch(ctx context.Context, batch index.Batch) error {
	if s == nil || s.Owner == nil {
		return native.ErrOwnerClosed
	}
	if s.options.AsyncPersistence {
		return (&Adapter{Owner: s.Owner}).Batch(ctx, batch)
	}
	completed := make(chan error, 1)
	callback := batch.PersistentCallback
	batch.PersistentCallback = func(err error) {
		// Signal durability before entering caller code. A callback may block,
		// re-enter Store, or panic; none of those should strand the synchronous
		// Batch waiter after the owner has completed persistence.
		completed <- err
		if callback != nil {
			callback(err)
		}
	}
	if err := (&Adapter{Owner: s.Owner}).Batch(ctx, batch); err != nil {
		return err
	}
	select {
	case err := <-completed:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// NewSearcher pins one immutable view for the request.
func (s *Store) NewSearcher(ctx context.Context) (*Searcher, error) {
	if s == nil || s.Owner == nil {
		return nil, native.ErrOwnerClosed
	}
	return (&Adapter{Owner: s.Owner}).NewSearcher(ctx, s.options)
}

// Sort creates an iterator which owns the request searcher until Close.
func (s *Store) Sort(ctx context.Context, seriesIDs []common.SeriesID, fieldKey index.FieldKey,
	order modelv1.Sort, timeRange *timestamp.TimeRange, pageSize int,
) (index.FieldIterator[*index.DocumentResult], error) {
	searcher, err := s.NewSearcher(ctx)
	if err != nil {
		return nil, err
	}
	iterator, err := searcher.Sort(ctx, seriesIDs, fieldKey, order, timeRange, pageSize)
	if err != nil {
		_ = searcher.Close()
		return nil, err
	}
	return &ownedIterator{FieldIterator: iterator, searcher: searcher}, nil
}

// EnableExternalSegments forwards the native staged-segment capability.
func (s *Store) EnableExternalSegments() (index.ExternalSegmentStreamer, error) {
	if s == nil || s.Owner == nil {
		return nil, native.ErrOwnerClosed
	}
	return s.Owner.EnableExternalSegments()
}

// TakeFileSnapshot copies the current immutable native root.
func (s *Store) TakeFileSnapshot(destination string) error {
	if s == nil || s.Owner == nil {
		return native.ErrOwnerClosed
	}
	return s.Owner.TakeFileSnapshot(destination)
}

// Stats returns current live document count and native bytes.
func (s *Store) Stats() (int64, int64) {
	if s == nil || s.Owner == nil {
		return 0, 0
	}
	return s.Owner.Stats()
}

// Close drains native persistence and releases the owner lease reference.
func (s *Store) Close() error {
	if s == nil || s.Owner == nil {
		return nil
	}
	return s.Owner.Close()
}

type ownedIterator struct {
	index.FieldIterator[*index.DocumentResult]
	searcher *Searcher
}

func (i *ownedIterator) Close() error {
	if i == nil {
		return nil
	}
	return errors.Join(i.FieldIterator.Close(), i.searcher.Close())
}
