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

package registry

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	metaschema "github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

// fakeRevRepo is a minimal RevisionRepository for registry unit tests.
type fakeRevRepo struct {
	resources map[string]map[string]int64
	latest    int64
}

func newFakeRevRepo(latest int64) *fakeRevRepo {
	return &fakeRevRepo{
		latest:    latest,
		resources: make(map[string]map[string]int64),
	}
}

func (f *fakeRevRepo) put(group, name string, rev int64) {
	if f.resources[group] == nil {
		f.resources[group] = make(map[string]int64)
	}
	f.resources[group][name] = rev
}

func (f *fakeRevRepo) LatestModRevision() int64 {
	return f.latest
}

func (f *fakeRevRepo) ResourceRevision(_ metaschema.Kind, group, name string) (int64, bool) {
	if g := f.resources[group]; g != nil {
		if rev, ok := g[name]; ok {
			return rev, true
		}
	}
	return 0, false
}

func (f *fakeRevRepo) IsAbsent(kind metaschema.Kind, group, name string) bool {
	_, ok := f.ResourceRevision(kind, group, name)
	return !ok
}

func TestNodeRepoRegistry_EmptyReturnsZero(t *testing.T) {
	r := NewNodeRepoRegistry()
	assert.Equal(t, int64(0), r.LatestModRevision())
	rev, ok := r.ResourceRevision(metaschema.KindStream, "g", "n")
	assert.Equal(t, int64(0), rev)
	assert.False(t, ok)
	assert.True(t, r.IsAbsent(metaschema.KindStream, "g", "n"))
	assert.False(t, r.HasKind(metaschema.KindStream))
}

func TestNodeRepoRegistry_NilRepoOrZeroKindIsNoop(t *testing.T) {
	r := NewNodeRepoRegistry()
	r.Register(metaschema.KindStream, nil)
	r.Register(0, newFakeRevRepo(5))
	assert.Equal(t, int64(0), r.LatestModRevision())
}

func TestNodeRepoRegistry_LatestModRevisionTakesMin(t *testing.T) {
	r := NewNodeRepoRegistry()
	r.Register(metaschema.KindMeasure|metaschema.KindIndexRule, newFakeRevRepo(10))
	r.Register(metaschema.KindStream, newFakeRevRepo(7))
	r.Register(metaschema.KindTrace, newFakeRevRepo(15))

	assert.Equal(t, int64(7), r.LatestModRevision(), "min over registered repos wins")
}

func TestNodeRepoRegistry_ResourceRevisionRoutesByKind(t *testing.T) {
	measureRepo := newFakeRevRepo(10)
	measureRepo.put("g1", "m1", 9)
	streamRepo := newFakeRevRepo(7)
	streamRepo.put("g1", "s1", 5)

	r := NewNodeRepoRegistry()
	r.Register(metaschema.KindMeasure, measureRepo)
	r.Register(metaschema.KindStream, streamRepo)

	rev, ok := r.ResourceRevision(metaschema.KindMeasure, "g1", "m1")
	assert.True(t, ok)
	assert.Equal(t, int64(9), rev)

	rev, ok = r.ResourceRevision(metaschema.KindStream, "g1", "s1")
	assert.True(t, ok)
	assert.Equal(t, int64(5), rev)

	// Wrong-kind lookup returns absent even though the name matches a
	// resource on a sibling repo — barrier semantics: a Stream key never
	// satisfies via the measure repo.
	_, ok = r.ResourceRevision(metaschema.KindStream, "g1", "m1")
	assert.False(t, ok)
}

func TestNodeRepoRegistry_GroupKindFanOut(t *testing.T) {
	measureRepo := newFakeRevRepo(10)
	measureRepo.put("measure-only", "measure-only", 8)
	streamRepo := newFakeRevRepo(7)
	streamRepo.put("stream-only", "stream-only", 6)

	r := NewNodeRepoRegistry()
	r.Register(metaschema.KindGroup|metaschema.KindMeasure, measureRepo)
	r.Register(metaschema.KindGroup|metaschema.KindStream, streamRepo)

	rev, ok := r.ResourceRevision(metaschema.KindGroup, "stream-only", "stream-only")
	assert.True(t, ok)
	assert.Equal(t, int64(6), rev)

	rev, ok = r.ResourceRevision(metaschema.KindGroup, "measure-only", "measure-only")
	assert.True(t, ok)
	assert.Equal(t, int64(8), rev)

	assert.True(t, r.IsAbsent(metaschema.KindGroup, "ghost", "ghost"))
}

func TestNodeRepoRegistry_DuplicateRegistrationIsIdempotent(t *testing.T) {
	repo := newFakeRevRepo(4)
	r := NewNodeRepoRegistry()
	r.Register(metaschema.KindStream, repo)
	r.Register(metaschema.KindStream, repo)
	r.Register(metaschema.KindStream|metaschema.KindIndexRule, repo)

	assert.Equal(t, int64(4), r.LatestModRevision())
	assert.True(t, r.HasKind(metaschema.KindStream))
	assert.True(t, r.HasKind(metaschema.KindIndexRule))
	assert.False(t, r.HasKind(metaschema.KindMeasure))
}

func TestNodeRepoRegistry_HasKind(t *testing.T) {
	r := NewNodeRepoRegistry()
	r.Register(metaschema.KindMeasure|metaschema.KindIndexRule|metaschema.KindIndexRuleBinding|metaschema.KindGroup, newFakeRevRepo(1))

	assert.True(t, r.HasKind(metaschema.KindMeasure))
	assert.True(t, r.HasKind(metaschema.KindIndexRule))
	assert.True(t, r.HasKind(metaschema.KindIndexRuleBinding))
	assert.True(t, r.HasKind(metaschema.KindGroup))
	assert.False(t, r.HasKind(metaschema.KindTopNAggregation), "TopN routes via schemaCache, not the registry")
	assert.False(t, r.HasKind(metaschema.KindProperty), "Property routes via schemaCache, not the registry")
}

func TestNodeRepoRegistry_ConcurrentRegisterAndLookup(t *testing.T) {
	r := NewNodeRepoRegistry()
	repos := []*fakeRevRepo{newFakeRevRepo(1), newFakeRevRepo(2), newFakeRevRepo(3), newFakeRevRepo(4)}
	for _, repo := range repos {
		repo.put("g", "n", 10)
	}

	var wg sync.WaitGroup
	wg.Add(len(repos))
	for i, repo := range repos {
		go func(idx int, rp *fakeRevRepo) {
			defer wg.Done()
			kinds := metaschema.KindMeasure
			if idx%2 == 0 {
				kinds = metaschema.KindStream
			}
			r.Register(kinds, rp)
		}(i, repo)
	}

	done := make(chan struct{})
	go func() {
		for range 1000 {
			_ = r.LatestModRevision()
			_, _ = r.ResourceRevision(metaschema.KindStream, "g", "n")
			_ = r.HasKind(metaschema.KindMeasure)
		}
		close(done)
	}()

	wg.Wait()
	<-done

	assert.Equal(t, int64(1), r.LatestModRevision())
	assert.True(t, r.HasKind(metaschema.KindStream))
	assert.True(t, r.HasKind(metaschema.KindMeasure))
}
