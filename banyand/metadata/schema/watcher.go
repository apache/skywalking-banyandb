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

package schema

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

type watchEventHandler interface {
	OnAddOrUpdate(Metadata)
	OnDelete(Metadata)
}
type watcherConfig struct {
	key           string
	revision      int64
	kind          Kind
	checkInterval time.Duration
}

type cacheEntry struct {
	valueHash   uint64 // Hash of the value
	modRevision int64  // Last modified revision
}

type watcher struct {
	cli           *clientv3.Client
	closer        *run.Closer
	l             *logger.Logger
	ticker        *time.Ticker
	cache         map[string]cacheEntry
	key           string
	handlers      []watchEventHandler
	revision      int64
	kind          Kind
	checkInterval time.Duration
	mu            sync.RWMutex
	startOnce     sync.Once
}

func newWatcher(cli *clientv3.Client, wc watcherConfig, l *logger.Logger) *watcher {
	if wc.checkInterval == 0 {
		wc.checkInterval = 5 * time.Minute
	}
	w := &watcher{
		cli:           cli,
		key:           wc.key,
		kind:          wc.kind,
		revision:      wc.revision,
		closer:        run.NewCloser(1),
		l:             l,
		checkInterval: wc.checkInterval,
		cache:         make(map[string]cacheEntry),
	}
	return w
}

func (w *watcher) Start() {
	w.startOnce.Do(func() {
		if w.revision < 1 {
			w.periodicSync()
		}
		go w.watch(w.revision)
	})
}

func (w *watcher) AddHandler(handler watchEventHandler) {
	if w.handlers == nil {
		w.handlers = make([]watchEventHandler, 0)
	}
	w.handlers = append(w.handlers, handler)
}

func (w *watcher) Close() {
	w.closer.Done()
	w.closer.CloseThenWait()
}

func (w *watcher) watch(revision int64) {
	if !w.closer.AddRunning() {
		return
	}
	defer w.closer.Done()
	cli := w.cli

	w.ticker = time.NewTicker(w.checkInterval)
	defer w.ticker.Stop()

OUTER:
	for {
		select {
		case <-w.closer.CloseNotify():
			return
		default:
		}

		if revision < 0 {
			// Use periodic sync to recover state and get new revision
			w.periodicSync()
			revision = w.revision
			continue
		}

		wch := cli.Watch(w.closer.Ctx(), w.key,
			clientv3.WithPrefix(),
			clientv3.WithRev(revision+1),
			clientv3.WithPrevKV(),
		)
		if wch == nil {
			continue
		}

		for {
			select {
			case <-w.closer.CloseNotify():
				w.l.Info().Msg("watcher closed")
				return
			case <-w.ticker.C:
				w.periodicSync()
			case watchResp, ok := <-wch:
				if !ok {
					select {
					case <-w.closer.CloseNotify():
						return
					default:
						revision = -1
						continue OUTER
					}
				}
				if err := watchResp.Err(); err != nil {
					if errors.Is(err, v3rpc.ErrCompacted) {
						revision = -1
						continue OUTER
					}
					continue
				}
				w.revision = watchResp.Header.Revision
				for _, event := range watchResp.Events {
					w.handle(event, &watchResp)
				}
			}
		}
	}
}

func (w *watcher) handle(watchEvent *clientv3.Event, watchResp *clientv3.WatchResponse) {
	keyStr := string(watchEvent.Kv.Key)
	entry := cacheEntry{
		valueHash:   convert.Hash(watchEvent.Kv.Value),
		modRevision: watchEvent.Kv.ModRevision,
	}

	handlers := w.handlers
	if len(handlers) == 0 {
		w.l.Panic().Msg("no handlers registered")
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	switch watchEvent.Type {
	case mvccpb.PUT:
		if existing, exists := w.cache[keyStr]; !exists || existing.modRevision < entry.modRevision {
			w.cache[keyStr] = entry

			md, err := w.kind.Unmarshal(watchEvent.Kv)
			if err != nil {
				w.l.Error().Stringer("event_header", &watchResp.Header).AnErr("err", err).Msg("failed to unmarshal message")
				return
			}
			for i := range handlers {
				handlers[i].OnAddOrUpdate(md)
			}
		}
	case mvccpb.DELETE:
		delete(w.cache, keyStr)
		md, err := w.kind.Unmarshal(watchEvent.PrevKv)
		if err != nil {
			w.l.Error().Stringer("event_header", &watchResp.Header).AnErr("err", err).Msg("failed to unmarshal message")
			return
		}
		for i := range handlers {
			handlers[i].OnDelete(md)
		}
	}
}

func (w *watcher) periodicSync() {
	resp, err := w.cli.Get(w.closer.Ctx(), w.key, clientv3.WithPrefix())
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			w.l.Error().Err(err).Msg("periodic sync failed to fetch keys")
		}
		return
	}

	currentState := make(map[string]cacheEntry, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		currentState[string(kv.Key)] = cacheEntry{
			valueHash:   convert.Hash(kv.Value),
			modRevision: kv.ModRevision,
		}
	}

	handlers := w.handlers
	if len(handlers) == 0 {
		w.l.Panic().Msg("no handlers registered")
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	// Detect deletions and changes
	for cachedKey, cachedEntry := range w.cache {
		currentEntry, exists := currentState[cachedKey]
		if !exists {
			// Handle deletion
			delete(w.cache, cachedKey)
			if md, err := w.getFromStore(cachedKey); err == nil {
				for i := range handlers {
					handlers[i].OnDelete(*md)
				}
			}
			continue
		}

		if currentEntry.valueHash != cachedEntry.valueHash {
			// Handle update
			if md, err := w.getFromStore(cachedKey); err == nil {
				for i := range handlers {
					handlers[i].OnAddOrUpdate(*md)
				}
				w.cache[cachedKey] = currentEntry
			}
		}
	}

	// Detect additions
	for key, entry := range currentState {
		if _, exists := w.cache[key]; !exists {
			if md, err := w.getFromStore(key); err == nil {
				for i := range handlers {
					handlers[i].OnAddOrUpdate(*md)
				}
				w.cache[key] = entry
			}
		}
	}
}

func (w *watcher) getFromStore(key string) (*Metadata, error) {
	resp, err := w.cli.Get(w.closer.Ctx(), key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, errors.New("key not found")
	}
	md, err := w.kind.Unmarshal(resp.Kvs[0])
	return &md, err
}
