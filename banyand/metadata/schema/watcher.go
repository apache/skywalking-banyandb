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
	"errors"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

type watchEventHandler interface {
	OnAddOrUpdate(Metadata)
	OnDelete(Metadata)
}
type watcherConfig struct {
	handler  watchEventHandler
	key      string
	revision int64
	kind     Kind
}

type watcher struct {
	handler  watchEventHandler
	cli      *clientv3.Client
	closer   *run.Closer
	l        *logger.Logger
	key      string
	revision int64
	kind     Kind
}

func newWatcher(cli *clientv3.Client, wc watcherConfig, l *logger.Logger) *watcher {
	w := &watcher{
		cli:      cli,
		key:      wc.key,
		kind:     wc.kind,
		handler:  wc.handler,
		revision: wc.revision,
		closer:   run.NewCloser(1),
		l:        l,
	}
	revision := w.revision
	if revision < 1 {
		revision = w.allEvents()
	}
	go w.watch(revision)
	return w
}

func (w *watcher) Close() error {
	w.closer.Done()
	w.closer.CloseThenWait()
	return nil
}

func (w *watcher) allEvents() int64 {
	cli := w.cli
	var resp *clientv3.GetResponse
	start := time.Now()
	var eventHandleTime time.Duration
	var eventSize int
	for {
		var err error
		if resp, err = cli.Get(w.closer.Ctx(), w.key, clientv3.WithPrefix()); err == nil {
			startHandle := time.Now()
			eventSize = len(resp.Kvs)
			w.handleAllEvents(resp.Kvs)
			eventHandleTime = time.Since(startHandle)
			break
		}
		select {
		case <-w.closer.CloseNotify():
			return -1
		case <-time.After(1 * time.Second):
		}
	}
	w.l.Info().Dur("event_handle_time", eventHandleTime).Dur("total_time", time.Since(start)).
		Int("event_size", eventSize).Str("key", w.key).Msg("watcher all events")
	return resp.Header.Revision
}

func (w *watcher) watch(revision int64) {
	if !w.closer.AddRunning() {
		return
	}
	defer w.closer.Done()
	cli := w.cli
	for {
		if revision < 0 {
			revision = w.allEvents()
		}
		select {
		case <-w.closer.CloseNotify():
			return
		default:
		}

		wch := cli.Watch(w.closer.Ctx(), w.key,
			clientv3.WithPrefix(),
			clientv3.WithRev(revision+1),
			clientv3.WithPrevKV(),
		)
		if wch == nil {
			continue
		}
		for watchResp := range wch {
			if err := watchResp.Err(); err != nil {
				select {
				case <-w.closer.CloseNotify():
					return
				default:
					if errors.Is(err, v3rpc.ErrCompacted) {
						revision = -1
						break
					}
					continue
				}
			}
			for _, event := range watchResp.Events {
				select {
				case <-w.closer.CloseNotify():
					return
				default:
					w.handle(event)
				}
			}
		}
	}
}

func (w *watcher) handle(watchEvent *clientv3.Event) {
	switch watchEvent.Type {
	case mvccpb.PUT:
		md, err := w.kind.Unmarshal(watchEvent.Kv)
		if err != nil {
			w.l.Error().AnErr("err", err).Msg("failed to unmarshal message")
			return
		}
		w.handler.OnAddOrUpdate(md)
	case mvccpb.DELETE:
		md, err := w.kind.Unmarshal(watchEvent.PrevKv)
		if err != nil {
			w.l.Error().AnErr("err", err).Msg("failed to unmarshal message")
			return
		}
		w.handler.OnDelete(md)
	}
}

func (w *watcher) handleAllEvents(kvs []*mvccpb.KeyValue) {
	for i := 0; i < len(kvs); i++ {
		md, err := w.kind.Unmarshal(kvs[i])
		if err != nil {
			w.l.Error().AnErr("err", err).Msg("failed to unmarshal message")
			continue
		}
		w.handler.OnAddOrUpdate(md)
	}
}
