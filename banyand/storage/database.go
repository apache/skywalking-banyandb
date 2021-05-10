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

package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/api/event"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/internal/bus"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/storage/kv"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var _ Database = (*DB)(nil)

type DB struct {
	root   string
	shards int
	repo   discovery.ServiceRepo
	q      queue.Queue
}

func (d *DB) Name() string {
	return "database"
}

func (d *DB) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("storage")
	fs.StringVar(&d.root, "root-path", "", "the root path of database")
	fs.IntVar(&d.shards, "shards", 1, "total shards size")
	return fs
}

func (d *DB) Validate() error {
	return nil
}

func (d *DB) PreRun() error {
	if err := d.init(); err != nil {
		return fmt.Errorf("failed to initialize db: %v", err)
	}
	if err := d.start(); err != nil {
		return fmt.Errorf("failed to start db: %v", err)
	}
	return d.repo.Publish(bus.Topic(event.ShardEventKindVersion.String()), bus.NewMessage(1, event.NewShard()))
}

type segment struct {
	lst []kv.Block
	sync.Mutex
}

func (s *segment) AddBlock(b kv.Block) {
	s.Lock()
	defer s.Unlock()
	s.lst = append(s.lst, b)
}

type shard struct {
	id  int
	lst []*segment
	sync.Mutex
}

func (s *shard) newSeg() *segment {
	s.Lock()
	defer s.Unlock()
	seg := &segment{}
	s.lst = append(s.lst, seg)
	return seg
}

func (s *shard) init() error {
	seg := s.newSeg()
	b, err := kv.NewBlock()
	if err != nil {
		return fmt.Errorf("failed to create segment: %v", err)
	}
	seg.AddBlock(b)
	return nil
}

func (d *DB) init() (err error) {
	if err = os.MkdirAll(d.root, os.ModeDir); err != nil {
		return fmt.Errorf("failed to create %s: %v", d.root, err)
	}
	var isEmpty bool
	if isEmpty, err = isEmptyDir(d.root); err != nil {
		return fmt.Errorf("checking directory contents failed: %v", err)
	}
	if !isEmpty {
		return nil
	}
	for i := 0; i < d.shards; i++ {
		s := newShard(i)
		err = multierr.Append(err, s.init())
	}
	if err != nil {
		return fmt.Errorf("failed to init shards: %v", err)
	}
	return nil
}

func (d *DB) start() error {
	return d.q.Subscribe(bus.Topic(data.TraceKindVersion.String()), d)
}

func (d *DB) Rev(message bus.Message) {
	//nolint
	_, ok := message.Data().(data.Trace)
	if !ok {
		return
	}
	//TODO: save data into target shard
}

func newShard(id int) *shard {
	return &shard{id: id}
}

func isEmptyDir(name string) (bool, error) {
	entries, err := ioutil.ReadDir(name)
	if err != nil {
		return false, err
	}
	return len(entries) == 0, nil
}
