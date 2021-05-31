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
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	shardTemplate = "%s/shard-%d"
	segTemplate   = "%s/seg-%s"
	blockTemplate = "%s/block-%s"

	segFormat   = "20060102"
	blockFormat = "1504"

	dirPerm = 0700
)

var ErrNoDiscoveryRepo = errors.New("no discovery repo exists")

var _ Database = (*DB)(nil)

// DB is a storage manager to physical data model.
// Notice: The current status of DB is under WIP. It only contains feats support to verify the series module.
type DB struct {
	root      string
	shards    int
	sLst      []*shard
	repo      discovery.ServiceRepo
	pluginLst []Plugin

	stopCh chan struct{}
}

func (d *DB) Serve() error {
	d.stopCh = make(chan struct{})
	<-d.stopCh
	return nil
}

func (d *DB) GracefulStop() {
	for _, s := range d.sLst {
		s.stop()
	}
	close(d.stopCh)
}

func (d *DB) Register(plugin Plugin) {
	d.pluginLst = append(d.pluginLst, plugin)
}

func (d *DB) Name() string {
	return "database"
}

func (d *DB) FlagSet() *run.FlagSet {
	flagS := run.NewFlagSet("storage")
	flagS.StringVar(&d.root, "root-path", "/tmp", "the root path of database")
	flagS.IntVar(&d.shards, "shards", 1, "total shards size")
	return flagS
}

func (d *DB) Validate() error {
	return nil
}

func (d *DB) PreRun() error {
	if err := d.init(); err != nil {
		return fmt.Errorf("failed to initialize db: %v", err)
	}
	return nil
}

func (d *DB) init() (err error) {
	if _, err = mkdir(d.root); err != nil {
		return fmt.Errorf("failed to create %s: %v", d.root, err)
	}
	var entris []fs.FileInfo
	if entris, err = ioutil.ReadDir(d.root); err != nil {
		return fmt.Errorf("failed to read directory contents failed: %v", err)
	}
	if len(entris) < 1 {
		return d.createShards()
	}
	return d.loadShards(entris)
}

func (d *DB) loadShards(_ []fs.FileInfo) (err error) {
	//TODO load existing shards
	return nil
}

func (d *DB) createShards() (err error) {
	for i := 0; i < d.shards; i++ {
		var shardLocation string
		if shardLocation, err = mkdir(shardTemplate, d.root, i); err != nil {
			return err
		}
		s := newShard(i, shardLocation)
		if sErr := s.init(d.pluginLst); sErr != nil {
			err = multierr.Append(err, sErr)
			continue
		}
		d.sLst = append(d.sLst, s)
	}
	return err
}

type shard struct {
	id  int
	lst []*segment
	sync.Mutex
	location string
}

func newShard(id int, location string) *shard {
	return &shard{
		id:       id,
		location: location,
	}
}

func (s *shard) newSeg(path string) *segment {
	s.Lock()
	defer s.Unlock()
	seg := &segment{
		path: path,
	}
	s.lst = append(s.lst, seg)
	return seg
}

func (s *shard) init(plugins []Plugin) error {
	segPath, err := mkdir(segTemplate, s.location, time.Now().Format(segFormat))
	if err != nil {
		return fmt.Errorf("failed to make segment directory: %v", err)
	}
	seg := s.newSeg(segPath)
	return seg.init(plugins)
}

func (s *shard) stop() {
	for _, seg := range s.lst {
		seg.close()
	}
}

func mkdir(format string, a ...interface{}) (path string, err error) {
	path = fmt.Sprintf(format, a...)
	if err = os.MkdirAll(path, dirPerm); err != nil {
		return "", err
	}
	return path, err
}

type segment struct {
	lst []*block
	sync.Mutex
	path string
}

func (s *segment) addBlock(b *block) {
	s.Lock()
	defer s.Unlock()
	s.lst = append(s.lst, b)
}

func (s *segment) init(plugins []Plugin) error {
	blockPath, err := mkdir(blockTemplate, s.path, time.Now().Format(blockFormat))
	if err != nil {
		return fmt.Errorf("failed to make block directory: %v", err)
	}
	var b *block
	if b, err = newBlock(blockPath, plugins); err != nil {
		return fmt.Errorf("failed to create segment: %v", err)
	}
	s.addBlock(b)
	return b.init()
}

func (s *segment) close() {
	for _, block := range s.lst {
		block.close()
	}
}
