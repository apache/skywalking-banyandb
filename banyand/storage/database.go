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

	"go.uber.org/atomic"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/pkg/logger"
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
	seriesLst []*series
	repo      discovery.ServiceRepo
	pluginLst []Plugin

	stopCh chan struct{}
	log    *logger.Logger
}

func (d *DB) Serve() error {
	d.stopCh = make(chan struct{})
	<-d.stopCh
	return nil
}

func (d *DB) GracefulStop() {
	for _, s := range d.seriesLst {
		s.stop()
	}
	if d.stopCh != nil {
		close(d.stopCh)
	}
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
	return flagS
}

func (d *DB) Validate() error {
	return nil
}

func (d *DB) PreRun() error {
	d.log = logger.GetLogger("database")
	if err := d.init(); err != nil {
		return fmt.Errorf("failed to initialize db: %v", err)
	}
	return nil
}

type series struct {
	sLst     []*shard
	init     bool
	location string
}

func (s *series) Reader(shard uint, name string, start, end uint64) kv.Reader {
	//TODO: find targets in all blocks
	b, ok := s.sLst[shard].activeBlock.Load().(*block)
	if !ok {
		return nil
	}
	return b.stores[name]
}

func (s *series) TimeSeriesReader(shard uint, name string, start, end uint64) kv.TimeSeriesReader {
	//TODO: find targets in all blocks
	b, ok := s.sLst[shard].activeBlock.Load().(*block)
	if !ok {
		return nil
	}
	return b.tsStores[name]
}

func (s *series) load(meta PluginMeta) error {
	//TODO: to implement load instead of removing old contents
	return os.RemoveAll(s.location)
}

func (s *series) create(meta PluginMeta, plugin Plugin) (err error) {
	if _, err = mkdir(s.location); err != nil {
		return err
	}

	for i := 0; i < int(meta.ShardNumber); i++ {
		var shardLocation string
		if shardLocation, err = mkdir(shardTemplate, s.location, i); err != nil {
			return err
		}
		so := newShard(i, shardLocation)
		if sErr := so.init(plugin); sErr != nil {
			err = multierr.Append(err, sErr)
			continue
		}
		s.sLst = append(s.sLst, so)
	}
	return err

}

func (s *series) stop() {
	for _, sa := range s.sLst {
		sa.stop()
	}
}

func (d *DB) init() (err error) {
	if _, err = mkdir(d.root); err != nil {
		return fmt.Errorf("failed to create %s: %v", d.root, err)
	}
	d.log.Info().Str("path", d.root).Msg("initialize database")
	var entries []fs.FileInfo
	if entries, err = ioutil.ReadDir(d.root); err != nil {
		return fmt.Errorf("failed to read directory contents failed: %v", err)
	}
	for _, plugin := range d.pluginLst {
		meta := plugin.Meta()

		s := &series{location: d.root + "/" + meta.ID}
		d.seriesLst = append(d.seriesLst, s)
		for _, entry := range entries {
			if entry.Name() == meta.ID && entry.IsDir() {
				err = s.load(meta)
				if err != nil {
					return err
				}
				d.log.Info().Str("ID", meta.ID).Msg("loaded series")
				break
			}
		}
		if !s.init {
			err = s.create(meta, plugin)
			if err != nil {
				return err
			}
			d.log.Info().Str("ID", meta.ID).Msg("created series")
		}
		plugin.Init(s, func(_ uint64) WritePoint {
			bLst := make([]*block, len(s.sLst))
			for i, sa := range s.sLst {
				b, ok := sa.activeBlock.Load().(*block)
				if !ok {
					continue
				}
				bLst[i] = b
			}
			return &writePoint{bLst: bLst}
		})
		d.log.Info().Str("ID", meta.ID).Msg("initialized plugin")
	}
	return err
}

var _ WritePoint = (*writePoint)(nil)

type writePoint struct {
	bLst []*block
}

func (w *writePoint) Close() error {
	return nil
}

func (w *writePoint) Writer(shard uint, name string) kv.Writer {
	return w.bLst[shard].stores[name]
}

func (w *writePoint) TimeSeriesWriter(shard uint, name string) kv.TimeSeriesWriter {
	return w.bLst[shard].tsStores[name]
}

type shard struct {
	id  int
	lst []*segment
	sync.Mutex
	location    string
	activeBlock atomic.Value
}

func newShard(id int, location string) *shard {
	return &shard{
		id:       id,
		location: location,
	}
}

func (s *shard) newSeg(shardID int, path string) *segment {
	s.Lock()
	defer s.Unlock()
	seg := &segment{
		path:    path,
		shardID: shardID,
	}
	s.lst = append(s.lst, seg)
	return seg
}

func (s *shard) init(plugin Plugin) error {
	segPath, err := mkdir(segTemplate, s.location, time.Now().Format(segFormat))
	if err != nil {
		return fmt.Errorf("failed to make segment directory: %v", err)
	}

	seg := s.newSeg(s.id, segPath)
	return seg.init(plugin, s.updateActiveBlock)
}

func (s *shard) updateActiveBlock(newBlock *block) {
	s.activeBlock.Store(newBlock)
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
	path    string
	shardID int
}

func (s *segment) addBlock(b *block) {
	s.Lock()
	defer s.Unlock()
	s.lst = append(s.lst, b)
}

func (s *segment) init(plugin Plugin, activeBlock func(newBlock *block)) error {
	blockPath, err := mkdir(blockTemplate, s.path, time.Now().Format(blockFormat))
	if err != nil {
		return fmt.Errorf("failed to make block directory: %v", err)
	}
	var b *block
	if b, err = newBlock(s.shardID, blockPath, plugin); err != nil {
		return fmt.Errorf("failed to create segment: %v", err)
	}
	activeBlock(b)
	s.addBlock(b)
	return b.init()
}

func (s *segment) close() {
	for _, b := range s.lst {
		b.close()
	}
}
