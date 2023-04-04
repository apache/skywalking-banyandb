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

package tsdb

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var (
	mtBytes    *prometheus.GaugeVec
	maxMtBytes *prometheus.GaugeVec
)

func init() {
	labels := []string{"module", "database", "shard", "component"}
	mtBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "banyand_memtables_bytes",
			Help: "Memory table size in bytes",
		},
		labels,
	)
	maxMtBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "banyand_memtables_max_bytes",
			Help: "Maximum amount of memory table available in bytes",
		},
		labels,
	)
}

func (s *shard) stat(_ time.Time, _ *logger.Logger) (r bool) {
	r = true
	defer func() {
		if r := recover(); r != nil {
			err, ok := r.(error)
			if !ok {
				err = fmt.Errorf("%v", r)
			}
			s.l.Warn().Err(errors.WithStack(err)).Msg("recovered")
		}
	}()
	seriesStat := s.seriesDatabase.Stats()
	s.curry(mtBytes).WithLabelValues("series").Set(float64(seriesStat.MemBytes))
	s.curry(maxMtBytes).WithLabelValues("series").Set(float64(seriesStat.MaxMemBytes))
	segStats := observability.Statistics{}
	blockStats := make(map[string]observability.Statistics)
	for _, seg := range s.segmentController.segments() {
		segStat := seg.Stats()
		segStats.MaxMemBytes += segStat.MaxMemBytes
		segStats.MemBytes += segStat.MemBytes
		for _, b := range seg.blockController.blocks() {
			if b.Closed() {
				continue
			}
			names, bss := b.stats()
			for i, bs := range bss {
				bsc, ok := blockStats[names[i]]
				if ok {
					bsc.MaxMemBytes += bs.MaxMemBytes
					bsc.MemBytes += bs.MemBytes
				} else {
					blockStats[names[i]] = bs
				}
			}
		}
	}
	s.curry(mtBytes).WithLabelValues("global-index").Set(float64(segStats.MemBytes))
	s.curry(maxMtBytes).WithLabelValues("global-index").Set(float64(segStats.MaxMemBytes))
	for name, bs := range blockStats {
		s.curry(mtBytes).WithLabelValues(name).Set(float64(bs.MemBytes))
		s.curry(maxMtBytes).WithLabelValues(name).Set(float64(bs.MaxMemBytes))
	}
	return
}

func (s *shard) curry(gv *prometheus.GaugeVec) *prometheus.GaugeVec {
	return gv.MustCurryWith(prometheus.Labels{
		"module":   s.position.Module,
		"database": s.position.Database,
		"shard":    s.position.Shard,
	})
}
