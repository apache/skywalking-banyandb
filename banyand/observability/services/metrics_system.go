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

package services

import (
	"context"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"

	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

var log = logger.GetLogger("metrics")

var (
	cpuCount      = 0
	once4CpuCount sync.Once
	cpuCountsFunc = cpu.Counts
	cpuTimesFunc  = cpu.Times
	startTime     = time.Now()
)

// gaugesMu protects the process-global gauge vars below. Multiple metricService
// instances can exist in the same process (e.g. distributed integration tests),
// so initMetrics() (writer) and collect*() (readers) must be synchronised.
var gaugesMu sync.RWMutex

var (
	cpuStateGauge    meter.Gauge
	cpuNumGauge      meter.Gauge
	memoryStateGauge meter.Gauge
	netStateGauge    meter.Gauge
	upTimeGauge      meter.Gauge
	diskStateGauge   meter.Gauge
	diskMap          = sync.Map{}
)

// UpdatePath updates a path to monitoring its disk usage.
func UpdatePath(path string) {
	diskMap.Store(path, 0)
}

// GetPathUsedPercent returns the used percent of the path.
func GetPathUsedPercent(path string) int {
	if v, ok := diskMap.Load(path); ok {
		return v.(int)
	}
	return 0
}

func getPath() (paths []string) {
	diskMap.Range(func(key, _ any) bool {
		paths = append(paths, key.(string))
		return true
	})
	return paths
}

func init() {
	MetricsCollector.Register("cpu", collectCPU)
	MetricsCollector.Register("memory", collectMemory)
	MetricsCollector.Register("net", collectNet)
	MetricsCollector.Register("upTime", collectUpTime)
	MetricsCollector.Register("disk", collectDisk)
}

func (p *metricService) initMetrics() {
	factory := p.With(observability.SystemScope)
	gaugesMu.Lock()
	defer gaugesMu.Unlock()
	cpuStateGauge = factory.NewGauge("cpu_state", "kind")
	cpuNumGauge = factory.NewGauge("cpu_num")
	memoryStateGauge = factory.NewGauge("memory_state", "kind")
	netStateGauge = factory.NewGauge("net_state", "kind", "name")
	upTimeGauge = factory.NewGauge("up_time")
	diskStateGauge = factory.NewGauge("disk", "path", "kind")
}

func collectCPU() {
	once4CpuCount.Do(func() {
		if c, err := cpuCountsFunc(false); err != nil {
			log.Error().Err(err).Msg("cannot get cpu count")
		} else {
			cpuCount = c
		}
	})
	s, err := cpuTimesFunc(false)
	if err != nil {
		log.Error().Err(err).Msg("cannot get cpu stat")
		return
	}
	if len(s) == 0 {
		log.Error().Msg("cannot get cpu stat")
		return
	}
	allStat := s[0]
	total := allStat.User + allStat.System + allStat.Idle + allStat.Nice + allStat.Iowait + allStat.Irq +
		allStat.Softirq + allStat.Steal + allStat.Guest + allStat.GuestNice
	gaugesMu.RLock()
	defer gaugesMu.RUnlock()
	if cpuNumGauge == nil || cpuStateGauge == nil {
		return
	}
	cpuNumGauge.Set(float64(cpuCount))
	cpuStateGauge.Set(allStat.User/total, "user")
	cpuStateGauge.Set(allStat.System/total, "system")
	cpuStateGauge.Set(allStat.Idle/total, "idle")
	cpuStateGauge.Set(allStat.Nice/total, "nice")
	cpuStateGauge.Set(allStat.Iowait/total, "iowait")
	cpuStateGauge.Set(allStat.Irq/total, "irq")
	cpuStateGauge.Set(allStat.Softirq/total, "softirq")
	cpuStateGauge.Set(allStat.Steal/total, "steal")
}

func collectMemory() {
	m, err := mem.VirtualMemory()
	if err != nil {
		log.Error().Err(err).Msg("cannot get memory stat")
		return
	}
	gaugesMu.RLock()
	defer gaugesMu.RUnlock()
	if memoryStateGauge == nil {
		return
	}
	memoryStateGauge.Set(m.UsedPercent/100, "used_percent")
	memoryStateGauge.Set(float64(m.Used), "used")
	memoryStateGauge.Set(float64(m.Total), "total")
}

func collectNet() {
	stats, err := getNetStat(context.Background())
	if err != nil {
		log.Error().Err(err).Msg("cannot get net stat")
		return
	}
	gaugesMu.RLock()
	defer gaugesMu.RUnlock()
	if netStateGauge == nil {
		return
	}
	for _, stat := range stats {
		netStateGauge.Set(float64(stat.BytesRecv), "bytes_recv", stat.Name)
		netStateGauge.Set(float64(stat.BytesSent), "bytes_sent", stat.Name)
		netStateGauge.Set(float64(stat.PacketsRecv), "packets_recv", stat.Name)
		netStateGauge.Set(float64(stat.PacketsSent), "packets_sent", stat.Name)
		netStateGauge.Set(float64(stat.Errin), "errin", stat.Name)
		netStateGauge.Set(float64(stat.Errout), "errout", stat.Name)
		netStateGauge.Set(float64(stat.Dropin), "dropin", stat.Name)
		netStateGauge.Set(float64(stat.Dropout), "dropout", stat.Name)
		netStateGauge.Set(float64(stat.Fifoin), "fifoin", stat.Name)
		netStateGauge.Set(float64(stat.Fifoout), "fifoout", stat.Name)
	}
}

func getNetStat(ctx context.Context) ([]net.IOCountersStat, error) {
	stats, err := net.IOCountersWithContext(ctx, true)
	if err != nil {
		return nil, err
	}
	var availableStats []net.IOCountersStat
	for _, stat := range stats {
		switch {
		// OS X
		case strings.HasPrefix(stat.Name, "en"):
		// Linux
		case strings.HasPrefix(stat.Name, "eth"):
		default:
			continue
		}
		// ignore empty interface
		if stat.BytesRecv == 0 || stat.BytesSent == 0 {
			continue
		}
		availableStats = append(availableStats, stat)
	}
	return availableStats, nil
}

func collectUpTime() {
	gaugesMu.RLock()
	defer gaugesMu.RUnlock()
	if upTimeGauge == nil {
		return
	}
	upTimeGauge.Set(time.Since(startTime).Seconds())
}

func collectDisk() {
	paths := getPath()
	type usageStat struct {
		path  string
		usage *disk.UsageStat
	}
	var stats []usageStat
	for _, path := range paths {
		usage, err := disk.Usage(path)
		if err != nil {
			if _, statErr := os.Stat(path); statErr != nil {
				if !os.IsNotExist(statErr) {
					log.Error().Err(statErr).Msgf("failed to get stat for path: %s", path)
				}
			}
			return
		}
		diskMap.Store(path, int(usage.UsedPercent))
		stats = append(stats, usageStat{path: path, usage: usage})
	}
	gaugesMu.RLock()
	defer gaugesMu.RUnlock()
	if diskStateGauge == nil {
		return
	}
	for _, s := range stats {
		diskStateGauge.Set(s.usage.UsedPercent/100, s.path, "used_percent")
		diskStateGauge.Set(float64(s.usage.Used), s.path, "used")
		diskStateGauge.Set(float64(s.usage.Total), s.path, "total")
	}
}
