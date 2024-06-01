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

package observability

import (
	"context"
	"strings"
	"sync"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

var log = logger.GetLogger("observability", "metrics", "system")

var (
	cpuCount      = 0
	once4CpuCount sync.Once
	cpuCountsFunc = cpu.Counts
	cpuTimesFunc  = cpu.Times
)

var (
	// RootScope is the root scope for all metrics.
	RootScope = meter.NewHierarchicalScope("banyandb", "_")
	// SystemScope is the system scope for all metrics.
	SystemScope = RootScope.SubScope("system")
)

func init() {
	MetricsCollector.Register("cpu", collectCPU)
	MetricsCollector.Register("memory", collectMemory)
	MetricsCollector.Register("net", collectNet)
}

func registerMetrics(provider meter.Provider) {
	provider.RegisterGauge("cpu_state", "kind")
	provider.RegisterGauge("cpu_num")
	provider.RegisterGauge("memory_state", "kind")
	provider.RegisterGauge("net_state", "kind", "name")
}

func collectCPU(provider meter.Provider) {
	cpuStateGauge := provider.GetGauge("cpu_state", "kind")
	if cpuStateGauge == nil {
		log.Error().Msg("cpuStateGauge is not registered")
	}
	cpuNumGauge := provider.GetGauge("cpu_num")
	if cpuNumGauge == nil {
		log.Error().Msg("cpuNumGauge is not registered")
	}
	once4CpuCount.Do(func() {
		if c, err := cpuCountsFunc(false); err != nil {
			log.Error().Err(err).Msg("cannot get cpu count")
		} else {
			cpuCount = c
		}
	})
	cpuNumGauge.Set(float64(cpuCount))
	s, err := cpuTimesFunc(false)
	if err != nil {
		log.Error().Err(err).Msg("cannot get cpu stat")
	}
	if len(s) == 0 {
		log.Error().Msg("cannot get cpu stat")
	}
	allStat := s[0]
	total := allStat.User + allStat.System + allStat.Idle + allStat.Nice + allStat.Iowait + allStat.Irq +
		allStat.Softirq + allStat.Steal + allStat.Guest + allStat.GuestNice
	cpuStateGauge.Set(allStat.User/total, "user")
	cpuStateGauge.Set(allStat.System/total, "system")
	cpuStateGauge.Set(allStat.Idle/total, "idle")
	cpuStateGauge.Set(allStat.Nice/total, "nice")
	cpuStateGauge.Set(allStat.Iowait/total, "iowait")
	cpuStateGauge.Set(allStat.Irq/total, "irq")
	cpuStateGauge.Set(allStat.Softirq/total, "softirq")
	cpuStateGauge.Set(allStat.Steal/total, "steal")
}

func collectMemory(provider meter.Provider) {
	memorySateGauge := provider.GetGauge("memory_state", "kind")
	if memorySateGauge == nil {
		log.Error().Msg("memorySateGauge is not registered")
	}
	m, err := mem.VirtualMemory()
	if err != nil {
		log.Error().Err(err).Msg("cannot get memory stat")
	}
	memorySateGauge.Set(m.UsedPercent/100, "used_percent")
	memorySateGauge.Set(float64(m.Used)/float64(m.Total), "used")
}

func collectNet(provider meter.Provider) {
	netStateGauge := provider.GetGauge("net_state", "kind", "name")
	if netStateGauge == nil {
		log.Error().Msg("netStateGauge is not registered")
	}
	stats, err := getNetStat(context.Background())
	if err != nil {
		log.Error().Err(err).Msg("cannot get net stat")
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
