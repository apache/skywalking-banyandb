package bpf

import (
	"fmt"
	"time"
	"bytes"

	"github.com/cilium/ebpf/link"
)

// X86 platform
//go:generate go run github.com/cilium/ebpf/cmd/bpf2go -target bpfel \
//    -cc clang -cflags "-O2 -g -Wall -I../bpf" \
//    -output-stem bpf_x86 -go-package bpf \
//    Bpf ../bpfsrc/fadvise.c

// ARM64 platform
//go:generate go run github.com/cilium/ebpf/cmd/bpf2go -target bpfel \
//    -cc clang -cflags "-O2 -g -Wall -I../bpf" \
//    -output-stem bpf_arm64 -go-package bpf \
//    Bpf ../bpfsrc/fadvise.c

// FadviseStats 表示用户态统计信息，仅聚焦我们关注的字段
// 目前只展示成功调用数和 AdviceDontneed 类型的调用
// 内核态结构 BpfFadviseStatsT 会包含全部类型

type FadviseStats struct {
	PID            uint32
	SuccessCalls   uint64
	AdviceDontneed uint64
}

type LruShrinkStats struct {
	NrScanned   uint64
	NrReclaimed uint64
	CallerPid   uint32
	CallerComm  string
}

// RunFadvise 启动 eBPF 程序，attach sys_enter/sys_exit 以及 lru_shrink tracepoint
func RunFadvise() (func(), error) {
	objs := BpfObjects{}
	if err := LoadBpfObjects(&objs, nil); err != nil {
		return nil, fmt.Errorf("loading BPF objects: %w", err)
	}

	tpEnter, err := link.Tracepoint("syscalls", "sys_enter_fadvise64", objs.TraceEnterFadvise64, nil)
	if err != nil {
		objs.Close()
		return nil, fmt.Errorf("attaching enter tracepoint: %w", err)
	}
	tpExit, err := link.Tracepoint("syscalls", "sys_exit_fadvise64", objs.TraceExitFadvise64, nil)
	if err != nil {
		tpEnter.Close()
		objs.Close()
		return nil, fmt.Errorf("attaching exit tracepoint: %w", err)
	}
	tpLru, err := link.Tracepoint("vmscan", "mm_vmscan_lru_shrink_inactive", objs.TraceLruShrinkInactive, nil)
	if err != nil {
		tpEnter.Close()
		tpExit.Close()
		objs.Close()
		return nil, fmt.Errorf("attaching lru_shrink tracepoint: %w", err)
	}

	cleanup := func() {
		tpEnter.Close()
		tpExit.Close()
		tpLru.Close()
		objs.Close()
	}
	return cleanup, nil
}

// GetFadviseStats 从 BPF map 中提取用户关心的统计数据
func GetFadviseStats(objs *BpfObjects) ([]FadviseStats, error) {
	if objs == nil || objs.FadviseStatsMap == nil {
		return nil, fmt.Errorf("BPF objects or map not initialized")
	}

	var stats []FadviseStats
	var key uint32
	var value BpfFadviseStatsT

	iter := objs.FadviseStatsMap.Iterate()
	for iter.Next(&key, &value) {
		stats = append(stats, FadviseStats{
			PID:            key,
			SuccessCalls:   value.SuccessCalls,
			AdviceDontneed: value.AdviceDontneed,
		})
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("iterating map: %w", err)
	}

	return stats, nil
}

// GetLruShrinkStats 提取 shrink map 中的统计信息
func GetLruShrinkStats(objs *BpfObjects) (*LruShrinkStats, error) {
	if objs == nil || objs.ShrinkStatsMap == nil {
		return nil, fmt.Errorf("BPF objects or map not initialized")
	}

	var key uint32 = 0
	var value BpfLruShrinkInfoT

	if err := objs.ShrinkStatsMap.Lookup(&key, &value); err != nil {
		return nil, fmt.Errorf("failed to lookup shrink stats: %w", err)
	}

	stats := &LruShrinkStats{
		NrScanned:   value.NrScanned,
		NrReclaimed: value.NrReclaimed,
		CallerPid:   value.CallerPid,
		CallerComm:  int8SliceToString(value.CallerComm[:]),
	}

	return stats, nil
}

func int8SliceToString(s []int8) string {
	b := make([]byte, len(s))
	for i, v := range s {
		if v == 0 {
			return string(b[:i])
		}
		b[i] = byte(v)
	}
	return string(b)
}

// MonitorFadviseWithTimeout 启动 fadvise 监控并在给定时间后返回结果
func MonitorFadviseWithTimeout(duration time.Duration) ([]FadviseStats, error) {
	objs := BpfObjects{}
	if err := LoadBpfObjects(&objs, nil); err != nil {
		return nil, fmt.Errorf("loading BPF objects: %w", err)
	}
	defer objs.Close()

	tpEnter, err := link.Tracepoint("syscalls", "sys_enter_fadvise64", objs.TraceEnterFadvise64, nil)
	if err != nil {
		return nil, fmt.Errorf("attaching enter tracepoint: %w", err)
	}
	defer tpEnter.Close()

	tpExit, err := link.Tracepoint("syscalls", "sys_exit_fadvise64", objs.TraceExitFadvise64, nil)
	if err != nil {
		return nil, fmt.Errorf("attaching exit tracepoint: %w", err)
	}
	defer tpExit.Close()

	tpLru, err := link.Tracepoint("vmscan", "mm_vmscan_lru_shrink_inactive", objs.TraceLruShrinkInactive, nil)
	if err != nil {
		return nil, fmt.Errorf("attaching lru_shrink tracepoint: %w", err)
	}
	defer tpLru.Close()

	time.Sleep(duration)
	return GetFadviseStats(&objs)
}

type DirectReclaimInfo struct {
	PID  uint32
	Comm string
}

func int8SliceToByte(s []int8) []byte {
	b := make([]byte, len(s))
	for i, v := range s {
		b[i] = byte(v)
	}
	return b
}

func GetDirectReclaimStats(objs *BpfObjects) (map[uint32]DirectReclaimInfo, error) {
	if objs == nil || objs.DirectReclaimMap == nil {
		return nil, fmt.Errorf("BPF objects or map not initialized")
	}

	result := make(map[uint32]DirectReclaimInfo)
	var key uint32
	var value BpfReclaimInfoT

	iter := objs.DirectReclaimMap.Iterate()
	for iter.Next(&key, &value) {
		result[key] = DirectReclaimInfo{
			PID:  key,
			Comm: string(bytes.Trim(int8SliceToByte(value.Comm[:]), "\x00")),
		}
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("iterating direct reclaim map: %w", err)
	}

	return result, nil
}