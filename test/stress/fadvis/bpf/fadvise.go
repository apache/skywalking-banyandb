package bpf

import (
	"fmt"
	"time"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
)

//go:generate go run github.com/cilium/ebpf/cmd/bpf2go -target native fadvise fadvise.c -- -I headers

type FadviseObjects struct {
	Fadvise64Trace *ebpf.Program `ebpf:"trace_sys_enter_fadvise64"`
	FadviseCalls   *ebpf.Map     `ebpf:"fadvise_calls"`
}

// LoadFadviseObjects 加载 eBPF 对象
func LoadFadviseObjects(obj interface{}, opts *ebpf.CollectionOptions) error {
	// 在生成 fadvise_bpfeb.go 和 fadvise_bpfel.go 文件后，
	// loadFadvise 函数会被自动创建
	spec, err := loadFadvise()
	if err != nil {
		return fmt.Errorf("loading BPF spec: %w", err)
	}
	return spec.LoadAndAssign(obj, opts)
}

// FadviseStats 结构存储 fadvise64 系统调用的统计信息
type FadviseStats struct {
	PID   uint32
	Count uint64
}

// RunFadvise 启动 eBPF 程序监控 fadvise64 系统调用
func RunFadvise() (func(), error) {
	objs := FadviseObjects{}
	if err := LoadFadviseObjects(&objs, nil); err != nil {
		return nil, fmt.Errorf("loading BPF objects: %w", err)
	}

	tp, err := link.Tracepoint("syscalls", "sys_enter_fadvise64", objs.Fadvise64Trace, nil)
	if err != nil {
		objs.Close()
		return nil, fmt.Errorf("attaching tracepoint: %w", err)
	}

	cleanup := func() {
		tp.Close()
		objs.Close()
	}

	return cleanup, nil
}

// GetFadviseStats 从 BPF map 中读取 fadvise64 调用统计信息
func GetFadviseStats(objs *FadviseObjects) ([]FadviseStats, error) {
	if objs == nil || objs.FadviseCalls == nil {
		return nil, fmt.Errorf("BPF objects or map not initialized")
	}

	var stats []FadviseStats
	var key uint32
	var value uint64

	iter := objs.FadviseCalls.Iterate()
	for iter.Next(&key, &value) {
		stats = append(stats, FadviseStats{
			PID:   key,
			Count: value,
		})
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("iterating map: %w", err)
	}

	return stats, nil
}

// MonitorFadviseWithTimeout 监控指定时间内的 fadvise64 调用
func MonitorFadviseWithTimeout(duration time.Duration) ([]FadviseStats, error) {
	objs := FadviseObjects{}
	if err := LoadFadviseObjects(&objs, nil); err != nil {
		return nil, fmt.Errorf("loading BPF objects: %w", err)
	}
	defer objs.Close()

	tp, err := link.Tracepoint("syscalls", "sys_enter_fadvise64", objs.Fadvise64Trace, nil)
	if err != nil {
		return nil, fmt.Errorf("attaching tracepoint: %w", err)
	}
	defer tp.Close()

	// 等待指定的时间
	time.Sleep(duration)

	// 收集和返回统计信息
	return GetFadviseStats(&objs)
}
