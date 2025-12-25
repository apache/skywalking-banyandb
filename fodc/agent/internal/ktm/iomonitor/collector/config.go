package collector

import (
	"time"
)

// CollectorConfig defines the collector configuration.
type CollectorConfig struct {
	Modules         []string      `mapstructure:"modules"`
	EBPF            EBPFConfig    `mapstructure:"ebpf"`
	Interval        time.Duration `mapstructure:"interval"`
	CleanupStrategy string        `mapstructure:"cleanup_strategy"`
	CleanupInterval time.Duration `mapstructure:"cleanup_interval"`
}

// EBPFConfig defines eBPF-specific configuration.
type EBPFConfig struct {
	PinPath      string `mapstructure:"pin_path"`       // Path to pin eBPF maps
	MapSizeLimit int    `mapstructure:"map_size_limit"` // Maximum map size
	CgroupPath   string `mapstructure:"cgroup_path"`    // Optional cgroup v2 path to filter PIDs
}
