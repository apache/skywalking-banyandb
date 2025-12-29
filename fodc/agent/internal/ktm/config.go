package ktm

import (
	"time"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor"
)

// Config represents the configuration for the KTM module.
type Config struct {
	Enabled  bool                 `mapstructure:"enabled"`
	Interval time.Duration        `mapstructure:"interval"`
	Modules  []string             `mapstructure:"modules"`
	EBPF     iomonitor.EBPFConfig `mapstructure:"ebpf"`
}

// DefaultConfig returns the default configuration for KTM.
func DefaultConfig() Config {
	return Config{
		Enabled:  false,
		Interval: 10 * time.Second,
		Modules:  []string{"iomonitor"},
		EBPF: iomonitor.EBPFConfig{
			PinPath:      "/sys/fs/bpf/ebpf-sidecar",
			MapSizeLimit: 10240,
		},
	}
}
