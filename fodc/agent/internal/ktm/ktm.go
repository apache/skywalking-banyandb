package ktm

import (
	"context"
	"fmt"
	"go.uber.org/zap"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor/collector"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor/metrics"
)

// KTM represents the Kernel Trace Module.
type KTM struct {
	config    Config
	logger    *zap.Logger
	collector *collector.Collector
	stopCh    chan struct{}
}

// NewKTM creates a new KTM instance.
func NewKTM(cfg Config, logger *zap.Logger) (*KTM, error) {
	if !cfg.Enabled {
		return &KTM{config: cfg, logger: logger}, nil
	}

	// Convert KTM config to Collector config
	collectorConfig := collector.CollectorConfig{
		Modules:  cfg.Modules,
		EBPF:     cfg.EBPF,
		Interval: cfg.Interval,
	}

	col, err := collector.New(collectorConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create collector: %w", err)
	}

	return &KTM{
		config:    cfg,
		logger:    logger,
		collector: col,
		stopCh:    make(chan struct{}),
	}, nil
}

// Start starts the KTM module.
func (k *KTM) Start(ctx context.Context) error {
	if !k.config.Enabled {
		k.logger.Info("KTM module is disabled")
		return nil
	}

	k.logger.Info("Starting KTM module")
	if err := k.collector.Start(ctx); err != nil {
		return fmt.Errorf("failed to start collector: %w", err)
	}

	return nil
}

// Stop stops the KTM module.
func (k *KTM) Stop() error {
	if !k.config.Enabled {
		return nil
	}

	k.logger.Info("Stopping KTM module")
	close(k.stopCh)
	return k.collector.Close()
}

// GetMetrics returns the collected metrics.
func (k *KTM) GetMetrics() *metrics.Store {
	if !k.config.Enabled || k.collector == nil {
		return nil
	}
	return k.collector.GetMetrics()
}
