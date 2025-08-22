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

package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// Config represents the complete configuration for the eBPF sidecar.
type Config struct {
	Export    ExportConfig    `mapstructure:"export"`
	Server    ServerConfig    `mapstructure:"server"`
	Collector CollectorConfig `mapstructure:"collector"`
}

// ServerConfig defines the server configuration.
type ServerConfig struct {
	HTTP HTTPConfig `mapstructure:"http"`
	GRPC GRPCConfig `mapstructure:"grpc"`
}

// GRPCConfig defines gRPC server configuration.
type GRPCConfig struct {
	TLS  TLSConfig `mapstructure:"tls"`
	Port int       `mapstructure:"port"`
}

// HTTPConfig defines HTTP server configuration.
type HTTPConfig struct {
	MetricsPath string `mapstructure:"metrics_path"`
	Port        int    `mapstructure:"port"`
}

// TLSConfig defines TLS configuration.
type TLSConfig struct {
	CertFile string `mapstructure:"cert_file"`
	KeyFile  string `mapstructure:"key_file"`
	Enabled  bool   `mapstructure:"enabled"`
}

// CollectorConfig defines the collector configuration.
type CollectorConfig struct {
	Modules  []string      `mapstructure:"modules"`
	eBPF     eBPFConfig    `mapstructure:"ebpf"`
	Interval time.Duration `mapstructure:"interval"`
}

// eBPFConfig defines eBPF-specific configuration.
type eBPFConfig struct {
	PinPath      string `mapstructure:"pin_path"`       // Path to pin eBPF maps
	MapSizeLimit int    `mapstructure:"map_size_limit"` // Maximum map size
}

// ExportConfig defines export configuration.
type ExportConfig struct {
	Type     string         `mapstructure:"type"` // "prometheus" or "banyandb"
	BanyanDB BanyanDBConfig `mapstructure:"banyandb"`
}

// BanyanDBConfig defines BanyanDB export configuration.
type BanyanDBConfig struct {
	Endpoint string        `mapstructure:"endpoint"`
	Group    string        `mapstructure:"group"`
	TLS      TLSConfig     `mapstructure:"tls"`
	Timeout  time.Duration `mapstructure:"timeout"`
}

// DefaultConfig returns the default configuration.
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			GRPC: GRPCConfig{
				Port: 9090,
				TLS: TLSConfig{
					Enabled: false,
				},
			},
			HTTP: HTTPConfig{
				Port:        8080,
				MetricsPath: "/metrics",
			},
		},
		Collector: CollectorConfig{
			Interval: 10 * time.Second,
			Modules:  []string{"fadvise", "memory"},
			eBPF: eBPFConfig{
				PinPath:      "/sys/fs/bpf/ebpf-sidecar",
				MapSizeLimit: 10240,
			},
		},
		Export: ExportConfig{
			Type: "prometheus",
			BanyanDB: BanyanDBConfig{
				Endpoint: "localhost:17912",
				Group:    "ebpf-metrics",
				Timeout:  30 * time.Second,
			},
		},
	}
}

// Load loads configuration from file or returns default.
func Load(configFile string) (*Config, error) {
	cfg := DefaultConfig()

	if configFile == "" {
		// Try to find config file in default locations
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
		viper.AddConfigPath("/etc/ebpf-sidecar/")
		viper.AddConfigPath("./configs/")
		viper.AddConfigPath(".")
	} else {
		viper.SetConfigFile(configFile)
	}

	// Set environment variable prefix
	viper.SetEnvPrefix("EBPF_SIDECAR")
	viper.AutomaticEnv()

	// Read configuration
	if err := viper.ReadInConfig(); err != nil {
		// If config file is explicitly specified, return error
		if configFile != "" {
			return nil, fmt.Errorf("failed to read config file %s: %w", configFile, err)
		}
		// Otherwise, use default config
		return cfg, nil
	}

	// Unmarshal configuration
	if err := viper.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	// Validate server configuration
	if c.Server.GRPC.Port <= 0 || c.Server.GRPC.Port > 65535 {
		return fmt.Errorf("invalid gRPC port: %d", c.Server.GRPC.Port)
	}
	if c.Server.HTTP.Port <= 0 || c.Server.HTTP.Port > 65535 {
		return fmt.Errorf("invalid HTTP port: %d", c.Server.HTTP.Port)
	}

	// Validate TLS configuration if enabled
	if c.Server.GRPC.TLS.Enabled {
		if c.Server.GRPC.TLS.CertFile == "" || c.Server.GRPC.TLS.KeyFile == "" {
			return fmt.Errorf("TLS enabled but cert_file or key_file not specified")
		}
	}

	// Validate collector configuration
	if c.Collector.Interval <= 0 {
		return fmt.Errorf("invalid collector interval: %s", c.Collector.Interval)
	}
	if len(c.Collector.Modules) == 0 {
		return fmt.Errorf("no collector modules specified")
	}

	// Validate export configuration
	if c.Export.Type != "prometheus" && c.Export.Type != "banyandb" {
		return fmt.Errorf("invalid export type: %s (must be 'prometheus' or 'banyandb')", c.Export.Type)
	}

	if c.Export.Type == "banyandb" {
		if c.Export.BanyanDB.Endpoint == "" {
			return fmt.Errorf("BanyanDB endpoint not specified")
		}
		if c.Export.BanyanDB.Group == "" {
			return fmt.Errorf("BanyanDB group not specified")
		}
	}

	return nil
}
