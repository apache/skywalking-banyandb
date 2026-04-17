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

package benchmark

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	defaultChartRef     = "oci://registry-1.docker.io/apache/skywalking-banyandb-helm"
	defaultChartVersion = "0.5.3"
	defaultReportDir    = ""
	defaultWriters      = 4
	defaultEntities     = 200
	defaultPoints       = 30
	defaultQueryWorkers = 4
	defaultQueryIters   = 200
	defaultMetricsIntv  = 5 * time.Second
)

// Config captures benchmark configuration from env vars.
type Config struct {
	ChartRef            string
	ChartVersion        string
	ReportDir           string
	Writers             int
	Entities            int
	PointsPerEntity     int
	QueryWorkers        int
	QueryIterations     int
	MetricsPollInterval time.Duration
}

// LoadConfig reads benchmark settings from environment variables.
func LoadConfig() Config {
	return Config{
		ChartRef:            getEnvString("BANYANDB_BENCH_CHART", defaultChartRef),
		ChartVersion:        getEnvString("BANYANDB_BENCH_CHART_VERSION", defaultChartVersion),
		ReportDir:           getEnvString("BANYANDB_BENCH_REPORT_DIR", defaultReportDir),
		Writers:             getEnvInt("BENCH_WRITERS", defaultWriters),
		Entities:            getEnvInt("BENCH_ENTITIES", defaultEntities),
		PointsPerEntity:     getEnvInt("BENCH_POINTS_PER_ENTITY", defaultPoints),
		QueryWorkers:        getEnvInt("BENCH_QUERY_WORKERS", defaultQueryWorkers),
		QueryIterations:     getEnvInt("BENCH_QUERY_ITERATIONS", defaultQueryIters),
		MetricsPollInterval: getEnvDuration("BENCH_METRICS_INTERVAL", defaultMetricsIntv),
	}
}

// Validate checks configuration values before benchmark execution.
func (c Config) Validate() error {
	if c.ChartRef == "" {
		return fmt.Errorf("BANYANDB_BENCH_CHART must not be empty")
	}
	if c.Writers <= 0 {
		return fmt.Errorf("BENCH_WRITERS must be > 0")
	}
	if c.Entities <= 0 {
		return fmt.Errorf("BENCH_ENTITIES must be > 0")
	}
	if c.PointsPerEntity <= 0 {
		return fmt.Errorf("BENCH_POINTS_PER_ENTITY must be > 0")
	}
	if c.QueryWorkers <= 0 {
		return fmt.Errorf("BENCH_QUERY_WORKERS must be > 0")
	}
	if c.QueryIterations <= 0 {
		return fmt.Errorf("BENCH_QUERY_ITERATIONS must be > 0")
	}
	if c.MetricsPollInterval <= 0 {
		return fmt.Errorf("BENCH_METRICS_INTERVAL must be > 0")
	}
	return nil
}

func getEnvString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			return parsed
		}
	}
	return def
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if parsed, err := time.ParseDuration(v); err == nil {
			return parsed
		}
	}
	return def
}
