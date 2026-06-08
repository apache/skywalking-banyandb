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

// Package lifecycle provides the lifecycle migration service.
package lifecycle

import (
	"context"
	"os"

	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/pkg/config"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/signal"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

// NewCommand creates a new lifecycle command.
func NewCommand() *cobra.Command {
	cmd, _ := NewCommandWithRegistry()
	return cmd
}

// NewCommandWithRegistry creates a new lifecycle command and also returns the
// metrics registry it wires. Tests and embedders use the registry to inspect the
// lifecycle and banyandb_lifecycle_migration_* metric families (e.g. via its
// observability.PrometheusHandlerProvider) without scraping a live HTTP port.
func NewCommandWithRegistry() (*cobra.Command, observability.MetricsRegistry) {
	logging := logger.Logging{}
	crashOutputCfg := panicdiag.NewCrashOutputConfig()
	metaSvc, err := metadata.NewClient()
	if err != nil {
		logger.GetLogger().Err(err).Msg("failed to initiate metadata service")
	}
	// Native metrics pipeline: a queue client that publishes _monitoring measure
	// writes to the co-located data node over gRPC. It is created idle here (no
	// dial until a node is registered); the lifecycle service registers the local
	// data node — with its --grpc-addr, known only after flag parsing — during its
	// Serve phase when native mode is enabled.
	// nil migration registry: this client carries native _monitoring writes, not
	// tier-migration traffic, so it must not emit the banyandb_lifecycle_migration_* family.
	metricsClient := pub.NewWithoutMetadata(nil)
	nodeSelector, _ := node.NewPickFirstSelector()
	nodeRegistry := grpc.NewClusterNodeRegistry(data.TopicMeasureWrite, metricsClient, nodeSelector)
	// Listener-suppressed: the lifecycle reuses its own HTTP port for /metrics
	// instead of opening the observability :2121 listener.
	metricSvc := services.NewMetricServiceWithoutListener(metaSvc, metricsClient, "lifecycle", nodeRegistry)
	svc := NewService(metaSvc, metricSvc, metricsClient, nodeRegistry)
	group := run.NewGroup("lifecycle")
	// metricSvc is registered before svc so its PreRun (building the providers)
	// runs first; svc.PreRun then safely derives its proof counter from it.
	group.Register(new(signal.Handler), metaSvc, metricSvc, svc)
	cmd := &cobra.Command{
		Short:             "Run lifecycle migration",
		DisableAutoGenTag: true,
		Version:           version.Build(),
		RunE: func(cmd *cobra.Command, _ []string) error {
			if installErr := crashOutputCfg.InstallGlobalCrashOutput(); installErr != nil {
				return installErr
			}
			if err = config.Load("logging", cmd.Flags()); err != nil {
				return err
			}
			if err = logger.Init(logging); err != nil {
				return err
			}
			defer func() {
				if err := recover(); err != nil {
					logger.GetLogger().Error().Msgf("panic occurred: %v", err)
					os.Exit(-1)
				}
			}()
			runCtx := context.Background()
			if metricSvc.NativeEnabled() {
				// Native mode stamps the lifecycle node identity onto every
				// _monitoring series; the metric service reads it from the context.
				runCtx = nativeNodeContext(runCtx)
			}
			if err := group.Run(runCtx); err != nil {
				logger.GetLogger().Error().Err(err).Stack().Str("name", group.Name()).Msg("Exit")
				os.Exit(-1)
			}
			return nil
		},
	}
	cmd.Flags().AddFlagSet(group.RegisterFlags().FlagSet)
	cmd.Flags().StringVar(&logging.Env, "logging-env", "prod", "the logging")
	cmd.Flags().StringVar(&logging.Level, "logging-level", "info", "the root level of logging")
	crashOutputCfg.RegisterFlags(cmd.Flags())
	return cmd, metricSvc
}
