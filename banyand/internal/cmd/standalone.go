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

package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/liaison"
	"github.com/apache/skywalking-banyandb/banyand/liaison/http"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/query"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/config"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/signal"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

var standaloneGroup = run.NewGroup("standalone")

func newStandaloneCmd() *cobra.Command {
	l := logger.GetLogger("bootstrap")
	ctx := context.Background()
	pipeline, err := queue.NewQueue(ctx)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate data pipeline")
	}
	metaSvc, err := metadata.NewService(ctx)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate metadata service")
	}
	streamSvc, err := stream.NewService(ctx, metaSvc, pipeline)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate stream service")
	}
	measureSvc, err := measure.NewService(ctx, metaSvc, pipeline)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate measure service")
	}
	q, err := query.NewService(ctx, streamSvc, measureSvc, metaSvc, pipeline)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate query processor")
	}
	tcp, err := liaison.NewEndpoint(ctx, pipeline, metaSvc)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate Endpoint transport layer")
	}
	profSvc := observability.NewProfService()
	metricSvc := observability.NewMetricService()
	httpServer := http.NewService()

	units := []run.Unit{
		new(signal.Handler),
		pipeline,
		metaSvc,
		measureSvc,
		streamSvc,
		q,
		tcp,
		httpServer,
		profSvc,
	}
	if metricSvc != nil {
		units = append(units, metricSvc)
	}
	// Meta the run Group units.
	standaloneGroup.Register(units...)
	logging := logger.Logging{}

	var flagNodeID string
	standaloneCmd := &cobra.Command{
		Use:     "standalone",
		Version: version.Build(),
		Short:   "Run as the standalone server",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			if err = config.Load("logging", cmd.Flags()); err != nil {
				return err
			}
			return logger.Init(logging)
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if flagNodeID == "" {
				return fmt.Errorf("data node id is required")
			}
			nodeID, err := common.GenerateNodeID("standalone", flagNodeID)
			if err != nil {
				return err
			}
			fmt.Print(logo)
			logger.GetLogger().Info().Msg("starting as a standalone server")
			// Spawn our go routines and wait for shutdown.
			if err := standaloneGroup.Run(context.WithValue(context.Background(), common.ContextNodeIDKey, nodeID)); err != nil {
				logger.GetLogger().Error().Err(err).Stack().Str("name", standaloneGroup.Name()).Msg("Exit")
				os.Exit(-1)
			}
			return nil
		},
	}

	standaloneCmd.Flags().StringVar(&flagNodeID, "data-node-id", "single-node", "the data node id of the standalone server")
	standaloneCmd.Flags().StringVar(&logging.Env, "logging-env", "prod", "the logging")
	standaloneCmd.Flags().StringVar(&logging.Level, "logging-level", "info", "the root level of logging")
	standaloneCmd.Flags().StringArrayVar(&logging.Modules, "logging-modules", nil, "the specific module")
	standaloneCmd.Flags().StringArrayVar(&logging.Levels, "logging-levels", nil, "the level logging of logging")
	standaloneCmd.Flags().AddFlagSet(standaloneGroup.RegisterFlags().FlagSet)
	return standaloneCmd
}
