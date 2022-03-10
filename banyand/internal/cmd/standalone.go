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
	"os"

	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/liaison"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/prof"
	"github.com/apache/skywalking-banyandb/banyand/query"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/config"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/signal"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

var (
	g = run.Group{Name: "standalone"}
)

func newStandaloneCmd() *cobra.Command {
	_ = logger.Bootstrap()
	l := logger.GetLogger("bootstrap")
	ctx := context.Background()
	repo, err := discovery.NewServiceRepo(ctx)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate service repository")
	}
	pipeline, err := queue.NewQueue(ctx, repo)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate data pipeline")
	}
	metaSvc, err := metadata.NewService(ctx)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate metadata service")
	}
	streamSvc, err := stream.NewService(ctx, metaSvc, repo, pipeline)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate stream service")
	}
	measureSvc, err := measure.NewService(ctx, metaSvc, repo, pipeline)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate measure service")
	}
	q, err := query.NewExecutor(ctx, streamSvc, measureSvc, metaSvc, repo, pipeline)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate query processor")
	}
	tcp, err := liaison.NewEndpoint(ctx, pipeline, repo, metaSvc)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate Endpoint transport layer")
	}
	profSvc := prof.NewProfService()

	// Meta the run Group units.
	g.Register(
		new(signal.Handler),
		repo,
		pipeline,
		metaSvc,
		measureSvc,
		streamSvc,
		q,
		tcp,
		profSvc,
	)
	logging := logger.Logging{}
	standaloneCmd := &cobra.Command{
		Use:     "standalone",
		Version: version.Build(),
		Short:   "Run as the standalone mode",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			if err = config.Load("logging", cmd.Flags()); err != nil {
				return err
			}
			return logger.Init(logging)
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			logger.GetLogger().Info().Msg("starting as a standalone server")
			// Spawn our go routines and wait for shutdown.
			if err := g.Run(); err != nil {
				logger.GetLogger().Error().Err(err).Stack().Str("name", g.Name).Msg("Exit")
				os.Exit(-1)
			}
			return nil
		},
	}

	standaloneCmd.Flags().StringVarP(&logging.Env, "logging.env", "", "dev", "the logging")
	standaloneCmd.Flags().StringVarP(&logging.Level, "logging.level", "", "debug", "the level of logging")
	standaloneCmd.Flags().AddFlagSet(g.RegisterFlags().FlagSet)
	return standaloneCmd
}
