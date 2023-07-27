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

	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/config"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/signal"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

var metaGroup = run.NewGroup("meta")

func newMetaCmd() *cobra.Command {
	l := logger.GetLogger("bootstrap")
	ctx := context.Background()
	metaSvc, err := metadata.NewService(ctx)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate metadata service")
	}
	profSvc := observability.NewProfService()
	metricSvc := observability.NewMetricService()

	units := []run.Unit{
		new(signal.Handler),
		metaSvc,
		profSvc,
	}
	if metricSvc != nil {
		units = append(units, metricSvc)
	}
	// Meta the run Group units.
	metaGroup.Register(units...)
	logging := logger.Logging{}
	metaCmd := &cobra.Command{
		Use:     "meta",
		Version: version.Build(),
		Short:   "Run as the meta server",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			if err = config.Load("logging", cmd.Flags()); err != nil {
				return err
			}
			return logger.Init(logging)
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			fmt.Print(logo)
			logger.GetLogger().Info().Msg("starting as a meta server")
			// Spawn our go routines and wait for shutdown.
			if err := metaGroup.Run(); err != nil {
				logger.GetLogger().Error().Err(err).Stack().Str("name", metaGroup.Name()).Msg("Exit")
				os.Exit(-1)
			}
			return nil
		},
	}

	metaCmd.Flags().StringVar(&logging.Env, "logging-env", "prod", "the logging")
	metaCmd.Flags().StringVar(&logging.Level, "logging-level", "info", "the root level of logging")
	metaCmd.Flags().StringArrayVar(&logging.Modules, "logging-modules", nil, "the specific module")
	metaCmd.Flags().StringArrayVar(&logging.Levels, "logging-levels", nil, "the level logging of logging")
	metaCmd.Flags().AddFlagSet(metaGroup.RegisterFlags().FlagSet)
	return metaCmd
}
