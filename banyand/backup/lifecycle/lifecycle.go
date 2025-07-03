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

	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/pkg/config"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/signal"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

// NewCommand creates a new lifecycle command.
func NewCommand() *cobra.Command {
	logging := logger.Logging{}
	metaSvc, err := metadata.NewClient(false, false)
	if err != nil {
		logger.GetLogger().Err(err).Msg("failed to initiate metadata service")
	}
	svc := NewService(metaSvc)
	group := run.NewGroup("lifecycle")
	group.Register(new(signal.Handler), metaSvc, svc)
	cmd := &cobra.Command{
		Short:             "Run lifecycle migration",
		DisableAutoGenTag: true,
		Version:           version.Build(),
		RunE: func(cmd *cobra.Command, _ []string) error {
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
			if err := group.Run(context.Background()); err != nil {
				logger.GetLogger().Error().Err(err).Stack().Str("name", group.Name()).Msg("Exit")
				os.Exit(-1)
			}
			return nil
		},
	}
	cmd.Flags().AddFlagSet(group.RegisterFlags().FlagSet)
	cmd.Flags().StringVar(&logging.Env, "logging-env", "prod", "the logging")
	cmd.Flags().StringVar(&logging.Level, "logging-level", "info", "the root level of logging")
	return cmd
}
