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
	"os/signal"
	"syscall"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/robfig/cron/v3"
	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/config"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

// NewLifecycleCommand creates a new lifecycle command.
func NewLifecycleCommand() *cobra.Command {
	var schedule string

	l := logger.GetLogger("bootstrap")

	metaSvc, err := metadata.NewClient(false)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate metadata service")
	}
	metricSvc := observability.NewMetricService(metaSvc, nil, "lifecycle", nil)
	svc := NewService(metaSvc, metricSvc)
	group := run.NewGroup("lifecycle")
	group.Register(metaSvc, metricSvc, svc)

	cmd := &cobra.Command{
		Short:             "Backup BanyanDB snapshots to remote storage",
		DisableAutoGenTag: true,
		Version:           version.Build(),
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := config.Load("logging", cmd.Flags()); err != nil {
				return err
			}
			if schedule == "" {
				return group.Run(context.Background())
			}
			schedLogger := logger.GetLogger().Named("lifecycle-scheduler")
			schedLogger.Info().Msgf("lifecycle migration will run with schedule: %s", schedule)
			clockInstance := clock.New()
			sch := timestamp.NewScheduler(schedLogger, clockInstance)
			err := sch.Register("lifecycle", cron.Descriptor, schedule, func(_ time.Time, l *logger.Logger) bool {
				err := group.Run(context.Background())
				if err != nil {
					l.Error().Err(err).Msg("lifecycle migration failed")
				} else {
					l.Info().Msg("lifecycle migration succeeded")
				}
				return true
			})
			if err != nil {
				return err
			}

			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
			schedLogger.Info().Msg("backup scheduler started, press Ctrl+C to exit")
			<-sigChan
			schedLogger.Info().Msg("shutting down backup scheduler...")
			sch.Close()
			return nil
		},
	}

	cmd.Flags().StringVar(
		&schedule,
		"schedule",
		"",
		"Schedule expression for periodic backup. Options: @yearly, @monthly, @weekly, @daily, @hourly or @every <duration>",
	)

	return cmd
}
