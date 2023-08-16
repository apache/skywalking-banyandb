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

	"github.com/apache/skywalking-banyandb/api/common"
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

var storageGroup = run.NewGroup("storage")

const (
	storageModeData  = "data"
	storageModeQuery = "query"
	storageModeMix   = "mix"
)

var flagStorageMode string

func newStorageCmd() *cobra.Command {
	l := logger.GetLogger("bootstrap")
	ctx := context.Background()
	// nolint: staticcheck
	pipeline, err := queue.NewQueue(ctx)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate data pipeline")
	}
	metaSvc, err := metadata.NewClient(ctx)
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
	// TODO: remove streamSVC and measureSvc from query processor. To use metaSvc instead.
	q, err := query.NewService(ctx, streamSvc, measureSvc, metaSvc, pipeline)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate query processor")
	}
	profSvc := observability.NewProfService()
	metricSvc := observability.NewMetricService()

	units := []run.Unit{
		new(signal.Handler),
		pipeline,
		measureSvc,
		streamSvc,
		q,
		profSvc,
	}
	if metricSvc != nil {
		units = append(units, metricSvc)
	}
	// Meta the run Group units.
	storageGroup.Register(units...)
	logging := logger.Logging{}
	storageCmd := &cobra.Command{
		Use:     "storage",
		Version: version.Build(),
		Short:   "Run as the storage server",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			if err = config.Load("logging", cmd.Flags()); err != nil {
				return err
			}
			return logger.Init(logging)
		},
		PreRun: func(cmd *cobra.Command, args []string) {
			if flagStorageMode == storageModeMix {
				return
			}
			switch flagStorageMode {
			case storageModeData:
				storageGroup.Deregister(q)
			case storageModeQuery:
				storageGroup.Deregister(streamSvc)
				storageGroup.Deregister(measureSvc)
			default:
				l.Fatal().Str("mode", flagStorageMode).Msg("unknown storage mode")
			}
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			node, err := common.GenerateNode(nil, nil)
			if err != nil {
				return err
			}
			logger.GetLogger().Info().Msg("starting as a storage server")
			// Spawn our go routines and wait for shutdown.
			if err := storageGroup.Run(context.WithValue(context.Background(), common.ContextNodeKey, node)); err != nil {
				logger.GetLogger().Error().Err(err).Stack().Str("name", storageGroup.Name()).Msg("Exit")
				os.Exit(-1)
			}
			return nil
		},
	}
	storageCmd.Flags().StringVarP(&flagStorageMode, "mode", "m", storageModeMix, "the storage mode, one of [data, query, mix]")
	storageCmd.Flags().AddFlagSet(storageGroup.RegisterFlags().FlagSet)
	return storageCmd
}
