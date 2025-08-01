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

package cmdsetup

import (
	"context"
	"os"

	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/property"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/query"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/queue/sub"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

func newDataCmd(runners ...run.Unit) *cobra.Command {
	l := logger.GetLogger("bootstrap")
	ctx := context.Background()
	metaSvc, err := metadata.NewClient(true, false)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate metadata service")
	}
	metricsPipeline := queue.Local()
	metricSvc := observability.NewMetricService(metaSvc, metricsPipeline, "data", nil)
	pm := protector.NewMemory(metricSvc)
	pipeline := sub.NewServer(metricSvc)
	propertySvc, err := property.NewService(metaSvc, pipeline, metricSvc, pm)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate property service")
	}
	streamSvc, err := stream.NewService(metaSvc, pipeline, metricSvc, pm)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate stream service")
	}
	measureSvc, err := measure.NewDataSVC(metaSvc, pipeline, metricsPipeline, metricSvc, pm)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate measure service")
	}
	q, err := query.NewService(ctx, streamSvc, measureSvc, metaSvc, pipeline)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate query processor")
	}
	profSvc := observability.NewProfService()

	var units []run.Unit
	units = append(units, runners...)
	units = append(units,
		metaSvc,
		metricsPipeline,
		metricSvc,
		pm,
		pipeline,
		propertySvc,
		measureSvc,
		streamSvc,
		q,
		profSvc,
	)
	dataGroup := run.NewGroup("data")
	dataGroup.Register(units...)
	dataCmd := &cobra.Command{
		Use:     "data",
		Version: version.Build(),
		Short:   "Run as the data server",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			node, err := common.GenerateNode(pipeline.GetPort(), nil, propertySvc.GetGossIPGrpcPort())
			if err != nil {
				return err
			}
			logger.GetLogger().Info().Msg("starting as a data server")
			// Spawn our go routines and wait for shutdown.
			if err := dataGroup.Run(context.WithValue(context.Background(), common.ContextNodeKey, node)); err != nil {
				logger.GetLogger().Error().Err(err).Stack().Str("name", dataGroup.Name()).Msg("Exit")
				os.Exit(-1)
			}
			return nil
		},
	}
	dataCmd.Flags().AddFlagSet(dataGroup.RegisterFlags().FlagSet)
	return dataCmd
}
