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
	"github.com/apache/skywalking-banyandb/banyand/dquery"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/liaison/http"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

func newLiaisonCmd(runners ...run.Unit) *cobra.Command {
	l := logger.GetLogger("bootstrap")
	ctx := context.Background()
	metaSvc, err := metadata.NewClient(false)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate metadata service")
	}
	pipeline := pub.New(metaSvc)
	localPipeline := queue.Local()
	nodeSel, err := node.NewMaglevSelector()
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate required node selector")
	}
	grpcServer := grpc.NewServer(ctx, pipeline, localPipeline, metaSvc, grpc.NewClusterNodeRegistry(pipeline, nodeSel))
	profSvc := observability.NewProfService()
	metricSvc := observability.NewMetricService()
	httpServer := http.NewServer()
	dQuery, err := dquery.NewService(metaSvc, localPipeline, pipeline)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate distributed query service")
	}
	var units []run.Unit
	units = append(units, runners...)
	units = append(units,
		metaSvc,
		localPipeline,
		pipeline,
		dQuery,
		grpcServer,
		httpServer,
		profSvc,
	)
	if metricSvc != nil {
		units = append(units, metricSvc)
	}
	liaisonGroup := run.NewGroup("liaison")
	liaisonGroup.Register(units...)
	liaisonCmd := &cobra.Command{
		Use:     "liaison",
		Version: version.Build(),
		Short:   "Run as the liaison server",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			node, err := common.GenerateNode(grpcServer.GetPort(), httpServer.GetPort())
			if err != nil {
				return err
			}
			logger.GetLogger().Info().Msg("starting as a liaison server")
			// Spawn our go routines and wait for shutdown.
			if err := liaisonGroup.Run(context.WithValue(context.Background(), common.ContextNodeKey, node)); err != nil {
				logger.GetLogger().Error().Err(err).Stack().Str("name", liaisonGroup.Name()).Msg("Exit")
				os.Exit(-1)
			}
			return nil
		},
	}
	liaisonCmd.Flags().AddFlagSet(liaisonGroup.RegisterFlags().FlagSet)
	return liaisonCmd
}
