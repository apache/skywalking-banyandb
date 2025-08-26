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

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/liaison/http"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedserver"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/property"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/query"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

func newStandaloneCmd(runners ...run.Unit) *cobra.Command {
	l := logger.GetLogger("bootstrap")
	ctx := context.Background()
	dataPipeline := queue.Local()
	metaSvc, err := embeddedserver.NewService(ctx)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate metadata service")
	}
	metricSvc := observability.NewMetricService(metaSvc, dataPipeline, "standalone", nil)
	pm := protector.NewMemory(metricSvc)
	propertySvc, err := property.NewService(metaSvc, dataPipeline, nil, metricSvc, pm)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate property service")
	}
	streamSvc, err := stream.NewService(metaSvc, dataPipeline, metricSvc, pm, nil)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate stream service")
	}
	var srvMetrics *grpcprom.ServerMetrics
	srvMetrics.UnaryServerInterceptor()
	srvMetrics.UnaryServerInterceptor()
	measureSvc, err := measure.NewStandalone(metaSvc, dataPipeline, nil, metricSvc, pm)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate measure service")
	}
	q, err := query.NewService(ctx, streamSvc, measureSvc, metaSvc, dataPipeline)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate query processor")
	}
	nr := grpc.NewLocalNodeRegistry()
	grpcServer := grpc.NewServer(ctx, dataPipeline, dataPipeline, dataPipeline, metaSvc, grpc.NodeRegistries{
		MeasureLiaisonNodeRegistry: nr,
		StreamLiaisonNodeRegistry:  nr,
		PropertyNodeRegistry:       nr,
	}, metricSvc)
	profSvc := observability.NewProfService()
	httpServer := http.NewServer(grpcServer.GetAuthReloader())

	var units []run.Unit
	units = append(units, runners...)
	units = append(units,
		dataPipeline,
		metaSvc,
		metricSvc,
		pm,
		propertySvc,
		measureSvc,
		streamSvc,
		q,
		grpcServer,
		httpServer,
		profSvc,
	)
	standaloneGroup := run.NewGroup("standalone")
	// Meta the run Group units.
	standaloneGroup.Register(units...)

	standaloneCmd := &cobra.Command{
		Use:     "standalone",
		Version: version.Build(),
		Short:   "Run as the standalone server",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			nodeID, err := common.GenerateNode(grpcServer.GetPort(), httpServer.GetPort(), nil)
			if err != nil {
				return err
			}
			logger.GetLogger().Info().Msg("starting as a standalone server")
			// Spawn our go routines and wait for shutdown.
			if err := standaloneGroup.Run(context.WithValue(context.Background(), common.ContextNodeKey, nodeID)); err != nil {
				logger.GetLogger().Error().Err(err).Stack().Str("name", standaloneGroup.Name()).Msg("Exit")
				os.Exit(-1)
			}
			return nil
		},
	}
	standaloneCmd.Flags().AddFlagSet(standaloneGroup.RegisterFlags().FlagSet)
	return standaloneCmd
}
