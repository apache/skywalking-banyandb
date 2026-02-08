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
	"github.com/apache/skywalking-banyandb/api/data"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/dquery"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc/route"
	"github.com/apache/skywalking-banyandb/banyand/liaison/http"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	metadatclient "github.com/apache/skywalking-banyandb/banyand/metadata/client"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/banyand/queue/sub"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

func newLiaisonCmd(runners ...run.Unit) *cobra.Command {
	l := logger.GetLogger("bootstrap")
	ctx := context.Background()
	metaSvc, err := metadatclient.NewClient(true, false)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate metadata service")
	}
	tire1Client := pub.New(metaSvc, databasev1.Role_ROLE_LIAISON)
	tire2Client := pub.New(metaSvc, databasev1.Role_ROLE_DATA)
	metaSvc.SetDataBroadcaster(tire2Client)
	metaSvc.SetLiaisonBroadcaster(tire1Client)

	localPipeline := queue.Local()

	measureLiaisonNodeSel := node.NewRoundRobinSelector(data.TopicMeasureWrite.String(), metaSvc)
	measureLiaisonNodeRegistry := grpc.NewClusterNodeRegistry(data.TopicMeasureWrite, tire1Client, measureLiaisonNodeSel)
	measureDataNodeSel := node.NewRoundRobinSelector(data.TopicMeasureWrite.String(), metaSvc)
	metricSvc := services.NewMetricService(metaSvc, tire1Client, "liaison", measureLiaisonNodeRegistry)
	metaSvc.SetMetricsRegistry(metricSvc)
	pm := protector.NewMemory(metricSvc)
	internalPipeline := sub.NewServerWithPorts(metricSvc, "liaison-server", 18912, 18913)
	measureSVC, err := measure.NewLiaison(metaSvc, internalPipeline, metricSvc, pm, measureDataNodeSel, tire2Client)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate measure liaison service")
	}

	streamLiaisonNodeSel := node.NewRoundRobinSelector(data.TopicStreamWrite.String(), metaSvc)
	streamDataNodeSel := node.NewRoundRobinSelector(data.TopicStreamPartSync.String(), metaSvc)
	streamSVC, err := stream.NewLiaison(metaSvc, internalPipeline, metricSvc, pm, streamDataNodeSel, tire2Client)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate stream liaison service")
	}

	traceLiaisonNodeSel := node.NewRoundRobinSelector(data.TopicTraceWrite.String(), metaSvc)
	traceDataNodeSel := node.NewRoundRobinSelector(data.TopicTracePartSync.String(), metaSvc)
	traceSVC, err := trace.NewLiaison(metaSvc, internalPipeline, metricSvc, pm, traceDataNodeSel, tire2Client)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate trace liaison service")
	}

	propertyNodeSel := node.NewRoundRobinSelector(data.TopicPropertyUpdate.String(), metaSvc)

	dQuery, err := dquery.NewService(metaSvc, localPipeline, tire2Client, metricSvc, streamSVC, measureSVC, traceSVC)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate distributed query service")
	}

	routeProviders := map[string]route.TableProvider{
		"tire1": tire1Client,
		"tire2": tire2Client,
	}

	grpcServer := grpc.NewServer(ctx, tire1Client, tire2Client, localPipeline, metaSvc, grpc.NodeRegistries{
		MeasureLiaisonNodeRegistry: measureLiaisonNodeRegistry,
		StreamLiaisonNodeRegistry:  grpc.NewClusterNodeRegistry(data.TopicStreamWrite, tire1Client, streamLiaisonNodeSel),
		PropertyNodeRegistry:       grpc.NewClusterNodeRegistry(data.TopicPropertyUpdate, tire2Client, propertyNodeSel),
		TraceLiaisonNodeRegistry:   grpc.NewClusterNodeRegistry(data.TopicTraceWrite, tire1Client, traceLiaisonNodeSel),
	}, metricSvc, pm, routeProviders)
	profSvc := observability.NewProfService()
	httpServer := http.NewServer(grpcServer.GetAuthReloader())
	var units []run.Unit
	units = append(units, runners...)
	units = append(units,
		metricSvc,
		metaSvc,
		localPipeline,
		internalPipeline,
		tire1Client,
		tire2Client,
		measureLiaisonNodeSel,
		measureDataNodeSel,
		streamLiaisonNodeSel,
		streamDataNodeSel,
		traceLiaisonNodeSel,
		traceDataNodeSel,
		propertyNodeSel,
		streamSVC,
		measureSVC,
		traceSVC,
		dQuery,
		grpcServer,
		httpServer,
		profSvc,
	)
	liaisonGroup := run.NewGroup("liaison")
	liaisonGroup.Register(units...)
	var nodeSelector string
	liaisonCmd := &cobra.Command{
		Use:     "liaison",
		Version: version.Build(),
		Short:   "Run as the liaison server",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			ctx := context.Background()
			if nodeSelector != "" {
				ctx = context.WithValue(ctx, common.ContextNodeSelectorKey, nodeSelector)
				var ls *pub.LabelSelector
				ls, err = pub.ParseLabelSelector(nodeSelector)
				if err != nil {
					return err
				}
				for _, sel := range []node.Selector{measureDataNodeSel, streamDataNodeSel, propertyNodeSel, traceDataNodeSel} {
					sel.SetNodeSelector(ls)
				}
			}
			node, err := common.GenerateNode(internalPipeline.GetPort(), httpServer.GetPort(), nil)
			if err != nil {
				return err
			}
			logger.GetLogger().Info().Msg("starting as a liaison server")
			ctx = context.WithValue(ctx, common.ContextNodeKey, node)
			// Spawn our go routines and wait for shutdown.
			if err := liaisonGroup.Run(ctx); err != nil {
				logger.GetLogger().Error().Err(err).Stack().Str("name", liaisonGroup.Name()).Msg("Exit")
				os.Exit(-1)
			}
			return nil
		},
	}
	liaisonCmd.Flags().AddFlagSet(liaisonGroup.RegisterFlags().FlagSet)
	liaisonCmd.PersistentFlags().StringVar(&nodeSelector, "data-node-selector", "",
		"the data node selector. e.g. key1=value1,key2=value2. If not set, all nodes are selected")
	return liaisonCmd
}
