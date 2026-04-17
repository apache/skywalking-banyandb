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

package benchmark

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/apache/skywalking-banyandb/pkg/test"
)

func runBenchmarkRF(ctx context.Context, repoRoot string, cfg Config, rf int) (RFResult, error) {
	result := RFResult{ReplicationFactor: rf}
	if err := cfg.Validate(); err != nil {
		return result, err
	}
	namespace := fmt.Sprintf("banyandb-bench-rf-%d-%d", rf, time.Now().UnixNano())

	if err := installChart(ctx, repoRoot, namespace, cfg); err != nil {
		return result, err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Minute)
		defer cancel()
		_ = uninstallChart(cleanupCtx, namespace)
		_ = deleteNamespace(cleanupCtx, namespace)
	}()

	if _, err := runCommand(ctx, "kubectl", "-n", namespace, "wait", "--for=condition=ready", "pod", "--all", "--timeout=600s"); err != nil {
		return result, err
	}

	pods, err := fetchPods(ctx, namespace)
	if err != nil {
		return result, err
	}
	services, err := fetchServices(ctx, namespace)
	if err != nil {
		return result, err
	}
	dataPods := discoverDataPods(pods)
	liaisonPods := discoverLiaisonPods(pods)
	if len(dataPods) == 0 {
		return result, fmt.Errorf("no data pods discovered in namespace %s", namespace)
	}
	if len(liaisonPods) == 0 {
		return result, fmt.Errorf("no liaison pods discovered in namespace %s", namespace)
	}
	grpcService, err := discoverGRPCService(services)
	if err != nil {
		return result, err
	}

	grpcPorts, err := test.AllocateFreePorts(1)
	if err != nil {
		return result, err
	}
	grpcPF, err := startPortForward(ctx, namespace, "svc/"+grpcService.Metadata.Name, grpcPorts[0], grpcPort)
	if err != nil {
		return result, err
	}
	defer grpcPF.Stop()

	conn, err := connectGRPC(ctx, fmt.Sprintf("127.0.0.1:%d", grpcPorts[0]))
	if err != nil {
		return result, err
	}
	defer conn.Close()
	rfCtx := context.WithoutCancel(ctx)

	err = createMeasureSchema(rfCtx, conn, rf)
	if err != nil {
		return result, err
	}
	if err = waitForWriteReady(rfCtx, conn, cfg.Writers); err != nil {
		return result, err
	}
	baseTime := time.Now().Truncate(time.Second)

	liaisonEndpoints, liaisonStop, err := startMetricsPortForwards(ctx, namespace, liaisonPods)
	if err != nil {
		return result, err
	}
	defer liaisonStop()
	dataEndpoints, dataStop, err := startMetricsPortForwards(ctx, namespace, dataPods)
	if err != nil {
		return result, err
	}
	defer dataStop()

	writeStats, writeResources, err := runWritePhase(ctx, conn, cfg, baseTime, liaisonEndpoints, dataEndpoints)
	if err != nil {
		return result, err
	}
	result.Write = writeStats
	result.Resources.WritePhase = writeResources

	if err = waitForVisibility(ctx, conn, baseTime, cfg.PointsPerEntity); err != nil {
		return result, err
	}

	readStats, readResources, err := runReadPhase(rfCtx, conn, cfg, baseTime, liaisonEndpoints, dataEndpoints)
	if err != nil {
		return result, err
	}
	result.Read = readStats
	result.Resources.ReadPhase = readResources
	return result, nil
}

func startMetricsPortForwards(ctx context.Context, namespace string, pods []kubePod) ([]string, func(), error) {
	ports, err := test.AllocateFreePorts(len(pods))
	if err != nil {
		return nil, nil, err
	}
	endpoints := make([]string, 0, len(pods))
	portForwards := make([]*portForward, 0, len(pods))
	for i, pod := range pods {
		pf, err := startPortForward(ctx, namespace, "pod/"+pod.Metadata.Name, ports[i], 2121)
		if err != nil {
			for _, p := range portForwards {
				p.Stop()
			}
			return nil, nil, err
		}
		portForwards = append(portForwards, pf)
		endpoints = append(endpoints, fmt.Sprintf("http://127.0.0.1:%d/metrics", ports[i]))
	}
	stop := func() {
		for _, p := range portForwards {
			p.Stop()
		}
	}
	return endpoints, stop, nil
}

func runWritePhase(ctx context.Context, conn *grpc.ClientConn, cfg Config, base time.Time, liaisonEndpoints, dataEndpoints []string) (WriteResult, ResourcePhase, error) {
	lSeriesCh, lCancel := startCollector(ctx, cfg.MetricsPollInterval, liaisonEndpoints)
	dSeriesCh, dCancel := startCollector(ctx, cfg.MetricsPollInterval, dataEndpoints)

	writeResult, err := writeMeasureData(ctx, conn, cfg, base)
	lCancel()
	dCancel()
	lSeries := <-lSeriesCh
	dSeries := <-dSeriesCh
	if err != nil {
		return WriteResult{}, ResourcePhase{}, err
	}
	if lSeries.Err != nil {
		return WriteResult{}, ResourcePhase{}, lSeries.Err
	}
	if dSeries.Err != nil {
		return WriteResult{}, ResourcePhase{}, dSeries.Err
	}
	if lSeries.Series == nil || dSeries.Series == nil {
		return WriteResult{}, ResourcePhase{}, fmt.Errorf("collector returned nil series")
	}
	return writeResult, ResourcePhase{
		Liaison: toResourceStats(*lSeries.Series),
		Data:    toResourceStats(*dSeries.Series),
	}, nil
}

func runReadPhase(ctx context.Context, conn *grpc.ClientConn, cfg Config, base time.Time, liaisonEndpoints, dataEndpoints []string) (ReadResult, ResourcePhase, error) {
	lSeriesCh, lCancel := startCollector(ctx, cfg.MetricsPollInterval, liaisonEndpoints)
	dSeriesCh, dCancel := startCollector(ctx, cfg.MetricsPollInterval, dataEndpoints)

	latencies, err := runReadQueries(ctx, conn, cfg, base)
	lCancel()
	dCancel()
	lSeries := <-lSeriesCh
	dSeries := <-dSeriesCh
	if err != nil {
		return ReadResult{}, ResourcePhase{}, err
	}
	if lSeries.Err != nil {
		return ReadResult{}, ResourcePhase{}, lSeries.Err
	}
	if dSeries.Err != nil {
		return ReadResult{}, ResourcePhase{}, dSeries.Err
	}
	if lSeries.Series == nil || dSeries.Series == nil {
		return ReadResult{}, ResourcePhase{}, fmt.Errorf("collector returned nil series")
	}
	return summarizeLatencies(latencies), ResourcePhase{
		Liaison: toResourceStats(*lSeries.Series),
		Data:    toResourceStats(*dSeries.Series),
	}, nil
}

type collectorResult struct {
	Series *AggregatedSeries
	Err    error
}

func startCollector(parent context.Context, interval time.Duration, endpoints []string) (<-chan collectorResult, context.CancelFunc) {
	resultCh := make(chan collectorResult, 1)
	ctx, cancel := context.WithCancel(parent)
	go func() {
		series, err := collectSeries(ctx, interval, endpoints)
		resultCh <- collectorResult{Series: &series, Err: err}
		close(resultCh)
	}()
	return resultCh, cancel
}

func toResourceStats(series AggregatedSeries) ResourceStats {
	stats := summarizeSeries(series)
	return ResourceStats{
		PeakRSSBytes:   stats.PeakRSSBytes,
		PeakRSSPercent: stats.PeakRSSPercent,
		MeanCPUPercent: stats.MeanCPUPercent,
		PeakCPUPercent: stats.PeakCPUPercent,
	}
}
