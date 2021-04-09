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
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/banyand/config"
	"github.com/apache/skywalking-banyandb/banyand/executor"
	"github.com/apache/skywalking-banyandb/banyand/index"
	"github.com/apache/skywalking-banyandb/banyand/internal/bus"
	"github.com/apache/skywalking-banyandb/banyand/series"
	"github.com/apache/skywalking-banyandb/banyand/shard"
	"github.com/apache/skywalking-banyandb/banyand/storage"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

var standAloneConfig config.Standalone

func newStandaloneCmd() *cobra.Command {
	standaloneCmd := &cobra.Command{
		Use:     "standalone",
		Version: version.Build(),
		Short:   "Run as the standalone mode",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			if standAloneConfig, err = config.Load(); err != nil {
				return err
			}
			if err = logger.InitLogger(standAloneConfig.Logging); err != nil {
				return err
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			logger.GetLogger().Info("starting as a standalone server")
			dataBus := bus.NewBus()
			err = multierr.Append(err, dataBus.Subscribe(storage.TraceRaw, shard.NewShard(dataBus)))
			err = multierr.Append(err, dataBus.Subscribe(storage.TraceSharded, executor.NewExecutor(dataBus)))
			err = multierr.Append(err, dataBus.Subscribe(storage.TraceIndex, index.NewIndex()))
			err = multierr.Append(err, dataBus.Subscribe(storage.TraceData, series.NewSeries()))
			if err != nil {
				return err
			}
			if err = dataBus.Publish(storage.TraceRaw, bus.NewMessage(0, "initialization")); err != nil {
				return err
			}
			ctx := newContext()
			<-ctx.Done()
			return nil
		},
	}

	return standaloneCmd
}

func newContext() context.Context {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		select {
		case <-ctx.Done():
			return
		case <-c:
			return
		}
	}()
	return ctx
}
