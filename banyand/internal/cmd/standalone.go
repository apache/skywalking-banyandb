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
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	executor2 "github.com/apache/skywalking-banyandb/banyand/executor"
	index2 "github.com/apache/skywalking-banyandb/banyand/index"
	series2 "github.com/apache/skywalking-banyandb/banyand/series"
	shard2 "github.com/apache/skywalking-banyandb/banyand/shard"
	"github.com/apache/skywalking-banyandb/banyand/storage"
	"github.com/apache/skywalking-banyandb/pkg/config"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/signal"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

var (
	g = run.Group{Name: "standalone"}
)

func newStandaloneCmd() *cobra.Command {
	_ = logger.Bootstrap()
	engine := new(storage.Pipeline)
	shard := new(shard2.Shard)
	executor := new(executor2.Executor)
	index := new(index2.Index)
	series := new(series2.Series)

	// Register the storage engine components.
	engine.Register(
		shard,
		executor,
		index,
		series,
	)

	// Register the run Group units.
	g.Register(
		new(signal.Handler),
		engine,
		shard,
		executor,
		index,
		series,
	)
	logging := logger.Logging{}
	standaloneCmd := &cobra.Command{
		Use:     "standalone",
		Version: version.Build(),
		Short:   "Run as the standalone mode",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			if err = config.Load("logging", cmd.Flags()); err != nil {
				return err
			}
			return logger.Init(logging)
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			logger.GetLogger().Info("starting as a standalone server")
			// Spawn our go routines and wait for shutdown.
			if err := g.Run(); err != nil {
				logger.GetLogger().WithOptions(zap.AddStacktrace(zap.FatalLevel)).
					Error("exit: ", logger.String("name", g.Name), logger.Error(err))
				os.Exit(-1)
			}
			return nil
		},
	}

	standaloneCmd.Flags().StringVarP(&logging.Env, "logging.env", "", "dev", "the logging")
	standaloneCmd.Flags().StringVarP(&logging.Level, "logging.level", "", "debug", "the level of logging")
	standaloneCmd.Flags().AddFlagSet(g.RegisterFlags().FlagSet)
	return standaloneCmd
}
