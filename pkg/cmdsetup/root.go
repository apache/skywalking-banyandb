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

// Package cmdsetup implements a real env in which to run tests.
package cmdsetup

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/cgroups"
	"github.com/apache/skywalking-banyandb/pkg/config"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

const logo = `
鈻堚枅鈻堚枅鈻堚枅鈺? 鈻堚枅鈻堚枅鈻堚晽 鈻堚枅鈻堚晽   鈻堚枅鈺椻枅鈻堚晽   鈻堚枅鈺?鈻堚枅鈻堚枅鈻堚晽 鈻堚枅鈻堚晽   鈻堚枅鈺椻枅鈻堚枅鈻堚枅鈻堚晽 鈻堚枅鈻堚枅鈻堚枅鈺?
鈻堚枅鈺斺晲鈺愨枅鈻堚晽鈻堚枅鈺斺晲鈺愨枅鈻堚晽鈻堚枅鈻堚枅鈺? 鈻堚枅鈺戔暁鈻堚枅鈺?鈻堚枅鈺斺暆鈻堚枅鈺斺晲鈺愨枅鈻堚晽鈻堚枅鈻堚枅鈺? 鈻堚枅鈺戔枅鈻堚晹鈺愨晲鈻堚枅鈺椻枅鈻堚晹鈺愨晲鈻堚枅鈺?
鈻堚枅鈻堚枅鈻堚枅鈺斺暆鈻堚枅鈻堚枅鈻堚枅鈻堚晳鈻堚枅鈺斺枅鈻堚晽 鈻堚枅鈺?鈺氣枅鈻堚枅鈻堚晹鈺?鈻堚枅鈻堚枅鈻堚枅鈻堚晳鈻堚枅鈺斺枅鈻堚晽 鈻堚枅鈺戔枅鈻堚晳  鈻堚枅鈺戔枅鈻堚枅鈻堚枅鈻堚晹鈺?
鈻堚枅鈺斺晲鈺愨枅鈻堚晽鈻堚枅鈺斺晲鈺愨枅鈻堚晳鈻堚枅鈺戔暁鈻堚枅鈺椻枅鈻堚晳  鈺氣枅鈻堚晹鈺? 鈻堚枅鈺斺晲鈺愨枅鈻堚晳鈻堚枅鈺戔暁鈻堚枅鈺椻枅鈻堚晳鈻堚枅鈺? 鈻堚枅鈺戔枅鈻堚晹鈺愨晲鈻堚枅鈺?
鈻堚枅鈻堚枅鈻堚枅鈺斺暆鈻堚枅鈺? 鈻堚枅鈺戔枅鈻堚晳 鈺氣枅鈻堚枅鈻堚晳   鈻堚枅鈺?  鈻堚枅鈺? 鈻堚枅鈺戔枅鈻堚晳 鈺氣枅鈻堚枅鈻堚晳鈻堚枅鈻堚枅鈻堚枅鈺斺暆鈻堚枅鈻堚枅鈻堚枅鈺斺暆
鈺氣晲鈺愨晲鈺愨晲鈺?鈺氣晲鈺? 鈺氣晲鈺濃暁鈺愨暆  鈺氣晲鈺愨晲鈺?  鈺氣晲鈺?  鈺氣晲鈺? 鈺氣晲鈺濃暁鈺愨暆  鈺氣晲鈺愨晲鈺濃暁鈺愨晲鈺愨晲鈺愨暆 鈺氣晲鈺愨晲鈺愨晲鈺?
`

// NewRoot returns a root command.
func NewRoot(runners ...run.Unit) *cobra.Command {
	logging := logger.Logging{}
	cmd := &cobra.Command{
		DisableAutoGenTag: true,
		Version:           version.Build(),
		Short:             "BanyanDB is an observability database",
		Long: logo + `
BanyanDB, as an observability database, aims to ingest, analyze and store Metrics, Tracing and Logging data
`,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) (err error) {
			fmt.Print(logo)
			if err = config.Load("logging", cmd.Flags()); err != nil {
				return err
			}

			if err = logger.Init(logging); err != nil {
				return err
			}

			logger.Infof("CPU Number: %d", cgroups.CPUs())
			return nil
		},
	}
	cmd.PersistentFlags().Var(&nodeIDProviderValue{&common.FlagNodeHostProvider},
		"node-host-provider", "the node host provider, can be hostname, ip or flag, default is hostname")
	cmd.PersistentFlags().StringVar(&common.FlagNodeHost, "node-host", "", "the node host of the server only used when node-host-provider is \"flag\"")
	cmd.PersistentFlags().StringVar(&logging.Env, "logging-env", "prod", "the logging")
	cmd.PersistentFlags().StringVar(&logging.Level, "logging-level", "info", "the root level of logging")
	cmd.PersistentFlags().StringSliceVar(&logging.Modules, "logging-modules", nil, "the specific module")
	cmd.PersistentFlags().StringSliceVar(&logging.Levels, "logging-levels", nil, "the level logging of logging")
	cmd.AddCommand(newStandaloneCmd(runners...))
	cmd.AddCommand(newDataCmd(runners...))
	cmd.AddCommand(newLiaisonCmd(runners...))
	return cmd
}

type nodeIDProviderValue struct {
	value *common.NodeHostProvider
}

func (c *nodeIDProviderValue) Set(s string) error {
	v, err := common.ParseNodeHostProvider(s)
	if err != nil {
		return err
	}
	*c.value = v
	return nil
}

func (c *nodeIDProviderValue) String() string {
	return c.value.String()
}

func (c *nodeIDProviderValue) Type() string {
	return "nodeIDProvider"
}
