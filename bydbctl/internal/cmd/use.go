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
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/apache/skywalking-banyandb/pkg/version"
)

func newUserCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "use group",
		Version: version.Build(),
		Short:   "Select a group",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			viper.Set("group", args[0])
			err = viper.WriteConfig()
			if err != nil {
				return err
			}
			fmt.Printf("Switched to [%s]", viper.GetString("group"))
			return nil
		},
	}
}
