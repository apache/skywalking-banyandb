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
	"github.com/go-resty/resty/v2"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

const propertySchemaPath = "/api/v1/property/{group}/{name}/{id}"

var (
	id   string
	tags []string
	ids  []string
)

func newPropertyCmd() *cobra.Command {
	propertyCmd := &cobra.Command{
		Use:     "property",
		Version: version.Build(),
		Short:   "Property operation",
	}

	applyCmd := &cobra.Command{
		Use:     "apply -f [file|dir|-]",
		Version: version.Build(),
		Short:   "Apply(Create or Update) properties from files",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return rest(func() ([]reqBody, error) { return parseFromYAMLForProperty(cmd.InOrStdin()) },
				func(request request) (*resty.Response, error) {
					s := new(propertyv1.Property)
					err := protojson.Unmarshal(request.data, s)
					if err != nil {
						return nil, err
					}
					cr := &propertyv1.ApplyRequest{
						Property: s,
						Strategy: propertyv1.ApplyRequest_STRATEGY_MERGE,
					}
					b, err := protojson.Marshal(cr)
					if err != nil {
						return nil, err
					}
					return request.req.SetPathParam("group", request.group).SetPathParam("name", request.name).
						SetPathParam("id", request.id).SetBody(b).Put(getPath(propertySchemaPath))
				}, yamlPrinter, enableTLS, insecure, cert)
		},
	}

	deleteCmd := &cobra.Command{
		Use:     "delete -g group -n name [-i id]",
		Version: version.Build(),
		Short:   "Delete a property",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				return request.req.SetPathParam("name", request.name).SetPathParam("group", request.group).
					SetPathParam("id", request.id).Delete(getPath(propertySchemaPath))
			}, yamlPrinter, enableTLS, insecure, cert)
		},
	}
	bindNameAndIDFlag(deleteCmd)

	queryCmd := &cobra.Command{
		Use:     "query -f [file|dir|-]",
		Version: version.Build(),
		Short:   "Query properties from files",
		Long:    timeRangeUsage,
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			return rest(func() ([]reqBody, error) { return simpleParseFromYAML(cmd.InOrStdin()) },
				func(request request) (*resty.Response, error) {
					return request.req.SetBody(request.data).Post(getPath("/api/v1/property/query"))
				}, yamlPrinter, enableTLS, insecure, cert)
		},
	}
	bindFileFlag(applyCmd, queryCmd)

	bindTLSRelatedFlag(applyCmd, deleteCmd, queryCmd)
	propertyCmd.AddCommand(applyCmd, deleteCmd, queryCmd)
	return propertyCmd
}
