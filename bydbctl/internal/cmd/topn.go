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

	"github.com/go-resty/resty/v2"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

const topnSchemaPath = "/api/v1/topn-agg/schema"

var topnSchemaPathWithParams = topnSchemaPath + pathTemp

func newTopnCmd() *cobra.Command {
	topnCmd := &cobra.Command{
		Use:     "topn",
		Version: version.Build(),
		Short:   "Topn operation",
	}

	// e.g. http://127.0.0.1:17913/api/v1/topn-agg/schema
	createCmd := &cobra.Command{
		Use:     "create -f [file|dir|-]",
		Version: version.Build(),
		Short:   "Create topn from files",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return rest(func() ([]reqBody, error) { return parseNameAndGroupFromYAML(cmd.InOrStdin()) },
				func(request request) (*resty.Response, error) {
					s := new(databasev1.TopNAggregation)
					err := protojson.Unmarshal(request.data, s)
					if err != nil {
						return nil, err
					}
					cr := &databasev1.TopNAggregationRegistryServiceCreateRequest{
						TopNAggregation: s,
					}
					b, err := protojson.Marshal(cr)
					if err != nil {
						return nil, err
					}
					return request.req.SetBody(b).Post(getPath(topnSchemaPath))
				},
				func(_ int, reqBody reqBody, _ []byte) error {
					fmt.Printf("topn %s.%s is created", reqBody.group, reqBody.name)
					fmt.Println()
					return nil
				}, enableTLS, insecure, cert)
		},
	}

	// e.g. http://127.0.0.1:17913/api/v1/topn-agg/schema/{sw_metric}/{my_measure_topn}
	updateCmd := &cobra.Command{
		Use:     "update -f [file|dir|-]",
		Version: version.Build(),
		Short:   "Update topn from files",
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			return rest(func() ([]reqBody, error) { return parseNameAndGroupFromYAML(cmd.InOrStdin()) },
				func(request request) (*resty.Response, error) {
					s := new(databasev1.TopNAggregation)
					err := protojson.Unmarshal(request.data, s)
					if err != nil {
						return nil, err
					}
					cr := &databasev1.TopNAggregationRegistryServiceUpdateRequest{
						TopNAggregation: s,
					}
					b, err := protojson.Marshal(cr)
					if err != nil {
						return nil, err
					}
					return request.req.SetBody(b).
						SetPathParam("name", request.name).SetPathParam("group", request.group).
						Put(getPath(topnSchemaPathWithParams))
				},
				func(_ int, reqBody reqBody, _ []byte) error {
					fmt.Printf("topn %s.%s is updated", reqBody.group, reqBody.name)
					fmt.Println()
					return nil
				}, enableTLS, insecure, cert)
		},
	}

	// e.g. http://127.0.0.1:17913/api/v1/topn-agg/schema/{sw_metric}/{my_measure_topn}
	getCmd := &cobra.Command{
		Use:     "get [-g group] -n name",
		Version: version.Build(),
		Short:   "Get a topn",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				return request.req.SetPathParam("name", request.name).SetPathParam("group", request.group).Get(getPath(topnSchemaPathWithParams))
			}, yamlPrinter, enableTLS, insecure, cert)
		},
	}

	deleteCmd := &cobra.Command{
		Use:     "delete [-g group] -n name",
		Version: version.Build(),
		Short:   "Delete a topn",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				return request.req.SetPathParam("name", request.name).SetPathParam("group", request.group).Delete(getPath(topnSchemaPathWithParams))
			}, func(_ int, reqBody reqBody, _ []byte) error {
				fmt.Printf("topn %s.%s is deleted", reqBody.group, reqBody.name)
				fmt.Println()
				return nil
			}, enableTLS, insecure, cert)
		},
	}
	bindNameFlag(getCmd, deleteCmd)

	// e.g. http://127.0.0.1:17913/api/v1/topn-agg/schema/lists/{sw_metric}
	listCmd := &cobra.Command{
		Use:     "list [-g group]",
		Version: version.Build(),
		Short:   "List topn",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				return request.req.SetPathParam("group", request.group).Get(getPath("/api/v1/topn-agg/schema/lists/{group}"))
			}, yamlPrinter, enableTLS, insecure, cert)
		},
	}

	// e.g. http://127.0.0.1:17913/api/v1/measure/topn
	queryCmd := &cobra.Command{
		Use:     "query [-s start_time] [-e end_time] -f [file|dir|-]",
		Version: version.Build(),
		Short:   "Query data in a topn",
		Long:    timeRangeUsage,
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			return rest(func() ([]reqBody, error) { return parseTimeRangeFromFlagAndYAML(cmd.InOrStdin()) },
				func(request request) (*resty.Response, error) {
					return request.req.SetBody(request.data).Post(getPath("/api/v1/measure/topn"))
				}, yamlPrinter, enableTLS, insecure, cert)
		},
	}
	bindFileFlag(createCmd, updateCmd, queryCmd)
	bindTimeRangeFlag(queryCmd)

	bindTLSRelatedFlag(getCmd, createCmd, deleteCmd, updateCmd, listCmd, queryCmd)
	topnCmd.AddCommand(getCmd, createCmd, deleteCmd, updateCmd, listCmd, queryCmd)
	return topnCmd
}
