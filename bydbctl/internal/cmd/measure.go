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

const measureSchemaPath = "/api/v1/measure/schema"

var measureSchemaPathWithParams = measureSchemaPath + pathTemp

func newMeasureCmd() *cobra.Command {
	measureCmd := &cobra.Command{
		Use:     "measure",
		Version: version.Build(),
		Short:   "Measure operation",
	}

	createCmd := &cobra.Command{
		Use:     "create -f [file|dir|-]",
		Version: version.Build(),
		Short:   "Create measures from files",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return rest(func() ([]reqBody, error) { return parseNameAndGroupFromYAML(cmd.InOrStdin()) },
				func(request request) (*resty.Response, error) {
					s := new(databasev1.Measure)
					err := protojson.Unmarshal(request.data, s)
					if err != nil {
						return nil, err
					}
					cr := &databasev1.MeasureRegistryServiceCreateRequest{
						Measure: s,
					}
					b, err := protojson.Marshal(cr)
					if err != nil {
						return nil, err
					}
					return request.req.SetBody(b).Post(getPath(measureSchemaPath))
				},
				func(_ int, reqBody reqBody, _ []byte) error {
					fmt.Printf("measure %s.%s is created", reqBody.group, reqBody.name)
					fmt.Println()
					return nil
				}, enableTLS, insecure, cert)
		},
	}

	updateCmd := &cobra.Command{
		Use:     "update -f [file|dir|-]",
		Version: version.Build(),
		Short:   "Update measures from files",
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			return rest(func() ([]reqBody, error) { return parseNameAndGroupFromYAML(cmd.InOrStdin()) },
				func(request request) (*resty.Response, error) {
					s := new(databasev1.Measure)
					err := protojson.Unmarshal(request.data, s)
					if err != nil {
						return nil, err
					}
					cr := &databasev1.MeasureRegistryServiceUpdateRequest{
						Measure: s,
					}
					b, err := protojson.Marshal(cr)
					if err != nil {
						return nil, err
					}
					return request.req.SetBody(b).
						SetPathParam("name", request.name).SetPathParam("group", request.group).
						Put(getPath(measureSchemaPathWithParams))
				},
				func(_ int, reqBody reqBody, _ []byte) error {
					fmt.Printf("measure %s.%s is updated", reqBody.group, reqBody.name)
					fmt.Println()
					return nil
				}, enableTLS, insecure, cert)
		},
	}

	getCmd := &cobra.Command{
		Use:     "get [-g group] -n name",
		Version: version.Build(),
		Short:   "Get a measure",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				return request.req.SetPathParam("name", request.name).SetPathParam("group", request.group).Get(getPath(measureSchemaPathWithParams))
			}, yamlPrinter, enableTLS, insecure, cert)
		},
	}

	deleteCmd := &cobra.Command{
		Use:     "delete [-g group] -n name",
		Version: version.Build(),
		Short:   "Delete a measure",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				return request.req.SetPathParam("name", request.name).SetPathParam("group", request.group).Delete(getPath(measureSchemaPathWithParams))
			}, func(_ int, reqBody reqBody, _ []byte) error {
				fmt.Printf("measure %s.%s is deleted", reqBody.group, reqBody.name)
				fmt.Println()
				return nil
			}, enableTLS, insecure, cert)
		},
	}
	bindNameFlag(getCmd, deleteCmd)

	listCmd := &cobra.Command{
		Use:     "list [-g group]",
		Version: version.Build(),
		Short:   "List measures",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				return request.req.SetPathParam("group", request.group).Get(getPath("/api/v1/measure/schema/lists/{group}"))
			}, yamlPrinter, enableTLS, insecure, cert)
		},
	}

	queryCmd := &cobra.Command{
		Use:     "query [-s start_time] [-e end_time] -f [file|dir|-]",
		Version: version.Build(),
		Short:   "Query data in a measure",
		Long:    timeRangeUsage,
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			return rest(func() ([]reqBody, error) { return parseTimeRangeFromFlagAndYAML(cmd.InOrStdin()) },
				func(request request) (*resty.Response, error) {
					return request.req.SetBody(request.data).Post(getPath("/api/v1/measure/data"))
				}, yamlPrinter, enableTLS, insecure, cert)
		},
	}
	bindFileFlag(createCmd, updateCmd, queryCmd)
	bindTimeRangeFlag(queryCmd)

	bindTLSRelatedFlag(getCmd, createCmd, deleteCmd, updateCmd, listCmd, queryCmd)
	measureCmd.AddCommand(getCmd, createCmd, deleteCmd, updateCmd, listCmd, queryCmd)
	return measureCmd
}
