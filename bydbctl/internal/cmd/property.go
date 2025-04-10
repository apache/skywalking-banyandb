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
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

const (
	propertySchemaPath = "/api/v1/property/schema"
	propertyDataPath   = "/api/v1/property/data/{group}/{name}/{id}"
)

var propertySchemaPathWithParams = propertySchemaPath + pathTemp

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

	schemaCmd := &cobra.Command{
		Use:     "schema",
		Version: version.Build(),
		Short:   "Property schema operations",
	}

	dataCmd := &cobra.Command{
		Use:     "data",
		Version: version.Build(),
		Short:   "Property data operations",
	}

	// Schema commands
	createSchemaCmd := &cobra.Command{
		Use:     "create -f [file|dir|-]",
		Version: version.Build(),
		Short:   "Create property schemas from files",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return rest(func() ([]reqBody, error) { return parseNameAndGroupFromYAML(cmd.InOrStdin()) },
				func(request request) (*resty.Response, error) {
					s := new(databasev1.Property)
					err := protojson.Unmarshal(request.data, s)
					if err != nil {
						return nil, err
					}
					cr := &databasev1.PropertyRegistryServiceCreateRequest{
						Property: s,
					}
					b, err := protojson.Marshal(cr)
					if err != nil {
						return nil, err
					}
					return request.req.SetBody(b).Post(getPath(propertySchemaPath))
				},
				func(_ int, reqBody reqBody, _ []byte) error {
					fmt.Printf("property schema %s.%s is created", reqBody.group, reqBody.name)
					fmt.Println()
					return nil
				}, enableTLS, insecure, cert)
		},
	}

	updateSchemaCmd := &cobra.Command{
		Use:     "update -f [file|dir|-]",
		Version: version.Build(),
		Short:   "Update property schemas from files",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return rest(func() ([]reqBody, error) { return parseNameAndGroupFromYAML(cmd.InOrStdin()) },
				func(request request) (*resty.Response, error) {
					s := new(databasev1.Property)
					err := protojson.Unmarshal(request.data, s)
					if err != nil {
						return nil, err
					}
					cr := &databasev1.PropertyRegistryServiceUpdateRequest{
						Property: s,
					}
					b, err := protojson.Marshal(cr)
					if err != nil {
						return nil, err
					}
					return request.req.SetBody(b).
						SetPathParam("name", request.name).SetPathParam("group", request.group).
						Put(getPath(propertySchemaPathWithParams))
				},
				func(_ int, reqBody reqBody, _ []byte) error {
					fmt.Printf("property schema %s.%s is updated", reqBody.group, reqBody.name)
					fmt.Println()
					return nil
				}, enableTLS, insecure, cert)
		},
	}

	getSchemaCmd := &cobra.Command{
		Use:     "get [-g group] -n name",
		Version: version.Build(),
		Short:   "Get a property schema",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				return request.req.SetPathParam("name", request.name).SetPathParam("group", request.group).Get(getPath(propertySchemaPathWithParams))
			}, yamlPrinter, enableTLS, insecure, cert)
		},
	}

	deleteSchemaCmd := &cobra.Command{
		Use:     "delete [-g group] -n name",
		Version: version.Build(),
		Short:   "Delete a property schema",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				return request.req.SetPathParam("name", request.name).SetPathParam("group", request.group).Delete(getPath(propertySchemaPathWithParams))
			}, func(_ int, reqBody reqBody, _ []byte) error {
				fmt.Printf("property schema %s.%s is deleted", reqBody.group, reqBody.name)
				fmt.Println()
				return nil
			}, enableTLS, insecure, cert)
		},
	}
	bindNameFlag(getSchemaCmd, deleteSchemaCmd)

	listSchemaCmd := &cobra.Command{
		Use:     "list [-g group]",
		Version: version.Build(),
		Short:   "List property schemas",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				return request.req.SetPathParam("group", request.group).Get(getPath("/api/v1/property/schema/lists/{group}"))
			}, yamlPrinter, enableTLS, insecure, cert)
		},
	}
	bindFileFlag(createSchemaCmd, updateSchemaCmd)

	// Data commands
	applyDataCmd := &cobra.Command{
		Use:     "apply -f [file|dir|-]",
		Version: version.Build(),
		Short:   "Apply(Create or Update) properties data from files",
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
						SetPathParam("id", request.id).SetBody(b).Put(getPath(propertyDataPath))
				}, yamlPrinter, enableTLS, insecure, cert)
		},
	}

	deleteDataCmd := &cobra.Command{
		Use:     "delete -g group -n name [-i id]",
		Version: version.Build(),
		Short:   "Delete property data",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				return request.req.SetPathParam("name", request.name).SetPathParam("group", request.group).
					SetPathParam("id", request.id).Delete(getPath(propertyDataPath))
			}, yamlPrinter, enableTLS, insecure, cert)
		},
	}
	bindNameAndIDFlag(deleteDataCmd)

	queryDataCmd := &cobra.Command{
		Use:     "query -f [file|dir|-]",
		Version: version.Build(),
		Short:   "Query property data from files",
		Long:    timeRangeUsage,
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			return rest(func() ([]reqBody, error) { return simpleParseFromYAML(cmd.InOrStdin()) },
				func(request request) (*resty.Response, error) {
					return request.req.SetBody(request.data).Post(getPath("/api/v1/property/data/query"))
				}, yamlPrinter, enableTLS, insecure, cert)
		},
	}
	bindFileFlag(applyDataCmd, queryDataCmd)

	// Bind flags and add commands
	bindTLSRelatedFlag(createSchemaCmd, updateSchemaCmd, getSchemaCmd, deleteSchemaCmd, listSchemaCmd)
	bindTLSRelatedFlag(applyDataCmd, deleteDataCmd, queryDataCmd)

	// Add schema commands to schema subcommand
	schemaCmd.AddCommand(createSchemaCmd, updateSchemaCmd, getSchemaCmd, deleteSchemaCmd, listSchemaCmd)

	// Add data commands to data subcommand
	dataCmd.AddCommand(applyDataCmd, deleteDataCmd, queryDataCmd)

	// Add schema and data subcommands to property command
	propertyCmd.AddCommand(schemaCmd, dataCmd)
	return propertyCmd
}
