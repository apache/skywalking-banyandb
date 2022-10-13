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
	"encoding/json"
	"fmt"

	"github.com/go-resty/resty/v2"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	property_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

const propertySchemaPath = "/api/v1/property/schema"

var (
	propertySchemaPathWithoutTagParams = propertySchemaPath + "/{group}/{name}/{id}"
	propertySchemaPathWithTagParams    = propertySchemaPath + "/{group}/{name}/{id}/{tag}"
)

func newPropertyCmd() *cobra.Command {
	propertyCmd := &cobra.Command{
		Use:     "property",
		Version: version.Build(),
		Short:   "Property operation",
	}

	createCmd := &cobra.Command{
		Use:     "create -f [file|dir|-]",
		Version: version.Build(),
		Short:   "Create properties from files",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return rest(func() ([]reqBody, error) { return parseFromYAMLForProperty(cmd.InOrStdin()) },
				func(request request) (*resty.Response, error) {
					s := new(property_v1.Property)
					err := protojson.Unmarshal(request.data, s)
					if err != nil {
						return nil, err
					}
					cr := &property_v1.ApplyRequest{
						Property: s,
					}
					b, err := json.Marshal(cr)
					if err != nil {
						return nil, err
					}
					return request.req.SetPathParam("group", request.group).SetPathParam("name", request.name).
						SetPathParam("id", request.id).SetBody(b).Post(getPath(propertySchemaPathWithoutTagParams))
				},
				func(_ int, reqBody reqBody, _ []byte) error {
					fmt.Printf("property %s.%s is created", reqBody.group, reqBody.name)
					fmt.Println()
					return nil
				})
		},
	}

	updateCmd := &cobra.Command{
		Use:     "update -f [file|dir|-]",
		Version: version.Build(),
		Short:   "Update properties from files",
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			return rest(func() ([]reqBody, error) { return parseFromYAMLForProperty(cmd.InOrStdin()) },
				func(request request) (*resty.Response, error) {
					s := new(property_v1.Property)
					err := protojson.Unmarshal(request.data, s)
					if err != nil {
						return nil, err
					}
					cr := &property_v1.ApplyRequest{
						Property: s,
					}
					b, err := json.Marshal(cr)
					if err != nil {
						return nil, err
					}
					return request.req.SetBody(b).
						SetPathParam("group", request.group).SetPathParam("name", request.name).SetPathParam("id", request.id).
						Put(getPath(propertySchemaPathWithoutTagParams))
				},
				func(_ int, reqBody reqBody, _ []byte) error {
					fmt.Printf("property %s.%s is updated", reqBody.group, reqBody.name)
					fmt.Println()
					return nil
				})
		},
	}

	getCmd := &cobra.Command{
		Use:     "get [-g group] -n name",
		Version: version.Build(),
		Short:   "Get a property",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				if request.tag == "" {
					return request.req.SetPathParam("name", request.name).SetPathParam("group", request.group).
						SetPathParam("id", request.id).Get(getPath(propertySchemaPathWithoutTagParams))
				}
				return request.req.SetPathParam("name", request.name).SetPathParam("group", request.group).
					SetPathParam("id", request.id).SetPathParam("tag", request.tag).
					Get(getPath(propertySchemaPathWithTagParams))
			}, yamlPrinter)
		},
	}

	deleteCmd := &cobra.Command{
		Use:     "delete [-g group] -n name",
		Version: version.Build(),
		Short:   "Delete a property",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				return request.req.SetPathParam("name", request.name).SetPathParam("group", request.group).
					SetPathParam("id", request.id).SetPathParam("tag", request.tag).Delete(getPath(propertySchemaPathWithTagParams))
			}, func(_ int, reqBody reqBody, _ []byte) error {
				fmt.Printf("property %s.%s is deleted", reqBody.group, reqBody.name)
				fmt.Println()
				return nil
			})
		},
	}
	bindNameFlag(getCmd, deleteCmd)

	listCmd := &cobra.Command{
		Use:     "list [-g group]",
		Version: version.Build(),
		Short:   "List properties",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				return request.req.SetPathParam("name", request.name).SetPathParam("group", request.group).
					SetPathParam("id", request.id).SetPathParam("tag", request.tag).Get(getPath(propertySchemaPathWithTagParams))
			}, yamlPrinter)
		},
	}

	bindFileFlag(createCmd, updateCmd)

	propertyCmd.AddCommand(getCmd, createCmd, deleteCmd, updateCmd, listCmd)
	return propertyCmd
}
