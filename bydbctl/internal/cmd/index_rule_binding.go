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

const indexRuleBindingSchemaPath = "/api/v1/index-rule-binding/schema"

var indexRuleBindingSchemaPathWithParams = indexRuleBindingSchemaPath + pathTemp

func newIndexRuleBindingCmd() *cobra.Command {
	indexRuleBindingCmd := &cobra.Command{
		Use:     "indexRuleBinding",
		Version: version.Build(),
		Short:   "IndexRuleBinding operation",
	}

	createCmd := &cobra.Command{
		Use:     "create -f [file|dir|-]",
		Version: version.Build(),
		Short:   "Create indexRuleBindings from files",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return rest(func() ([]reqBody, error) { return parseNameAndGroupFromYAML(cmd.InOrStdin()) },
				func(request request) (*resty.Response, error) {
					s := new(databasev1.IndexRuleBinding)
					err := protojson.Unmarshal(request.data, s)
					if err != nil {
						return nil, err
					}
					cr := &databasev1.IndexRuleBindingRegistryServiceCreateRequest{
						IndexRuleBinding: s,
					}
					b, err := protojson.Marshal(cr)
					if err != nil {
						return nil, err
					}
					return request.req.SetBody(b).Post(getPath(indexRuleBindingSchemaPath))
				},
				func(_ int, reqBody reqBody, _ []byte) error {
					fmt.Printf("indexRuleBinding %s.%s is created", reqBody.group, reqBody.name)
					fmt.Println()
					return nil
				}, enableTLS, insecure, cert)
		},
	}

	updateCmd := &cobra.Command{
		Use:     "update -f [file|dir|-]",
		Version: version.Build(),
		Short:   "Update indexRuleBindings from files",
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			return rest(func() ([]reqBody, error) { return parseNameAndGroupFromYAML(cmd.InOrStdin()) },
				func(request request) (*resty.Response, error) {
					s := new(databasev1.IndexRuleBinding)
					err := protojson.Unmarshal(request.data, s)
					if err != nil {
						return nil, err
					}
					cr := &databasev1.IndexRuleBindingRegistryServiceUpdateRequest{
						IndexRuleBinding: s,
					}
					b, err := protojson.Marshal(cr)
					if err != nil {
						return nil, err
					}
					return request.req.SetBody(b).
						SetPathParam("name", request.name).SetPathParam("group", request.group).
						Put(getPath(indexRuleBindingSchemaPathWithParams))
				},
				func(_ int, reqBody reqBody, _ []byte) error {
					fmt.Printf("indexRuleBinding %s.%s is updated", reqBody.group, reqBody.name)
					fmt.Println()
					return nil
				}, enableTLS, insecure, cert)
		},
	}

	getCmd := &cobra.Command{
		Use:     "get [-g group] -n name",
		Version: version.Build(),
		Short:   "Get a indexRuleBinding",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				return request.req.SetPathParam("name", request.name).SetPathParam("group", request.group).Get(getPath(indexRuleBindingSchemaPathWithParams))
			}, yamlPrinter, enableTLS, insecure, cert)
		},
	}

	deleteCmd := &cobra.Command{
		Use:     "delete [-g group] -n name",
		Version: version.Build(),
		Short:   "Delete a indexRuleBinding",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				return request.req.SetPathParam("name", request.name).SetPathParam("group", request.group).Delete(getPath(indexRuleBindingSchemaPathWithParams))
			}, func(_ int, reqBody reqBody, _ []byte) error {
				fmt.Printf("indexRuleBinding %s.%s is deleted", reqBody.group, reqBody.name)
				fmt.Println()
				return nil
			}, enableTLS, insecure, cert)
		},
	}
	bindNameFlag(getCmd, deleteCmd)

	listCmd := &cobra.Command{
		Use:     "list [-g group]",
		Version: version.Build(),
		Short:   "List indexRuleBindings",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				return request.req.SetPathParam("group", request.group).Get(getPath("/api/v1/index-rule-binding/schema/lists/{group}"))
			}, yamlPrinter, enableTLS, insecure, cert)
		},
	}

	bindFileFlag(createCmd, updateCmd)

	bindTLSRelatedFlag(getCmd, createCmd, deleteCmd, updateCmd, listCmd)
	indexRuleBindingCmd.AddCommand(getCmd, createCmd, deleteCmd, updateCmd, listCmd)
	return indexRuleBindingCmd
}
