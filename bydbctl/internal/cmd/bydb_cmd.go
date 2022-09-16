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
	"errors"
	"fmt"

	"github.com/ghodss/yaml"
	"github.com/go-resty/resty/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newBanyanDBCmd() []*cobra.Command {
	UseGroupCmd := newUserGroupCmd()
	GroupCmd := newGroupCmd()
	StreamCmd := newStreamCmd()
	// todo: add more commands
	// MeasureCmd := newMeasureCmd()
	// PropertyCmd := newPropertyCmd()
	// IndexRuleCmd := newIndexRuleCmd()
	// IndexRuleBindingCmd := newIndexRuleBindingCmd()

	return []*cobra.Command{UseGroupCmd, GroupCmd, StreamCmd}
}

type reqFn func(request *resty.Request) (*resty.Response, error)

func rest(root string, fn reqFn) error {
	client := resty.New()
	var data map[string]interface{}
	err := json.Unmarshal([]byte(raw), &data)
	if err != nil {
		return err
	}
	stream, ok := data[root].(map[string]interface{})
	if !ok {
		return errors.New("input json format error")
	}
	metadata, ok := stream["metadata"].(map[string]interface{})
	if !ok {
		return errors.New("input json format error")
	}
	_, ok = metadata["group"].(string)
	if !ok {
		metadata["group"] = viper.GetString("group")
		if metadata["group"] == "" {
			return errors.New("please specify a group through the input json or the config file")
		}
	}
	resp, err := fn(client.R().SetBody(data))
	if err != nil {
		return err
	}
	yamlResult, err := yaml.JSONToYAML(resp.Body())
	if err != nil {
		return err
	}
	fmt.Print(string(yamlResult))
	return nil
}
