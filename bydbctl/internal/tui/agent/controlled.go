// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package agent

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// Controlled MCP server and tool names shared by every provider adapter and the tool bridge.
const (
	ControlledMCPServerName = "bydbctl-controlled-tools"
	ToolListGroupsSchemas   = "list_groups_schemas"
	ToolDescribeSchema      = "describe_schema"
	ToolProposeQueryPlan    = "propose_query_plan"
	ToolValidateBydbQL      = "validate_bydbql"
	ToolExecuteBydbQL       = "execute_bydbql"
)

var controlledToolNames = []string{
	ToolListGroupsSchemas,
	ToolDescribeSchema,
	ToolProposeQueryPlan,
	ToolValidateBydbQL,
	ToolExecuteBydbQL,
}

// ControlledToolNames returns the complete immutable-by-copy tool allowlist.
func ControlledToolNames() []string {
	return append([]string(nil), controlledToolNames...)
}

// ValidateControlledMCPServer verifies the provider-independent controlled-server contract.
func ValidateControlledMCPServer(server ControlledMCPServer) error {
	if server.Name != ControlledMCPServerName {
		return fmt.Errorf("controlled MCP server must be named %q", ControlledMCPServerName)
	}
	if !filepath.IsAbs(server.Command) {
		return errors.New("controlled MCP server command must be absolute")
	}
	if !SameToolSet(server.EnabledTools, controlledToolNames) {
		return fmt.Errorf("controlled MCP tool allowlist must contain exactly %s", strings.Join(controlledToolNames, ", "))
	}
	return nil
}
