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

package exporter

import (
	"context"
	"fmt"
	"slices"

	"google.golang.org/grpc"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
)

// FetchGroups lists every group through the liaison's registry service.
func FetchGroups(ctx context.Context, conn *grpc.ClientConn) ([]*commonv1.Group, error) {
	resp, err := databasev1.NewGroupRegistryServiceClient(conn).List(ctx, &databasev1.GroupRegistryServiceListRequest{})
	if err != nil {
		return nil, Exit(ExitCodeFor(err), "list groups: %w", err)
	}
	return resp.GetGroup(), nil
}

// ValidateSelectors checks that every group a selector names exists in the schema and
// belongs to the selected catalog, so a typo fails before anything is planned.
func ValidateSelectors(groups []*commonv1.Group, selectors []*transferv1.Selector) error {
	byName := make(map[string]commonv1.Catalog, len(groups))
	for _, g := range groups {
		byName[g.GetMetadata().GetName()] = g.GetCatalog()
	}
	for _, s := range selectors {
		for _, name := range s.GetGroups() {
			catalog, ok := byName[name]
			if !ok {
				return fmt.Errorf("selector group %q does not exist", name)
			}
			if catalog != s.GetCatalog() {
				return fmt.Errorf("selector group %q is a %s group, not %s", name, transfer.CatalogName(catalog), transfer.CatalogName(s.GetCatalog()))
			}
		}
	}
	return nil
}

// StageNames returns the lifecycle stage names a group declares, plus the default tier.
func StageNames(g *commonv1.Group) []string {
	names := []string{DefaultStageName}
	for _, st := range g.GetResourceOpts().GetStages() {
		names = append(names, st.GetName())
	}
	return names
}

// StageWarnings reports every (node, group, stage) a Plan frame carries whose stage the
// group schema does not declare: the node holds data the current lifecycle config does
// not describe, which the operator should know before exporting.
func StageWarnings(groups []*commonv1.Group, result *PlanResult) []string {
	declared := make(map[string][]string, len(groups))
	for _, g := range groups {
		declared[g.GetMetadata().GetName()] = StageNames(g)
	}
	var warnings []string
	for _, f := range result.Frames {
		stage := f.GetStage() // RunPlan already normalized it
		for _, u := range f.GetUnits() {
			group := unitGroup(u)
			stages, ok := declared[group]
			if !ok {
				warnings = append(warnings, fmt.Sprintf("node %s reports group %q which is not in the schema", f.GetNodeId(), group))
				continue
			}
			if !slices.Contains(stages, stage) {
				warnings = append(warnings, fmt.Sprintf("node %s reports stage %q for group %q; the schema declares %v", f.GetNodeId(), stage, group, stages))
			}
		}
	}
	return uniqueSorted(warnings)
}
