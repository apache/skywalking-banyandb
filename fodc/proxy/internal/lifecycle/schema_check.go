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

package lifecycle

import (
	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/internal/consistency"
)

// assembleSchemaConsistency unions the per-agent SchemaConsistency partials
// (each agent's local node cache/runtime fingerprints) and compares them against
// registryByGroup -- the registry truth the proxy fetched once from a
// schema-serving node -- running the checker once per group. Fingerprint compute
// happened in the agents and (for the registry) in the proxy's fetch; the
// checker only compares. It returns the per-group verdict, which the caller
// writes back onto the merged GroupLifecycleInfo (per-group collection errors
// already ride on GroupLifecycleInfo.errors and are unioned by the group merge).
//
// shortfall is set when an agent that should have reported did not, so the
// checker degrades affected groups to UNKNOWN rather than concluding CONSISTENT
// from a partial node set. A group with no registry truth (registry fetch failed
// or omitted it) is also UNKNOWN rather than treating every node object as an
// orphan.
func assembleSchemaConsistency(
	allData []*agentLifecycleData, checker *consistency.Checker, shortfall bool,
	registryByGroup map[string]map[consistency.ObjectKey]uint64,
) map[string]*fodcv1.SchemaConsistency {
	nodesByGroup := make(map[string]map[string][]*consistency.NodeObjectFP)
	for _, ad := range allData {
		if ad == nil || ad.Data == nil {
			continue
		}
		for _, g := range ad.Data.GetGroups() {
			sc := g.GetSchemaConsistency()
			if sc == nil {
				continue
			}
			nodes, ok := nodesByGroup[g.GetName()]
			if !ok {
				nodes = make(map[string][]*consistency.NodeObjectFP)
				nodesByGroup[g.GetName()] = nodes
			}
			for _, oc := range sc.GetObjects() {
				for _, nf := range oc.GetNodeFingerprints() {
					nodes[nf.GetNode()] = append(nodes[nf.GetNode()], &consistency.NodeObjectFP{
						Kind: oc.GetKind(), Name: oc.GetName(),
						Cache: nf.GetCacheFingerprint(), Runtime: nf.GetRuntimeFingerprint(),
					})
				}
			}
		}
	}

	verdicts := make(map[string]*fodcv1.SchemaConsistency, len(nodesByGroup))
	for group, nodeMap := range nodesByGroup {
		registry := registryByGroup[group]
		if len(registry) == 0 {
			verdicts[group] = &fodcv1.SchemaConsistency{
				Status: fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNKNOWN,
			}
			continue
		}
		nodes := make([]consistency.NodeObjects, 0, len(nodeMap))
		for node, objects := range nodeMap {
			nodes = append(nodes, consistency.NodeObjects{Node: node, Objects: objects})
		}
		expected := len(nodes)
		if shortfall {
			expected = len(nodes) + 1
		}
		verdicts[group] = checker.Check(group, registry, nodes, expected)
	}
	return verdicts
}
