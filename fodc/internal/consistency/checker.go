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

package consistency

import (
	"sort"
	"sync"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
)

// consecutiveCyclesToReport is how many back-to-back collection rounds a
// divergence must survive before it is reported. Schema propagation is
// asynchronous and the node-side sweep is lock-free, so a single round's
// disagreement is usually in-flight state or a torn read; requiring two rounds
// removes that class of false positive.
const consecutiveCyclesToReport = 2

// ObjectKey identifies a schema object inside one group.
type ObjectKey struct {
	Kind string
	Name string
}

// NodeObjectFP is one object's cache/runtime fingerprints on a node. It is the
// checker's internal carrier, assembled by the proxy from each agent's partial
// SchemaConsistency, so the wire format ships no parallel per-node message.
type NodeObjectFP struct {
	Kind    string
	Name    string
	Cache   uint64
	Runtime uint64
}

// NodeObjects is one node's reported schema objects (its cache/runtime layers).
type NodeObjects struct {
	Node    string
	Objects []*NodeObjectFP
}

// issueKey identifies one object's divergence on one node, which is the
// granularity the suppression counter tracks. It deliberately omits the issue
// type: any divergence on the same object/node that persists across rounds is
// suspicious, whatever layer it surfaces in, so the streak is kept per object
// rather than per type. The reported type is then the latest round's reading.
type issueKey struct {
	Group string
	Node  string
	Kind  string
	Name  string
}

// Checker compares the registry view against what each node reports,
// suppressing divergences that have not persisted across rounds.
//
// It is stateful: the suppression counters live across calls, so one instance
// must be reused between collection cycles. InspectAll fans groups out
// concurrently, so all access is mutex-guarded.
type Checker struct {
	streaks map[issueKey]int
	mu      sync.Mutex
}

// NewChecker creates a checker with empty suppression state.
func NewChecker() *Checker {
	return &Checker{streaks: make(map[issueKey]int)}
}

// Check compares registryFingerprints (fetched once from a schema-serving node)
// against every node's reported cache/runtime. It returns the group-level
// verdict: an object-centric table pairing each object's registry truth with
// every node's cache/runtime fingerprints, the surviving issues, and a rollup
// status.
//
// expectedNodes is how many data and liaison nodes are on the roster; a shortfall
// downgrades the verdict to UNKNOWN rather than letting a silent node pass as
// consistent.
func (c *Checker) Check(
	group string,
	registryFingerprints map[ObjectKey]uint64,
	nodes []NodeObjects,
	expectedNodes int,
) *fodcv1.SchemaConsistency {
	c.mu.Lock()
	defer c.mu.Unlock()

	result := &fodcv1.SchemaConsistency{}
	live := make(map[issueKey]struct{})
	suppressed := false
	objects := newObjectAccumulator(registryFingerprints)

	for _, n := range nodes {
		reported := make(map[ObjectKey]*NodeObjectFP, len(n.Objects))
		for _, o := range n.Objects {
			reported[ObjectKey{Kind: o.Kind, Name: o.Name}] = o
			objects.addNode(n.Node, o)
		}
		// registry has it -> compare against what this node reports.
		for key, registryFP := range registryFingerprints {
			issueType := classify(registryFP, reported[key])
			if issueType == fodcv1.SchemaIssueType_SCHEMA_ISSUE_TYPE_UNSPECIFIED {
				continue
			}
			if c.track(group, n.Node, key, live) < consecutiveCyclesToReport {
				suppressed = true
				continue
			}
			result.Issues = append(result.Issues, &fodcv1.SchemaIssue{
				Kind: key.Kind, Name: key.Name, Node: n.Node, Type: issueType,
			})
		}
		// node has it but registry does not -> orphan.
		for key := range reported {
			if _, inRegistry := registryFingerprints[key]; inRegistry {
				continue
			}
			if c.track(group, n.Node, key, live) < consecutiveCyclesToReport {
				suppressed = true
				continue
			}
			result.Issues = append(result.Issues, &fodcv1.SchemaIssue{
				Kind: key.Kind, Name: key.Name, Node: n.Node,
				Type: fodcv1.SchemaIssueType_SCHEMA_ISSUE_TYPE_ORPHAN,
			})
		}
	}
	c.prune(group, live)
	sortIssues(result.Issues)
	result.Objects = objects.sorted()

	switch {
	case len(result.Issues) > 0:
		result.Status = fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT
	// No node answered at all: absence of evidence, not evidence of agreement.
	// Also covers expectedNodes being unknown (roster lookup failed, reported 0).
	case len(nodes) == 0:
		result.Status = fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNKNOWN
	case suppressed || len(nodes) < expectedNodes:
		result.Status = fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNKNOWN
	default:
		result.Status = fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT
	}
	return result
}

// objectAccumulator collects every schema object once, pairing the registry
// truth with each node's cache/runtime fingerprints into the object-centric
// output.
type objectAccumulator struct {
	m map[ObjectKey]*fodcv1.ObjectConsistency
}

// newObjectAccumulator seeds one entry per registry object with its truth
// fingerprint; node fingerprints are added as nodes report them.
func newObjectAccumulator(fps map[ObjectKey]uint64) *objectAccumulator {
	a := &objectAccumulator{m: make(map[ObjectKey]*fodcv1.ObjectConsistency, len(fps))}
	for key, fp := range fps {
		a.m[key] = &fodcv1.ObjectConsistency{Kind: key.Kind, Name: key.Name, RegistryFingerprint: fp}
	}
	return a
}

// addNode records one node's cache/runtime fingerprints for an object, creating
// the entry (with a zero registry fingerprint) when the object is an orphan the
// registry does not know.
func (a *objectAccumulator) addNode(node string, o *NodeObjectFP) {
	key := ObjectKey{Kind: o.Kind, Name: o.Name}
	oc, ok := a.m[key]
	if !ok {
		oc = &fodcv1.ObjectConsistency{Kind: key.Kind, Name: key.Name}
		a.m[key] = oc
	}
	oc.NodeFingerprints = append(oc.NodeFingerprints, &fodcv1.NodeFingerprint{
		Node: node, CacheFingerprint: o.Cache, RuntimeFingerprint: o.Runtime,
	})
}

// sorted flattens the accumulator and orders it deterministically via the shared
// sorter, keeping the checker's ordering in lockstep with the proxy merge.
func (a *objectAccumulator) sorted() []*fodcv1.ObjectConsistency {
	out := make([]*fodcv1.ObjectConsistency, 0, len(a.m))
	for _, oc := range a.m {
		out = append(out, oc)
	}
	SortObjectConsistencies(out)
	return out
}

func sortIssues(issues []*fodcv1.SchemaIssue) {
	sort.Slice(issues, func(i, j int) bool {
		if issues[i].GetKind() != issues[j].GetKind() {
			return issues[i].GetKind() < issues[j].GetKind()
		}
		if issues[i].GetName() != issues[j].GetName() {
			return issues[i].GetName() < issues[j].GetName()
		}
		return issues[i].GetNode() < issues[j].GetNode()
	})
}

// classify returns the issue type for one object on one node, or UNSPECIFIED
// when it agrees across all layers. When both boundaries disagree only the
// upstream one is reported: the downstream difference is a symptom of the same
// root cause.
func classify(registryFP uint64, o *NodeObjectFP) fodcv1.SchemaIssueType {
	if o == nil {
		return fodcv1.SchemaIssueType_SCHEMA_ISSUE_TYPE_MISSING_IN_CACHE
	}
	switch {
	case o.Cache != registryFP:
		return fodcv1.SchemaIssueType_SCHEMA_ISSUE_TYPE_CACHE_STALE
	case o.Runtime != o.Cache:
		return fodcv1.SchemaIssueType_SCHEMA_ISSUE_TYPE_RUNTIME_NOT_APPLIED
	default:
		return fodcv1.SchemaIssueType_SCHEMA_ISSUE_TYPE_UNSPECIFIED
	}
}

// track marks a divergence live and bumps its consecutive-round counter.
func (c *Checker) track(group, node string, key ObjectKey, live map[issueKey]struct{}) int {
	ik := issueKey{Group: group, Node: node, Kind: key.Kind, Name: key.Name}
	live[ik] = struct{}{}
	c.streaks[ik]++
	return c.streaks[ik]
}

// prune drops counters for objects in this group that no longer diverge or no
// longer exist, keeping the map bounded by the live divergence set rather than
// by history. Other groups' counters are untouched.
func (c *Checker) prune(group string, live map[issueKey]struct{}) {
	for k := range c.streaks {
		if k.Group != group {
			continue
		}
		if _, stillLive := live[k]; !stillLive {
			delete(c.streaks, k)
		}
	}
}

// trackedCount reports how many suppression counters are held. Tests use it to
// assert pruning.
func (c *Checker) trackedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.streaks)
}
