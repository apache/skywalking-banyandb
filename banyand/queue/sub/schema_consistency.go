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

package sub

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/schema/consistency"
)

// registryReadConcurrency caps how many groups may query the registry at once.
// InspectAll fans out to maxInspectGroupConcurrency groups, and each group
// issues several registry reads that each broadcast to every META node; without
// this cap one cycle would burst well over a hundred concurrent broadcasts.
const registryReadConcurrency = 4

// schemaConsistencyErrorPrefix labels info.Errors entries that come from the
// schema consistency check, matching the prefixed style of the collector's
// TopLevelErrorPrefix and friends.
const schemaConsistencyErrorPrefix = "schema consistency: "

// checkSchemaConsistency compares the registry view of a group against what its
// data and liaison nodes report.
//
// The verdict is nil when it cannot be formed at all (property/disabled group,
// or the registry fingerprints could not be built); the caller then leaves the
// field unset rather than claiming consistency it did not establish. The error
// is non-nil whenever a degradation occurred that the operator should see: a
// fatal registry-build failure (verdict also nil) or a roster read failure
// (verdict still returned as UNKNOWN). The caller surfaces it on info.Errors.
func (s *server) checkSchemaConsistency(
	ctx context.Context, group *commonv1.Group,
	dataInfo []*databasev1.DataInfo, liaisonInfo []*databasev1.LiaisonInfo,
) (*fodcv1.SchemaConsistency, error) {
	if s.schemaChecker == nil {
		return nil, nil
	}
	// PROPERTY groups are skipped by the whole inspection pipeline (see
	// InfoCollectorRegistry.CollectDataInfo) and no node schemaRepo tracks them,
	// so checking them would report UNKNOWN forever instead of saying nothing.
	if group.GetCatalog() == commonv1.Catalog_CATALOG_PROPERTY {
		return nil, nil
	}
	groupName := group.GetMetadata().GetName()
	registryFP, err := s.buildRegistryFingerprintsThrottled(ctx, group)
	if err != nil {
		s.log.Warn().Err(err).Str("group", groupName).
			Msg("failed to build registry fingerprints; skipping schema consistency check")
		return nil, fmt.Errorf("failed to build registry fingerprints for %s: %w", groupName, err)
	}

	nodes := make([]consistency.NodeObjects, 0, len(dataInfo)+len(liaisonInfo))
	appendNode := func(name string, objects []*databasev1.ObjectSchemaState) {
		if name == "" && len(objects) == 0 {
			return
		}
		nodes = append(nodes, consistency.NodeObjects{Node: name, Objects: objects})
	}
	for _, di := range dataInfo {
		appendNode(di.GetNode().GetMetadata().GetName(), di.GetSchemaObjects())
	}
	// The lifecycle path historically only collected data nodes; liaison nodes
	// hold the same schema, so the caller collects and passes them in to be
	// included here, or their drift would be invisible.
	for _, li := range liaisonInfo {
		appendNode(li.GetNode().GetMetadata().GetName(), li.GetSchemaObjects())
	}
	expected, rosterErr := s.expectedNodeCount(ctx)
	if rosterErr != nil {
		// The roster read failed, so whether a node is silently missing is
		// unknowable. Force a shortfall so the verdict is UNKNOWN rather than a
		// CONSISTENT that a silent node could hide behind; the error is returned
		// so the caller can surface why the verdict is degraded.
		expected = len(nodes) + 1
	}
	return s.schemaChecker.Check(groupName, registryFP, nodes, expected), rosterErr
}

// buildRegistryFingerprintsThrottled bounds how many groups hit the registry
// concurrently. It respects ctx so a canceled inspection does not queue behind
// the semaphore.
func (s *server) buildRegistryFingerprintsThrottled(
	ctx context.Context, group *commonv1.Group,
) (map[consistency.ObjectKey]uint64, error) {
	if s.registrySem != nil {
		select {
		case s.registrySem <- struct{}{}:
			defer func() { <-s.registrySem }()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.buildRegistryFingerprints(ctx, group)
}

// buildRegistryFingerprints reads the group's schema from the registry once and
// fingerprints every object in it.
//
// Each registry call fans out to every META node and may trigger a repair write,
// so the reads are issued once per group here -- never once per node.
func (s *server) buildRegistryFingerprints(
	ctx context.Context, group *commonv1.Group,
) (map[consistency.ObjectKey]uint64, error) {
	groupName := group.GetMetadata().GetName()
	result := make(map[consistency.ObjectKey]uint64)

	groupFP, err := consistency.Fingerprint(group, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fingerprint group %s: %w", groupName, err)
	}
	result[consistency.ObjectKey{Kind: schema.KindGroup.String(), Name: groupName}] = groupFP

	opt := schema.ListOpt{Group: groupName}
	rules, err := s.metadataRepo.IndexRuleRegistry().ListIndexRule(ctx, opt)
	if err != nil {
		return nil, fmt.Errorf("failed to list index rules of %s: %w", groupName, err)
	}
	ruleIndex := make(map[string]*databasev1.IndexRule, len(rules))
	for _, r := range rules {
		name := r.GetMetadata().GetName()
		ruleIndex[groupName+"/"+name] = r
		ruleFP, ruleErr := consistency.Fingerprint(r, nil)
		if ruleErr != nil {
			return nil, fmt.Errorf("failed to fingerprint index rule %s: %w", name, ruleErr)
		}
		result[consistency.ObjectKey{Kind: schema.KindIndexRule.String(), Name: name}] = ruleFP
	}

	bindings, err := s.metadataRepo.IndexRuleBindingRegistry().ListIndexRuleBinding(ctx, opt)
	if err != nil {
		return nil, fmt.Errorf("failed to list index rule bindings of %s: %w", groupName, err)
	}
	lookups := registryLookups(bindings, ruleIndex, group.GetCatalog())

	if resourceErr := s.fingerprintResources(ctx, group, opt, lookups, result); resourceErr != nil {
		return nil, resourceErr
	}
	return result, nil
}

// fingerprintResources adds the group's catalog-specific resources to result.
func (s *server) fingerprintResources(
	ctx context.Context,
	group *commonv1.Group,
	opt schema.ListOpt,
	lookups registryLookupSet,
	result map[consistency.ObjectKey]uint64,
) error {
	groupName := group.GetMetadata().GetName()
	switch group.GetCatalog() {
	case commonv1.Catalog_CATALOG_STREAM:
		specs, err := s.metadataRepo.StreamRegistry().ListStream(ctx, opt)
		if err != nil {
			return fmt.Errorf("failed to list streams of %s: %w", groupName, err)
		}
		for _, spec := range specs {
			if fpErr := addResourceFingerprint(result, schema.KindStream, groupName,
				spec.GetMetadata().GetName(), spec, lookups); fpErr != nil {
				return fpErr
			}
		}
	case commonv1.Catalog_CATALOG_MEASURE:
		specs, err := s.metadataRepo.MeasureRegistry().ListMeasure(ctx, opt)
		if err != nil {
			return fmt.Errorf("failed to list measures of %s: %w", groupName, err)
		}
		for _, spec := range specs {
			if fpErr := addResourceFingerprint(result, schema.KindMeasure, groupName,
				spec.GetMetadata().GetName(), spec, lookups); fpErr != nil {
				return fpErr
			}
		}
	case commonv1.Catalog_CATALOG_TRACE:
		specs, err := s.metadataRepo.TraceRegistry().ListTrace(ctx, opt)
		if err != nil {
			return fmt.Errorf("failed to list traces of %s: %w", groupName, err)
		}
		for _, spec := range specs {
			if fpErr := addResourceFingerprint(result, schema.KindTrace, groupName,
				spec.GetMetadata().GetName(), spec, lookups); fpErr != nil {
				return fpErr
			}
		}
	default:
		// PROPERTY and unspecified groups hold no objects a node schemaRepo tracks.
	}
	return nil
}

// addResourceFingerprint fingerprints one resource together with the index rules
// bound to it, and records the value under the resource's kind and name.
func addResourceFingerprint(
	result map[consistency.ObjectKey]uint64,
	kind schema.Kind,
	group, name string,
	spec proto.Message,
	lookups registryLookupSet,
) error {
	fp, err := consistency.Fingerprint(spec,
		consistency.DeriveBoundRules(group, name, lookups.bindings, lookups.rules))
	if err != nil {
		return fmt.Errorf("failed to fingerprint %s %s: %w", kind, name, err)
	}
	result[consistency.ObjectKey{Kind: kind.String(), Name: name}] = fp
	return nil
}

// expectedNodeCount is how many nodes should have answered. The error is non-nil
// when the roster could not be read: the count is then unknown, and the caller
// must not let it pass as a satisfied quorum -- a silent node would otherwise
// read as CONSISTENT instead of UNKNOWN.
func (s *server) expectedNodeCount(ctx context.Context) (int, error) {
	total := 0
	for _, role := range []databasev1.Role{databasev1.Role_ROLE_DATA, databasev1.Role_ROLE_LIAISON} {
		nodes, err := s.metadataRepo.NodeRegistry().ListNode(ctx, role)
		if err != nil {
			s.log.Warn().Err(err).Stringer("role", role).
				Msg("failed to list nodes for the schema consistency quorum")
			return 0, fmt.Errorf("failed to list %s nodes for the schema consistency quorum: %w", role, err)
		}
		total += len(nodes)
	}
	return total, nil
}

// registryLookupSet pairs the two adapters DeriveBoundRules needs.
type registryLookupSet struct {
	bindings consistency.BindingLookup
	rules    consistency.RuleLookup
}

func registryLookups(
	bindings []*databasev1.IndexRuleBinding,
	ruleIndex map[string]*databasev1.IndexRule,
	catalog commonv1.Catalog,
) registryLookupSet {
	return registryLookupSet{
		bindings: registryBindingLookup{bindings: bindings, catalog: catalog},
		rules:    registryRuleLookup{rules: ruleIndex},
	}
}

// registryBindingLookup scopes registry bindings to one subject and catalog.
type registryBindingLookup struct {
	bindings []*databasev1.IndexRuleBinding
	catalog  commonv1.Catalog
}

func (l registryBindingLookup) BindingsOf(_, name string) []*databasev1.IndexRuleBinding {
	var result []*databasev1.IndexRuleBinding
	for _, b := range l.bindings {
		// Scope by catalog so this matches the node-side cache, whose binding
		// index is catalog-filtered upstream. Deliberately NOT filtered by
		// begin_at/expire_at: the cache does not filter either, and matching the
		// registry-side helper in banyand/metadata/client.go would turn every
		// expired binding into a permanent false positive.
		if b.GetSubject().GetName() == name && b.GetSubject().GetCatalog() == l.catalog {
			result = append(result, b)
		}
	}
	return result
}

// registryRuleLookup resolves rules from the batch already listed for the group.
type registryRuleLookup struct {
	rules map[string]*databasev1.IndexRule
}

func (l registryRuleLookup) Rule(group, name string) *databasev1.IndexRule {
	return l.rules[group+"/"+name]
}
