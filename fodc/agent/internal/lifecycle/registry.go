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
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/internal/consistency"
	"github.com/apache/skywalking-banyandb/fodc/internal/timeouts"
)

// Kind strings match schema.Kind.String(), the same values the node stamps on
// ObjectSnapshot.kind, so registry and node fingerprints key on the same object.
const (
	kindGroup     = "group"
	kindStream    = "stream"
	kindMeasure   = "measure"
	kindTrace     = "trace"
	kindIndexRule = "indexRule"
)

// registryObjectFP is one registry object's truth fingerprint.
type registryObjectFP struct {
	kind string
	name string
	fp   uint64
}

// FetchSchemaRegistry reads the authoritative registry from the local liaison
// client endpoint and returns its per-object fingerprints, one entry per group,
// which the agent streams to the proxy. Fingerprint compute lives here in the
// FODC agent, never in the banyandb data path, and it runs only on the one agent
// the proxy selects, so the authoritative registry is read once per collection
// cycle. A fatal read failure is returned as an error (the proxy surfaces it
// rather than silently degrading to UNKNOWN); a group whose own read failed is
// returned as a group carrying its error and no objects.
func (c *Collector) FetchSchemaRegistry(ctx context.Context) ([]*fodcv1.SchemaRegistryGroup, error) {
	if c.registryAddr == "" {
		return nil, fmt.Errorf("no schema-registry address configured")
	}
	byGroup, groupErrs, err := fetchRegistryFingerprints(ctx, c.registryAddr)
	if err != nil {
		return nil, err
	}
	groups := make([]*fodcv1.SchemaRegistryGroup, 0, len(byGroup)+len(groupErrs))
	for group, objects := range byGroup {
		sg := &fodcv1.SchemaRegistryGroup{Group: group, Objects: make([]*fodcv1.SchemaObjectFingerprint, 0, len(objects))}
		for key, fp := range objects {
			sg.Objects = append(sg.Objects, &fodcv1.SchemaObjectFingerprint{Kind: key.Kind, Name: key.Name, Fingerprint: fp})
		}
		groups = append(groups, sg)
	}
	for group, e := range groupErrs {
		groups = append(groups, &fodcv1.SchemaRegistryGroup{Group: group, Error: e})
	}
	return groups, nil
}

// fetchRegistryFingerprints dials the local schema-serving node and computes the
// registry-truth fingerprint of every object in every group. It returns the
// per-group fingerprints; the per-group read errors (a group whose registry read
// fails is omitted from the map so the checker degrades it to UNKNOWN, but the
// reason is returned so the caller can surface it); and a fatal error when the
// node itself is unreachable.
func fetchRegistryFingerprints(
	ctx context.Context, address string,
) (map[string]map[consistency.ObjectKey]uint64, map[string]string, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", address, err)
	}
	defer func() { _ = conn.Close() }()

	reqCtx, cancel := context.WithTimeout(ctx, timeouts.AgentInspectAll)
	defer cancel()
	groupClient := databasev1.NewGroupRegistryServiceClient(conn)
	listResp, err := groupClient.List(reqCtx, &databasev1.GroupRegistryServiceListRequest{})
	if err != nil {
		return nil, nil, fmt.Errorf("list groups on %s: %w", address, err)
	}

	out := make(map[string]map[consistency.ObjectKey]uint64, len(listResp.GetGroup()))
	groupErrs := make(map[string]string)
	for _, group := range listResp.GetGroup() {
		name := group.GetMetadata().GetName()
		objects, fpErr := registryFingerprintsForGroup(reqCtx, conn, group)
		if fpErr != nil {
			groupErrs[name] = fpErr.Error()
			continue
		}
		m := make(map[consistency.ObjectKey]uint64, len(objects))
		for _, ro := range objects {
			m[consistency.ObjectKey{Kind: ro.kind, Name: ro.name}] = ro.fp
		}
		out[name] = m
	}
	return out, groupErrs, nil
}

// registryFingerprintsForGroup fingerprints every object in one group's registry
// view (group, index rules, and each resource with its derived bound rules).
func registryFingerprintsForGroup(
	ctx context.Context, conn *grpc.ClientConn, group *commonv1.Group,
) ([]registryObjectFP, error) {
	groupName := group.GetMetadata().GetName()
	var objects []registryObjectFP

	groupFP, err := consistency.Fingerprint(group, nil)
	if err != nil {
		return nil, fmt.Errorf("fingerprint group %s: %w", groupName, err)
	}
	objects = append(objects, registryObjectFP{kind: kindGroup, name: groupName, fp: groupFP})

	ruleClient := databasev1.NewIndexRuleRegistryServiceClient(conn)
	ruleResp, err := ruleClient.List(ctx, &databasev1.IndexRuleRegistryServiceListRequest{Group: groupName})
	if err != nil {
		return nil, fmt.Errorf("list index rules of %s: %w", groupName, err)
	}
	ruleIndex := make(map[string]*databasev1.IndexRule, len(ruleResp.GetIndexRule()))
	for _, rule := range ruleResp.GetIndexRule() {
		name := rule.GetMetadata().GetName()
		ruleIndex[groupName+"/"+name] = rule
		ruleFP, ruleErr := consistency.Fingerprint(rule, nil)
		if ruleErr != nil {
			return nil, fmt.Errorf("fingerprint index rule %s: %w", name, ruleErr)
		}
		objects = append(objects, registryObjectFP{kind: kindIndexRule, name: name, fp: ruleFP})
	}

	bindingClient := databasev1.NewIndexRuleBindingRegistryServiceClient(conn)
	bindingResp, err := bindingClient.List(ctx, &databasev1.IndexRuleBindingRegistryServiceListRequest{Group: groupName})
	if err != nil {
		return nil, fmt.Errorf("list index rule bindings of %s: %w", groupName, err)
	}
	lookups := registryLookupSet{
		bindings: registryBindingLookup{bindings: bindingResp.GetIndexRuleBinding(), catalog: group.GetCatalog()},
		rules:    registryRuleLookup{rules: ruleIndex},
	}

	resources, resErr := registryResourceFingerprints(ctx, conn, group, lookups)
	if resErr != nil {
		return nil, resErr
	}
	return append(objects, resources...), nil
}

func registryResourceFingerprints(
	ctx context.Context, conn *grpc.ClientConn, group *commonv1.Group, lookups registryLookupSet,
) ([]registryObjectFP, error) {
	groupName := group.GetMetadata().GetName()
	var objects []registryObjectFP
	add := func(kind, name string, fp uint64) {
		objects = append(objects, registryObjectFP{kind: kind, name: name, fp: fp})
	}
	switch group.GetCatalog() {
	case commonv1.Catalog_CATALOG_STREAM:
		resp, err := databasev1.NewStreamRegistryServiceClient(conn).List(ctx, &databasev1.StreamRegistryServiceListRequest{Group: groupName})
		if err != nil {
			return nil, fmt.Errorf("list streams of %s: %w", groupName, err)
		}
		for _, spec := range resp.GetStream() {
			name := spec.GetMetadata().GetName()
			fp, fpErr := consistency.Fingerprint(spec, consistency.DeriveBoundRules(groupName, name, lookups.bindings, lookups.rules))
			if fpErr != nil {
				return nil, fmt.Errorf("fingerprint stream %s: %w", name, fpErr)
			}
			add(kindStream, name, fp)
		}
	case commonv1.Catalog_CATALOG_MEASURE:
		resp, err := databasev1.NewMeasureRegistryServiceClient(conn).List(ctx, &databasev1.MeasureRegistryServiceListRequest{Group: groupName})
		if err != nil {
			return nil, fmt.Errorf("list measures of %s: %w", groupName, err)
		}
		for _, spec := range resp.GetMeasure() {
			name := spec.GetMetadata().GetName()
			fp, fpErr := consistency.Fingerprint(spec, consistency.DeriveBoundRules(groupName, name, lookups.bindings, lookups.rules))
			if fpErr != nil {
				return nil, fmt.Errorf("fingerprint measure %s: %w", name, fpErr)
			}
			add(kindMeasure, name, fp)
		}
	case commonv1.Catalog_CATALOG_TRACE:
		resp, err := databasev1.NewTraceRegistryServiceClient(conn).List(ctx, &databasev1.TraceRegistryServiceListRequest{Group: groupName})
		if err != nil {
			return nil, fmt.Errorf("list traces of %s: %w", groupName, err)
		}
		for _, spec := range resp.GetTrace() {
			name := spec.GetMetadata().GetName()
			fp, fpErr := consistency.Fingerprint(spec, consistency.DeriveBoundRules(groupName, name, lookups.bindings, lookups.rules))
			if fpErr != nil {
				return nil, fmt.Errorf("fingerprint trace %s: %w", name, fpErr)
			}
			add(kindTrace, name, fp)
		}
	default:
		// PROPERTY and unspecified groups hold no fingerprintable resources.
	}
	return objects, nil
}

// registryLookupSet pairs the two adapters DeriveBoundRules needs.
type registryLookupSet struct {
	rules    registryRuleLookup
	bindings registryBindingLookup
}

// registryBindingLookup scopes registry bindings to one subject and catalog,
// matching the node-side cache's catalog-filtered (but not validity-window
// filtered) binding index.
type registryBindingLookup struct {
	bindings []*databasev1.IndexRuleBinding
	catalog  commonv1.Catalog
}

func (l registryBindingLookup) BindingsOf(_, name string) []*databasev1.IndexRuleBinding {
	var result []*databasev1.IndexRuleBinding
	for _, b := range l.bindings {
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
