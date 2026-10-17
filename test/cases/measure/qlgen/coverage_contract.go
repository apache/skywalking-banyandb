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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package qlgen

import "fmt"

// coverageContract is audited independently of generated cases. Removing a
// generator branch must leave a missing witness, not erase its coverage claim.
// A pending entry records a gap; it never counts as successful QL coverage.
var coverageContract = []Feature{
	{ID: "t15.aggregate_argument_qualifier", Status: "supported"},
	{ID: "t15.hidden_group", Status: "supported"},
	{ID: "t15.scalar.zone", Status: "supported"},
	{ID: "t15.scalar.host", Status: "supported"},
	{ID: "t15.scalar.v", Status: "supported"},
	{ID: "t15.scalar.f", Status: "supported"},
	{ID: "t15.scalar.time", Status: "supported"},

	{ID: "t15.null_empty", Status: "supported"},
	{ID: "t15.fixture", Status: "supported"},
	{ID: "t15.rebind", Status: "supported"},
	{ID: "t15.scalar", Status: "supported"},
	{ID: "t15.tuple", Status: "supported"},
	{ID: "t15.complete_input", Status: "supported"},
	{ID: "t15.expression", Status: "supported"},
	{ID: "t15.where", Status: "supported"},
	{ID: "t15.pagination", Status: "supported"},
	{ID: "t15.limit_early", Status: "supported"},
	{ID: "t15.offset_complete", Status: "supported"},
	{ID: "t15.no_scan", Status: "supported"},
	{ID: "t15.having", Status: "supported"},
	{ID: "t15.unsupported_keys", Status: "rejection"},
	{ID: "t15.hidden_order", Status: "rejection"},
	{ID: "t14.contextual_contains", Status: "supported"},
	{ID: "t14.indexed_array_rejection", Status: "rejection"},
	{ID: "t14.aggregate_syntax", Status: "supported"},
	{ID: "t14.group_only", Status: "supported"},
	{ID: "t14.membership_coercion", Status: "supported"},
	{ID: "t14.legacy_membership", Status: "supported"},
	{ID: "t14.membership_duplicates", Status: "supported"},
	{ID: "t14.membership_literal", Status: "supported"},
	{ID: "t14.membership_F", Status: "supported"},
	{ID: "t14.membership_spelling", Status: "supported"},
	{ID: "t14.membership_binding", Status: "supported"},
	{ID: "t14.membership_form", Status: "supported"},
	{ID: "t14.membership_null", Status: "supported"},
	{ID: "t14.membership_scalar_source", Status: "supported"},
	{ID: "t14.membership_array_source", Status: "supported"},
	{ID: "t14.membership_recovery", Status: "supported"},
	{ID: "t14.membership_having", Status: "supported"},
	{ID: "t14.invalid_membership", Status: "rejection"},
	{ID: "t14.fixture", Status: "supported"},
	{ID: "t14.rebind", Status: "supported"},
	{ID: "t14.grouped", Status: "supported"},
	{ID: "t14.local", Status: "supported"},
	{ID: "t14.three_valued", Status: "supported"},
	{ID: "t14.hidden_aggregate", Status: "supported"},
	{ID: "t14.hidden_group", Status: "supported"},
	{ID: "t14.repeated_aggregate", Status: "supported"},
	{ID: "t14.native", Status: "supported"},
	{ID: "t14.expression", Status: "supported"},
	{ID: "t14.scalar", Status: "supported"},
	{ID: "t14.empty", Status: "supported"},
	{ID: "t14.raw_filter", Status: "supported"},
	{ID: "t14.complete_groups", Status: "supported"},
	{ID: "t14.final_pagination", Status: "supported"},
	{ID: "t14.no_scan", Status: "supported"},
	{ID: "t14.invalid_reference", Status: "rejection"},
	{ID: "t12.legacy_schema_fixture", Status: "supported"},
	{ID: "t12.legacy_schema_compatibility", Status: "supported"},
	{ID: "t12.constant_predicate", Status: "supported"},
	{ID: "t12.arithmetic", Status: "supported"},
	{ID: "t12.boolean_logic", Status: "supported"},
	{ID: "t12.coalesce", Status: "supported"},
	{ID: "t12.complete_input", Status: "supported"},
	{ID: "t12.empty_aggregate", Status: "supported"},
	{ID: "t12.empty_membership", Status: "supported"},
	{ID: "t12.field_filter", Status: "supported"},
	{ID: "t12.final_limit", Status: "supported"},
	{ID: "t12.fixture", Status: "supported"},
	{ID: "t12.float_filter", Status: "supported"},
	{ID: "t12.group_order", Status: "supported"},
	{ID: "t12.hidden_input", Status: "supported"},
	{ID: "t12.legacy_time", Status: "supported"},
	{ID: "t12.lifecycle", Status: "supported"},
	{ID: "t12.local_aggregate", Status: "supported"},
	{ID: "t12.masked_boolean", Status: "supported"},
	{ID: "t12.masked_case", Status: "supported"},
	{ID: "t12.membership", Status: "supported"},
	{ID: "t12.nan_policy", Status: "supported"},
	{ID: "t12.native_tag_conjunction", Status: "supported"},
	{ID: "t12.no_scan", Status: "supported"},
	{ID: "t12.null_predicate", Status: "supported"},
	{ID: "t12.omitted_limit", Status: "supported"},
	{ID: "t12.parameter_cast", Status: "supported"},
	{ID: "t12.parameter_order", Status: "supported"},
	{ID: "t12.projection_order", Status: "supported"},
	{ID: "t12.rebind", Status: "supported"},
	{ID: "t12.tag_field_or", Status: "supported"},
	{ID: "t12.three_valued", Status: "supported"},
	{ID: "t12.time_field_or", Status: "supported"},
	{ID: "t12.aggregate_before_limit", Status: "supported"},
	{ID: "t12.invalid_value", Status: "rejection"},
	{ID: "t12.invalid_type", Status: "rejection"},
	{ID: "t10.arithmetic", Status: "supported"},
	{ID: "t10.array_output", Status: "supported"},
	{ID: "t10.boolean_logic", Status: "supported"},
	{ID: "t10.casts", Status: "supported"},
	{ID: "t10.comparisons", Status: "supported"},
	{ID: "t10.default_names", Status: "supported"},
	{ID: "t10.fixture", Status: "supported"},
	{ID: "t10.float_literals", Status: "supported"},
	{ID: "t10.invalid_type", Status: "rejection"},
	{ID: "t10.invalid_value", Status: "rejection"},
	{ID: "t10.lexer_compat", Status: "supported"},
	{ID: "t10.masked_case", Status: "supported"},
	{ID: "t10.masked_coalesce", Status: "supported"},
	{ID: "t10.membership", Status: "supported"},
	{ID: "t10.mixed_numeric", Status: "supported"},
	{ID: "t10.no_scan", Status: "supported"},
	{ID: "t10.numeric_null", Status: "supported"},
	{ID: "t10.pagination", Status: "supported"},
	{ID: "t10.parameter_cast", Status: "supported"},
	{ID: "t10.rebind", Status: "supported"},
	{ID: "t10.selection_mask", Status: "supported"},
	{ID: "t10.simple_case", Status: "supported"},
	{ID: "t10.string_output", Status: "supported"},
	{ID: "t10.three_valued", Status: "supported"},
	{ID: "t10.timestamp_cast", Status: "supported"},
	{ID: "t10.typed_null", Status: "supported"},
	{ID: "t10.unreachable_invalid", Status: "supported"},
	{ID: "t08.fixture", Status: "supported"},
	{ID: "t08.native_float", Status: "supported"},
	{ID: "t08.null_empty", Status: "supported"},
	{ID: "t08.tuple_keys", Status: "supported"},
	{ID: "t08.rebind", Status: "supported"},
	{ID: "t08.shared_leaves", Status: "supported"},
	{ID: "t08.exact_integer", Status: "supported"},
	{ID: "t08.scalar_entity", Status: "supported"},
	{ID: "t08.complete_groups", Status: "supported"},
	{ID: "t08.dedup", Status: "supported"},
	{ID: "t08.fallback", Status: "supported"},
	{ID: "t08.weighted_average", Status: "supported"},
	{ID: "t08.no_scan", Status: "supported"},
	{ID: "t08.lifecycle", Status: "supported"},
	{ID: "t08.integer_overflow", Status: "rejection"},
	{ID: "t08.child_revision", Status: "supported"},
	{ID: "t07.fixture", Status: "supported"},
	{ID: "t07.aggregate.COUNT", Status: "supported"},
	{ID: "t07.aggregate.SUM", Status: "supported"},
	{ID: "t07.aggregate.MIN", Status: "supported"},
	{ID: "t07.aggregate.MAX", Status: "supported"},
	{ID: "t07.aggregate.AVG", Status: "supported"},
	{ID: "t07.aggregate.MEAN", Status: "supported"},
	{ID: "t07.null_empty", Status: "supported"},
	{ID: "t07.rebind", Status: "supported"},
	{ID: "t07.multiple", Status: "supported"},
	{ID: "t07.shared_state", Status: "supported"},
	{ID: "t07.typed_keys", Status: "supported"},
	{ID: "t07.hidden_slots", Status: "supported"},
	{ID: "t07.count_tags", Status: "supported"},
	{ID: "t07.local_activation", Status: "supported"},
	{ID: "t07.default_names", Status: "supported"},
	{ID: "t07.qualified_columns", Status: "supported"},
	{ID: "t07.residual_time", Status: "supported"},
	{ID: "t07.complete_input", Status: "supported"},
	{ID: "t07.dedup", Status: "supported"},
	{ID: "t07.limit", Status: "supported"},
	{ID: "t07.invalid_shape", Status: "rejection"},
	{ID: "t03.order.timestamp_alias", Status: "supported"},
	{ID: "t03.invalid.order", Status: "rejection"},
	{ID: "t03.time.domain", Status: "supported"},
	{ID: "t03.projection.typed", Status: "supported"},
	{ID: "t03.projection.repeated", Status: "supported"},
	{ID: "t03.projection.families", Status: "supported"},
	{ID: "t03.projection.timestamp", Status: "supported"},
	{ID: "t03.time.fractional", Status: "supported"},
	{ID: "t03.time.boundaries", Status: "supported"},
	{ID: "t03.time.or", Status: "supported"},
	{ID: "t03.time.intersection", Status: "supported"},
	{ID: "t03.empty", Status: "supported"},
	{ID: "t03.time.fresh_now", Status: "supported"},
	{ID: "t03.time.interval", Status: "supported"},
	{ID: "t03.time.rebind", Status: "supported"},
	{ID: "t03.limit.extended", Status: "supported"},
	{ID: "t03.limit.complete_input", Status: "supported"},
	{ID: "t03.invalid.names", Status: "rejection"},
	{ID: "t03.invalid.time", Status: "rejection"},
	{ID: "aggregate.AVG", Status: "supported"},
	{ID: "aggregate.COUNT", Status: "supported"},
	{ID: "aggregate.COUNT_DISTINCT", Status: "supported"},
	{ID: "aggregate.MAX", Status: "supported"},
	{ID: "aggregate.MEAN", Status: "supported"},
	{ID: "aggregate.MIN", Status: "supported"},
	{ID: "aggregate.SUM", Status: "supported"},
	{ID: "binding.params", Status: "supported"},
	{ID: "binding.rebind", Status: "supported"},
	{ID: "combinations.pairwise", Status: "supported"},
	{ID: "filter.and", Status: "supported"},
	{ID: "filter.eq", Status: "supported"},
	{ID: "filter.ge", Status: "supported"},
	{ID: "filter.gt", Status: "supported"},
	{ID: "filter.having", Status: "supported"},
	{ID: "filter.in", Status: "supported"},
	{ID: "filter.le", Status: "supported"},
	{ID: "filter.lt", Status: "supported"},
	{ID: "filter.match", Status: "supported"},
	{ID: "filter.ne", Status: "supported"},
	{ID: "filter.nested", Status: "supported"},
	{ID: "filter.not_having", Status: "supported"},
	{ID: "filter.not_in", Status: "supported"},
	{ID: "filter.or", Status: "supported"},
	{ID: "group.tags", Status: "supported"},
	{ID: "group.time_bucket", Status: "supported"},
	{ID: "order.ASC", Status: "supported"},
	{ID: "order.DESC", Status: "supported"},
	{ID: "pagination.limit", Status: "supported"},
	{ID: "pagination.offset", Status: "supported"},
	{ID: "projection.all", Status: "supported"},
	{ID: "projection.fields", Status: "supported"},
	{ID: "projection.mixed", Status: "supported"},
	{ID: "projection.tags", Status: "supported"},
	{ID: "projection.top", Status: "supported"},
	{ID: "projection.typed", Status: "supported"},
	{ID: "source.explicit", Status: "supported"},
	{ID: "source.parenthesized", Status: "supported"},
	{ID: "time.<", Status: "supported"},
	{ID: "time.<=", Status: "supported"},
	{ID: "time.=", Status: "supported"},
	{ID: "time.>", Status: "supported"},
	{ID: "time.>=", Status: "supported"},
	{ID: "time.BETWEEN", Status: "supported"},
	{ID: "trace.enabled", Status: "supported"},
	{ID: "values.arrays", Status: "supported"},
	{ID: "values.int", Status: "supported"},
	{ID: "values.int_list", Status: "supported"},
	{ID: "values.nulls", Status: "supported"},
	{ID: "values.string", Status: "supported"},
	{ID: "values.string_list", Status: "supported"},
	{ID: "invalid.aggregate", Status: "rejection"},
	{ID: "invalid.parameters", Status: "rejection"},
	{ID: "invalid.projection", Status: "rejection"},
	{ID: "invalid.range", Status: "rejection"},
	{ID: "invalid.syntax", Status: "rejection"},
	{ID: "catalog.B1", Status: "supported"},
	{ID: "catalog.B2", Status: "supported"},
	{ID: "catalog.B3", Status: "supported"},
	{ID: "catalog.B4", Status: "supported"},
	{ID: "catalog.B5", Status: "pending", Reason: "Multiple sort keys and NULL placement require T17"},
	{ID: "catalog.B6", Status: "supported"},
	{ID: "catalog.B7", Status: "supported"},
	{ID: "catalog.B8", Status: "supported"},
	{ID: "catalog.B9", Status: "pending", Reason: "Computed/ordinal grouping requires T16"},
	{ID: "legacy.low_float_mean", Status: "pending", Reason: "Native legacy MEAN clamps values below one; the oracle retains arithmetic mean semantics"},
	{ID: "legacy.stages", Status: "pending", Reason: "Successful stage selection needs a generated staged-group fixture"},
	{ID: "legacy.index_order", Status: "pending", Reason: "Successful index-rule ordering needs a generated rule and binding fixture"},
	{ID: "legacy.show_topn", Status: "pending", Reason: "SHOW TOP uses a separate persisted TopN rule, outside the SELECT capability fixture"},
	{ID: "legacy.match_analyzer", Status: "pending", Reason: "Custom MATCH analyzers need generated analyzer/index fixtures"},
	{ID: "legacy.null_predicates", Status: "pending", Reason: "NULL equality is not SQL null equality; successful predicates need an explicit null-semantics contract"},
	{ID: "legacy.empty_arrays", Status: "pending", Reason: "Storage encoding collapses empty and null arrays; distinct results need a storage and QL semantics contract"},
	{ID: "legacy.time_values", Status: "pending", Reason: "Relative/now and integer time forms need clock-controlled semantics and rejection witnesses"},
	{ID: "legacy.strict_time_bounds", Status: "pending", Reason: "Legacy TIME comparisons are inclusive; strict operators at an existing row need a boundary contract"},
	{ID: "legacy.identifiers", Status: "pending", Reason: "Quoted/dotted/keyword identifiers need a generated schema and escaping witnesses"},
	{ID: "legacy.multigroup", Status: "pending", Reason: "Multiple groups need generated schema compatibility, deduplication and authorization fixtures"},
	{ID: "legacy.group_shapes", Status: "pending", Reason: "Field-only, bucket-only and default-width grouping need independent legacy result contracts"},
	{ID: "legacy.predicate_bindings", Status: "pending", Reason: "Empty/scalar/array and NULL/bytes/timestamp bindings need positional type and expansion witnesses"},
	{ID: "legacy.aggregate_shapes", Status: "pending", Reason: "Ungrouped, empty and nullable/tag-target aggregation need independent legacy result contracts"},
	{ID: "legacy.syntax_boundaries", Status: "pending", Reason: "Depth64/65, clause order and malformed/empty projection need generated boundary witnesses"},
	{ID: "legacy.default_limit", Status: "supported"},
}

// PendingCoverage returns explicit grammar and semantic gaps in the finite model.
func PendingCoverage() []Feature {
	var pending []Feature
	for _, feature := range coverageContract {
		if feature.Status == "pending" {
			pending = append(pending, feature)
		}
	}
	return pending
}

// ValidateCoverageContract prevents silently deleting or relabeling a capability.
// New grammar support requires auditing this inventory as well as adding cases.
func ValidateCoverageContract(features []Feature) error {
	actual := make(map[string]Feature, len(features))
	for _, feature := range features {
		if _, exists := actual[feature.ID]; exists {
			return fmt.Errorf("duplicate QL capability %s", feature.ID)
		}
		actual[feature.ID] = feature
	}
	for _, required := range coverageContract {
		feature, exists := actual[required.ID]
		if !exists {
			return fmt.Errorf("required QL capability %s is missing", required.ID)
		}
		if feature.Status != required.Status {
			return fmt.Errorf("QL capability %s has status %s, audited contract requires %s", required.ID, feature.Status, required.Status)
		}
		if required.Status == "pending" && feature.Reason != required.Reason {
			return fmt.Errorf("pending QL capability %s differs from its audited coverage gap", required.ID)
		}
		delete(actual, required.ID)
	}
	for id := range actual {
		return fmt.Errorf("QL capability %s has not been audited in the coverage contract", id)
	}
	return nil
}
