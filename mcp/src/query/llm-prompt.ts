/**
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Apache Software Foundation (ASF) licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import type { ResourcesByGroup } from './types.js';

/**
 * Generate the LLM prompt for converting natural language to BydbQL queries.
 */
export function generateQueryPrompt(
  description: string,
  args: Record<string, unknown>,
  groups: string[] = [],
  resourcesByGroup: ResourcesByGroup = {},
): string {
  const groupsInfo = groups.length > 0 
    ? `\n\nAvailable Groups in BanyanDB:\n${groups.map(g => `- ${g}`).join('\n')}\n\nWhen extracting group names from the description, prefer using one of these available groups. If the description mentions a group that doesn't exist in this list, you may still use it, but prefer matching available groups when possible.`
    : '';

  // Build resources information - organized as resource -> group mapping for easy lookup
  let resourcesInfo = '';
  const resourceToGroupMap: Record<string, { type: string; group: string }> = {};
  const groupsWithResources = Object.keys(resourcesByGroup).filter(group => {
    const resources = resourcesByGroup[group];
    return resources.streams.length > 0 || resources.measures.length > 0 || 
           resources.traces.length > 0 || resources.properties.length > 0 || resources.topNItems.length > 0 || resources.indexRule.length > 0;
  });

  // Build resource-to-group mapping
  for (const group of groupsWithResources) {
    const resources = resourcesByGroup[group];
    for (const stream of resources.streams) {
      if (stream) resourceToGroupMap[stream] = { type: 'STREAM', group };
    }
    for (const measure of resources.measures) {
      if (measure) resourceToGroupMap[measure] = { type: 'MEASURE', group };
    }
    for (const trace of resources.traces) {
      if (trace) resourceToGroupMap[trace] = { type: 'TRACE', group };
    }
    for (const property of resources.properties) {
      if (property) resourceToGroupMap[property] = { type: 'PROPERTY', group };
    }
    for (const topNItem of resources.topNItems) {
      if (topNItem) resourceToGroupMap[topNItem] = { type: 'TOPN', group };
    }
  }

  if (Object.keys(resourceToGroupMap).length > 0) {
    resourcesInfo = '\n\nAvailable Resources in BanyanDB (Resource -> Group Mapping):\n';
    // Group by resource type for better readability
    const resourcesByType: Record<string, Array<{ name: string; group: string }>> = {
      STREAM: [],
      MEASURE: [],
      TRACE: [],
      PROPERTY: [],
      TOPN: [],
    };
    
    for (const [resourceName, info] of Object.entries(resourceToGroupMap)) {
      resourcesByType[info.type].push({ name: resourceName, group: info.group });
    }

    for (const [type, resources] of Object.entries(resourcesByType)) {
      if (resources.length > 0) {
        resourcesInfo += `\n${type}s:\n`;
        for (const resource of resources) {
          resourcesInfo += `  "${resource.name}" -> Group "${resource.group}"\n`;
        }
      }
    }
    resourcesInfo += '\nWhen extracting resource names from the description, prefer using one of these available resources. CRITICAL: If no resource type is found in the description, look up the resource name in this mapping to find its corresponding resource type (STREAM, MEASURE, TRACE, PROPERTY, or TOPN). If no group name is found in the description, look up the resource name in this mapping to find its corresponding group. If the description mentions a resource that doesn\'t exist in this list, you may still use it, but prefer matching available resources when possible.';
  }
  
  // Build index rules information - collect all indexed fields for ORDER BY validation
  let indexRulesInfo = '';
  const allIndexedFields: string[] = [];
  const indexedFieldsByGroup: Record<string, string[]> = {};
  const groupsWithIndexRules = Object.keys(resourcesByGroup).filter(group => {
    const resources = resourcesByGroup[group];
    return resources.indexRule.length > 0;
  });

  if (groupsWithIndexRules.length > 0) {
    // Collect all indexed fields
    for (const group of groupsWithIndexRules) {
      const indexRules = resourcesByGroup[group].indexRule;
      const fields: string[] = [];
      for (const indexRule of indexRules) {
        if (indexRule) {
          allIndexedFields.push(indexRule);
          fields.push(indexRule);
        }
      }
      if (fields.length > 0) {
        indexedFieldsByGroup[group] = fields;
      }
    }

    indexRulesInfo = '\n\nAvailable Indexed Fields for ORDER BY (by Group):\n';
    for (const group of groupsWithIndexRules) {
      const fields = indexedFieldsByGroup[group];
      if (fields && fields.length > 0) {
        indexRulesInfo += `\nGroup "${group}":\n`;
        for (const field of fields) {
          indexRulesInfo += `  - "${field}"\n`;
        }
      }
    }
    indexRulesInfo += `\nAll Indexed Fields (across all groups): ${allIndexedFields.map(f => `"${f}"`).join(', ')}\n`;
    indexRulesInfo += '\nIndex Rules define which tags/fields are indexed and can be used efficiently in ORDER BY clauses.';
  }
  
  return `You are a BydbQL query generator. Convert the following natural language description into a valid BydbQL query.${groupsInfo}${resourcesInfo}${indexRulesInfo}

BydbQL Syntax:
- Resource types: STREAM, MEASURE, TRACE, PROPERTY, TOPN
- TIME clause: TIME >= '-1h', TIME > '-1d', TIME BETWEEN '-24h' AND '-1h'
  - Use TIME > for "from last X" (e.g., "from last day" = TIME > '-1d')
  - Use TIME >= for "since" or "in the last X"
- LIMIT clause: LIMIT N (must come AFTER ORDER BY clause)
- Clause order: TIME → AGGREGATE BY → ORDER BY → LIMIT

Semantic Understanding:
- Distinguish "last N [time units]" (TIME clause) from "last N [data points]" (LIMIT clause):
  - "last 30 hours/days" → TIME clause
  - "last 30 zipkin_span/items/data points" → LIMIT clause
  - When a number appears directly before/after a resource name → LIMIT (data points)
  - When a number appears with a time unit → TIME (time range)

Resource Type Detection:
- Detect from keywords: STREAM (log, logs, stream, event), MEASURE (metric, measure, stat), TRACE (trace, span), PROPERTY (property, metadata, config)
- If no keywords found, look up resource name in "Available Resources" mapping above

Resource and Group Name Extraction:
- Resource name patterns:
  - Explicit names before "in"/"of" (e.g., "service_cpm_minute in metricsMinute")
  - Type keywords as names (e.g., "list log data" → name: "log")
  - Underscore-separated or CamelCase identifiers
- Group name patterns (priority order):
  - "the group is group_name" or "group is group_name"
  - "in group_name" or "of group_name"
  - "group_name's"
- If no group found, look up resource name in "Available Resources" mapping above

Query Format Selection:

1. TOPN Query (only for MEASURE with ranking keywords + number):
   - Keywords: "top", "highest", "lowest", "best", "worst" followed by NUMBER
   - Format: SHOW TOP N FROM MEASURE measure_name IN group_name TIME time_condition [AGGREGATE BY agg_function] [ORDER BY DESC|ASC]
   - ORDER BY: Use only "ORDER BY DESC" or "ORDER BY ASC" (no field name)
   - Do NOT use LIMIT clause in TOPN queries
   - Example: SHOW TOP 10 FROM MEASURE cpu_usage IN default TIME > '-1h' AGGREGATE BY MAX ORDER BY DESC

2. Common Query (default for all other cases):
   - Format: SELECT fields FROM RESOURCE_TYPE resource_name IN group_name [TIME clause] [AGGREGATE BY clause] [ORDER BY clause] [LIMIT clause]
   - Example: SELECT * FROM TRACE zipkin_span IN default ORDER BY timestamp_millis DESC LIMIT 30

Clause Detection:

AGGREGATE BY:
- Keywords: "sum"/"total" → SUM, "max"/"maximum" → MAX, "min"/"minimum" → MIN, "mean"/"average" → MEAN, "count" → COUNT
- Preserve explicit clauses exactly as provided

ORDER BY:
- Syntax: ORDER BY field [ASC|DESC] or ORDER BY TIME [ASC|DESC]
- Keywords: "highest"/"largest" → DESC, "lowest"/"smallest" → ASC
- For TOPN queries: Use only "ORDER BY DESC" or "ORDER BY ASC" (no field name)
- CRITICAL: Field Validation for ORDER BY:
  - When extracting an ORDER BY field from the user description, you MUST check if that field exists in the "Available Indexed Fields" list above
  - Extract the field name (excluding ASC/DESC keywords) and compare it against the indexed fields
  - If the field exists in the indexed fields list → use it as-is
  - If the field does NOT exist in the indexed fields list → find the most similar field from the indexed fields list and use that instead
  - Similarity matching: Look for fields that:
    * Share common prefixes/suffixes (e.g., "timestamp" vs "timestamp_millis")
    * Have similar names (e.g., "time" vs "timestamp", "id" vs "trace_id")
    * Match partial words (e.g., "millis" matches "timestamp_millis")
  - Always preserve the ASC/DESC direction from the original description
  - Example: If user says "ORDER BY time DESC" but only "timestamp_millis" is indexed → use "ORDER BY timestamp_millis DESC"
- Preserve explicit clauses exactly as provided, but validate the field name against indexed fields

LIMIT:
- Use when requesting specific number of results: "last N [resource]", "first N [resource]", "N [resource]"
- Must come AFTER ORDER BY clause
- Typically pair with ORDER BY: "last N" → ORDER BY timestamp DESC LIMIT N

User description: "${description}"${typeof args.resource_type === 'string' && args.resource_type ? `\n\nIMPORTANT: Resource type is explicitly provided: ${args.resource_type.toUpperCase()}. Use this value instead of extracting from description.` : ''}${typeof args.resource_name === 'string' && args.resource_name ? `\n\nIMPORTANT: Resource name is explicitly provided: ${args.resource_name}. Use this value instead of extracting from description.` : ''}${typeof args.group === 'string' && args.group ? `\n\nIMPORTANT: Group name is explicitly provided: ${args.group}. Use this value instead of extracting from description.` : ''}

Processing Rules:
- Use explicitly provided values with HIGHEST PRIORITY
- Extract from description only if not explicitly provided:
  - Resource type: Detect from keywords or lookup in mapping
  - Resource name: Extract explicit names or use type keywords
  - Group name: Extract using patterns or lookup in mapping
  - TIME clause: Distinguish time range from data points
  - LIMIT clause: Extract when requesting number of results
  - AGGREGATE BY: Extract from keywords or preserve explicit clauses
  - ORDER BY: Extract from keywords or preserve explicit clauses, BUT MUST validate field name against indexed fields list and use similar field if exact match not found
- Preserve explicit TIME, AGGREGATE BY clauses exactly as provided
- For ORDER BY: Preserve ASC/DESC direction but validate and correct field name against indexed fields

JSON Response:
- "bydbql": ALWAYS REQUIRED
- "type": Include if explicitly provided or needed for clarity. ${typeof args.resource_type === 'string' && args.resource_type ? `MUST be "${args.resource_type.toUpperCase()}"` : 'Extract from description'}
- "name": Include if explicitly provided or needed for clarity. ${typeof args.resource_name === 'string' && args.resource_name ? `MUST be "${args.resource_name}"` : 'Extract from description'}
- "group": Include if explicitly provided or needed for clarity. ${typeof args.group === 'string' && args.group ? `MUST be "${args.group}"` : 'Extract from description'}
- "explanations": Include ONLY if description indicates explanations are needed

Example:
{
  "bydbql": "SELECT * FROM TRACE zipkin_span IN default ORDER BY timestamp_millis DESC LIMIT 30",
  "type": ${typeof args.resource_type === 'string' && args.resource_type ? `"${args.resource_type.toUpperCase()}"` : '"TRACE"'},
  "name": ${typeof args.resource_name === 'string' && args.resource_name ? `"${args.resource_name}"` : '"zipkin_span"'},
  "group": ${typeof args.group === 'string' && args.group ? `"${args.group}"` : '"default"'}
}

Return ONLY the JSON object, no markdown formatting or additional text.`;
}

