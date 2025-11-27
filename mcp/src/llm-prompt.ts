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

import type { ResourcesByGroup } from './query-generator.js';

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
           resources.traces.length > 0 || resources.properties.length > 0 || resources.topNItems.length > 0;
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
  
  return `You are a BydbQL query generator. Convert the following natural language description into a valid BydbQL query.${groupsInfo}${resourcesInfo}

BydbQL Syntax:
- Resource types: STREAM, MEASURE, TRACE, PROPERTY, TOPN
- TIME clause examples: TIME >= '-1h', TIME > '-1d', TIME BETWEEN '-24h' AND '-1h'
- Use TIME > for "from last X" (e.g., "from last day" = TIME > '-1d'), TIME >= for "since" or "in the last X"
- For "from last day", use TIME > '-1d' (not TIME >=)
- LIMIT clause: LIMIT N (limits the number of results returned)
- LIMIT clause order: Must come AFTER ORDER BY clause: SELECT ... ORDER BY field DESC LIMIT N

CRITICAL Semantic Understanding - Understanding User Intent:
- You must understand the MEANING and INTENT of the description, not just match patterns
- Distinguish between "last N [time units]" vs "last N [data points/items]":
  - "last 30 hours" or "last 30 days" → TIME clause (time range)
  - "last 30 zipkin_span" or "last 30 items" or "30 data points" → LIMIT clause (number of results)
  - "list the last 30 zipkin_span" means "list 30 data points of zipkin_span", NOT "last 30 hours"
- When a number appears directly before or after a resource name (e.g., "last 30 zipkin_span", "30 zipkin_span", "zipkin_span 30"), it typically refers to the NUMBER OF DATA POINTS, not a time unit
- When a number appears with a time unit (e.g., "last 30 hours", "30 days ago", "past 2 weeks"), it refers to a TIME RANGE
- Examples:
  - "list the last 30 [resource_name] order by time" → SELECT * FROM TRACE [resource_name] IN default ORDER BY timestamp DESC LIMIT 30
  - "get last 10 [resource_name]" → SELECT * FROM STREAM [resource_name] IN default ORDER BY timestamp DESC LIMIT 10
  - "show last 5 [resource_name]" → SELECT * FROM MEASURE [resource_name] IN default ORDER BY timestamp DESC LIMIT 5
  - "list [resource_name] from last 30 hours" → SELECT * FROM STREAM [resource_name] IN default TIME >= '-30h'
  - "get data from last 2 days" → SELECT * FROM STREAM [resource_name] IN default TIME >= '-2d'

Resource Type Detection:
- First, try to detect from keywords in the description:
  - STREAM: for logs, events, streams (keywords: log, logs, stream, streams, event, events)
  - MEASURE: for metrics, measurements, statistics (keywords: metric, metrics, measure, measures, stat, stats, statistics)
  - TRACE: for traces, spans, tracing data (keywords: trace, traces, span, spans, tracing)
  - PROPERTY: for properties, metadata, configuration (keywords: property, properties, metadata, config)
- CRITICAL: If no resource type can be detected from the description keywords, look up the resource name in the "Available Resources in BanyanDB (Resource -> Group Mapping)" section above to find its corresponding resource type. The mapping shows resources organized by type (STREAMs, MEASUREs, TRACEs, PROPERTYs, TOPNs), so you can determine the type by finding which section contains the resource name.


Resource and Group Name Patterns:
- The user description may express resource and group relationships in different ways:
  - Standard pattern: "resource_name in group_name" (e.g., "service_cpm_minute in metricsMinute")
  - Alternative pattern: "resource_name of group_name" (e.g., "service_cpm_minute of metricsMinute")
  - Possessive pattern: "group_name's resource_name" (e.g., "metricsMinute's service_cpm_minute")
  - Group-only pattern: "the group is group_name" or "group is group_name" (e.g., "list log data in last day, the group is recordsLog")
- Resource names can be:
  - Simple identifiers: "log", "metric", "trace", "stream" (these are valid resource names)
  - Underscore-separated: "service_cpm_minute", "service_instance_cpm_minute", "log_stream"
  - CamelCase: "logStream", "metricStream"
- Group names can be camelCase (e.g., metricsMinute, recordsLog) or underscore-separated (e.g., sw_metric)
- All these patterns should generate the same BydbQL format: SELECT ... FROM RESOURCE_TYPE resource_name IN group_name ...

CRITICAL Resource Name Extraction:
- Extract resource names from the description by looking for:
  1. Explicit resource names mentioned before "in", "of", or possessive markers (e.g., "service_cpm_minute in metricsMinute")
  2. Simple identifiers that match the resource type context (e.g., "log" for STREAM, "metric" for MEASURE)
  3. Underscore-separated identifiers (e.g., "service_cpm_minute", "log_stream")
- When the description mentions a resource type keyword (log, metric, trace, etc.) without an explicit resource name:
  - Extract the keyword itself as the resource name if it appears as a standalone word (e.g., "list log data" → resource name: "log")
  - Examples:
    - "list log data in last day, the group is recordsLog" → type: STREAM (from "log"), name: "log", group: "recordsLog"
    - "get metrics from metricsMinute" → type: MEASURE (from "metrics"), name: "metric" or extract explicit name if present, group: "metricsMinute"
- Group name extraction patterns (in order of priority):
  - "the group is group_name" or "group is group_name" → extract group_name (e.g., "the group is recordsLog" → "recordsLog")
  - "in group_name" or "group group_name" → extract group_name
  - "of group_name" → extract group_name
  - "group_name's" → extract group_name
- CRITICAL: If no group name is found in the description, you MUST find it from the "Available Resources in BanyanDB (Resource -> Group Mapping)" section above. Look up the resource name mentioned in the description in that mapping to find its corresponding group. For example, if the description mentions "zipkin_span" and the mapping shows "zipkin_span" -> Group "default", use "default" as the group. Only use "default" if the resource cannot be found in the mapping.

CRITICAL: Choose the correct query format based on the description:

1. TOPN Query (ONLY use when explicitly requested):
   - Use TOPN format if the description contains ranking keywords (e.g., "top", "highest", "lowest", "best", "worst") followed by a NUMBER (e.g., "top 10", "top 5", "top-N", "topN", "show top 10", "highest 5", "lowest 3", "best 10")
   - The word "show" alone does NOT indicate a TOPN query - it's just a common verb
   - Examples that indicate TOPN: "top 10", "top 5", "show top 10", "highest 5", "lowest 3", "best 10", "top-N"
   - Examples that do NOT indicate TOPN: "show", "show me", "display", "get", "fetch"
   - If TOPN is indicated AND the resource type is MEASURE:
     - Use: SHOW TOP N FROM MEASURE measure_name IN group_name TIME time_condition [AGGREGATE BY agg_function] [ORDER BY [value] [ASC|DESC]]
     - Example: SHOW TOP 10 FROM MEASURE cpu_usage IN default TIME > '-1h' AGGREGATE BY SUM ORDER BY value DESC

2. Common Query (DEFAULT for all other cases):
   - Use SELECT format if the description does NOT contain ranking keywords ("top", "highest", "lowest", "best", "worst") followed by a number
   - This includes descriptions with just "show", "get", "display", "fetch", etc. without ranking keywords + number
   - Use: SELECT fields FROM RESOURCE_TYPE resource_name IN group_name [TIME clause] [AGGREGATE BY clause] [ORDER BY clause] [LIMIT clause]
   - Example: SELECT * FROM MEASURE cpu_usage IN default TIME > '-1h' AGGREGATE BY SUM ORDER BY value DESC LIMIT 10
   - Example with LIMIT: SELECT * FROM TRACE zipkin_span IN default ORDER BY timestamp_millis DESC LIMIT 30

AGGREGATE BY clause Detection:
- Syntax: AGGREGATE BY SUM | MEAN | COUNT | MAX | MIN
- Used to aggregate data points over the time range (SUM for totals, MAX for maximum values, MIN for minimum values, MEAN/AVG for averages, COUNT for counts)
- Extract from natural language keywords:
  - "sum", "total", "totals", "summing" → AGGREGATE BY SUM
  - "max", "maximum", "maximize", "highest value" → AGGREGATE BY MAX
  - "min", "minimum", "minimize", "lowest value" → AGGREGATE BY MIN
  - "mean", "average", "avg", "averaging" → AGGREGATE BY MEAN
  - "count", "counting", "number of" → AGGREGATE BY COUNT
- If the description contains an explicit "AGGREGATE BY" clause, preserve it exactly as provided
- Examples: AGGREGATE BY SUM, AGGREGATE BY MAX, AGGREGATE BY MEAN

ORDER BY clause Detection:
- Syntax: ORDER BY field [ASC|DESC] or ORDER BY TIME [ASC|DESC] (TIME is shorthand for timestamps)
- Fields are flexible: You can use any field from the resource for ordering. Common examples include: latency, start_time, timestamp, timestamp_millis, duration, value
- Extract from natural language:
  - "order by", "sort by" followed by field name → ORDER BY field
  - "highest", "largest", "biggest", "longest", "slowest", "top" → ORDER BY field DESC
  - "lowest", "smallest", "shortest", "fastest", "bottom" → ORDER BY field ASC
  - If the description contains an explicit "ORDER BY" clause, preserve it exactly as provided
- Examples: ORDER BY latency DESC, ORDER BY start_time ASC, ORDER BY TIME DESC
- For TOPN queries: ORDER BY DESC (for highest values) or ORDER BY ASC (for lowest values) - field name is optional

Top-N Query Syntax (for measures):
- Top N key (the field used for ranking) is NOT REQUIRED for measures. TOP N queries can work without specifying a key field.
- ORDER BY clause is OPTIONAL for top N queries on measures. If not specified, the default ordering will be used.
- Do NOT include LIMIT clause in TOPN queries. Use SHOW TOP N syntax instead.

LIMIT clause Detection:
- Syntax: LIMIT N (where N is a positive integer)
- Use LIMIT when the description requests a specific NUMBER OF RESULTS or DATA POINTS:
  - "last N [resource_name]" (e.g., "last 30 zipkin_span") → LIMIT N
  - "first N [resource_name]" → LIMIT N (with ORDER BY ASC)
  - "N [resource_name]" (e.g., "30 zipkin_span") → LIMIT N
  - "show N items" or "get N records" → LIMIT N
- LIMIT is used to restrict the number of returned results, NOT to specify a time range
- When LIMIT is used, typically ORDER BY should also be included to ensure meaningful ordering:
  - "last N" usually implies ORDER BY timestamp/time DESC LIMIT N
  - "first N" usually implies ORDER BY timestamp/time ASC LIMIT N
- LIMIT clause MUST come AFTER ORDER BY clause in the query
- Examples:
  - "list the last 30 zipkin_span order by time" → SELECT * FROM TRACE zipkin_span IN default ORDER BY timestamp_millis DESC LIMIT 30
  - "get first 10 logs" → SELECT * FROM STREAM log IN default ORDER BY timestamp ASC LIMIT 10
  - "show 5 metrics" → SELECT * FROM MEASURE metric IN default LIMIT 5

CRITICAL Clause Ordering Rules (applies to ALL query types):
- Clause order MUST be: TIME (if present), then AGGREGATE BY (if present), then ORDER BY (if present), then LIMIT (if present)
- AGGREGATE BY must ALWAYS come BEFORE ORDER BY
- LIMIT must ALWAYS come AFTER ORDER BY (if ORDER BY is present)

User description: "${description}"${typeof args.resource_type === 'string' && args.resource_type ? `\n\nIMPORTANT: Resource type is explicitly provided: ${args.resource_type.toUpperCase()}. Use this value instead of extracting from description.` : ''}${typeof args.resource_name === 'string' && args.resource_name ? `\n\nIMPORTANT: Resource name is explicitly provided: ${args.resource_name}. Use this value instead of extracting from description.` : ''}${typeof args.group === 'string' && args.group ? `\n\nIMPORTANT: Group name is explicitly provided: ${args.group}. Use this value instead of extracting from description.` : ''}

CRITICAL Preservation Rules:
- Use explicitly provided values (if any) with HIGHEST PRIORITY. Only extract from description if values are NOT explicitly provided.
- Analyze the description to extract ALL of the following (only if not explicitly provided):
  - Resource type (STREAM, MEASURE, TRACE, or PROPERTY) - first detect from keywords like "log", "metric", "trace". If no resource type can be detected from keywords, look up the resource name in the "Available Resources in BanyanDB (Resource -> Group Mapping)" section to find its corresponding resource type.
  - Resource name - follow CRITICAL Resource Name Extraction rules above (extract explicit names or use type keywords like "log", "metric" as resource names)
  - Group name - follow group name extraction patterns above (look for "the group is group_name", "in group_name", etc.). CRITICAL: If no group name is found in the description, look up the resource name in the "Available Resources in BanyanDB (Resource -> Group Mapping)" section to find its corresponding group. Only use "default" if the resource cannot be found in the mapping.
  - TIME clause - understand semantic meaning: distinguish "last N [time units]" (TIME clause) from "last N [data points]" (LIMIT clause)
  - LIMIT clause - extract when description requests specific number of results (e.g., "last 30 zipkin_span" means LIMIT 30, not TIME)
  - AGGREGATE BY clause (if mentioned in natural language or explicitly)
  - ORDER BY clause (if mentioned in natural language or explicitly) - when LIMIT is present, ORDER BY is typically needed for meaningful results
- If the user description contains a TIME clause, you MUST preserve it exactly as provided
- If the user description contains an AGGREGATE BY clause, you MUST preserve it in the generated query exactly as provided
- If the user description contains an ORDER BY clause, you MUST preserve it in the generated query exactly as provided
  - CRITICAL FORMAT SELECTION: 
  - Check if the description contains ranking keywords ("top", "highest", "lowest", "best", "worst") followed by a NUMBER (e.g., "top 10", "top 5", "top-N", "topN", "show top 10", "highest 5", "lowest 3", "best 10")
  - IMPORTANT: The word "show" alone does NOT indicate TOPN - only ranking keywords + number (e.g., "top N", "highest N", "lowest N") indicate TOPN
  - Extract the resource type from the description (STREAM, MEASURE, TRACE, or PROPERTY) - first detect from keywords, but if no type can be detected, look up the resource name in the "Available Resources in BanyanDB (Resource -> Group Mapping)" section to find its corresponding resource type.
  - Extract the resource name from the description (follow CRITICAL Resource Name Extraction rules - extract explicit names or use type keywords as resource names)
  - Extract the group name from the description (follow group name extraction patterns - look for "the group is group_name", "in group_name", etc.). CRITICAL: If no group name is found in the description, look up the resource name in the "Available Resources in BanyanDB (Resource -> Group Mapping)" section to find its corresponding group. Only use "default" if the resource cannot be found in the mapping.
  - Extract TIME clause from natural language - CRITICAL: distinguish "last N [time units]" from "last N [data points]"
  - Extract LIMIT clause when description requests specific number of results (e.g., "last 30 zipkin_span" → LIMIT 30, not TIME)
  - Extract AGGREGATE BY clause from natural language or explicit clause in description
  - Extract ORDER BY clause from natural language or explicit clause in description - when LIMIT is present, ORDER BY is typically needed
  - If YES (contains ranking keyword + number) AND resource type is MEASURE: Use "SHOW TOP N FROM MEASURE measure_name IN group_name TIME time_condition [AGGREGATE BY agg_function] [ORDER BY [value] [ASC|DESC]]"
  - If NO (no ranking keyword + number) OR resource type is not MEASURE: Use "SELECT fields FROM RESOURCE_TYPE resource_name IN group_name [TIME clause] [AGGREGATE BY clause] [ORDER BY clause] [LIMIT clause]"

CRITICAL JSON Response Requirements - Conditional Parameter Inclusion:
- Analyze the description to determine which parameters are needed or indicated
- If the description indicates that no explanations or other parameters are needed, do NOT return those parameters
- The same applies to other parameters - return the corresponding parameters according to what's indicated in the description
- If there is no indication, all parameters will be returned by default
- "bydbql": ALWAYS REQUIRED - Generate the complete BydbQL query using the type, name, and group values specified above along with TIME, LIMIT, AGGREGATE BY, and ORDER BY clauses extracted from description (understand semantic meaning, not just pattern matching)
- "type": Include if explicitly provided or if needed for clarity. ${typeof args.resource_type === 'string' && args.resource_type ? `MUST be "${args.resource_type.toUpperCase()}" (explicitly provided)` : 'Extract from description (STREAM, MEASURE, TRACE, or PROPERTY) or default to STREAM if not indicated'}
- "name": Include if explicitly provided or if needed for clarity. ${typeof args.resource_name === 'string' && args.resource_name ? `MUST be "${args.resource_name}" (explicitly provided)` : 'Extract from description using CRITICAL Resource Name Extraction rules (extract explicit names or use type keywords like "log", "metric", "trace" as resource names when they appear in the description) if not indicated'}
- "group": Include if explicitly provided or if needed for clarity. ${typeof args.group === 'string' && args.group ? `MUST be "${args.group}" (explicitly provided)` : 'Extract from description using group name extraction patterns (look for "the group is group_name", "in group_name", etc.) if not indicated'}
- "explanations": Include ONLY if the description indicates that explanations are needed or requested. If the description indicates no explanations are needed, omit this parameter entirely. If not indicated, include by default.

Return a JSON object with the following structure (include only parameters that are needed or indicated):
- "bydbql": ALWAYS REQUIRED
- "type": Include if explicitly provided above or if needed for clarity
- "name": Include if explicitly provided above or if needed for clarity  
- "group": Include if explicitly provided above or if needed for clarity
- "explanations": Include ONLY if the description indicates explanations are needed. If the description indicates no explanations are needed, omit this parameter.

Example (with all parameters, but omit any that are not needed):
{
  "bydbql": "the complete BydbQL query using the values specified above",
  "type": ${typeof args.resource_type === 'string' && args.resource_type ? `"${args.resource_type.toUpperCase()}"` : '"extract from description (STREAM/MEASURE/TRACE/PROPERTY)"'},
  "name": ${typeof args.resource_name === 'string' && args.resource_name ? `"${args.resource_name}"` : '"extract from description"'},
  "group": ${typeof args.group === 'string' && args.group ? `"${args.group}"` : '"extract from description"'},
  "explanations": "brief explanation (omit if not needed)"
}

IMPORTANT: 
- If values are explicitly provided above, you MUST use those exact values in your JSON response. Do NOT extract different values from the description if values are explicitly provided.
- Only include parameters that are indicated as needed in the description. If the description indicates no explanations or other parameters are needed, omit them.
- If there is no indication about which parameters to include, return all parameters by default.
- Return ONLY the JSON object, no markdown formatting or additional text.`;
}
