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

/**
 * Generate the LLM prompt for converting natural language to BydbQL queries.
 */
export function generateQueryPrompt(description: string, args: Record<string, unknown>): string {
  return `You are a BydbQL query generator. Convert the following natural language description into a valid BydbQL query.

BydbQL Syntax:
- Resource types: STREAM, MEASURE, TRACE, PROPERTY
- TIME clause examples: TIME >= '-1h', TIME > '-1d', TIME BETWEEN '-24h' AND '-1h'
- Use TIME > for "from last X" (e.g., "from last day" = TIME > '-1d'), TIME >= for "since" or "in the last X"
- For "from last day", use TIME > '-1d' (not TIME >=)

Resource Type Detection:
- STREAM: for logs, events, streams (keywords: log, logs, stream, streams, event, events)
- MEASURE: for metrics, measurements, statistics (keywords: metric, metrics, measure, measures, stat, stats, statistics)
- TRACE: for traces, spans, tracing data (keywords: trace, traces, span, spans, tracing)
- PROPERTY: for properties, metadata, configuration (keywords: property, properties, metadata, config)
- If no clear resource type is mentioned, default to STREAM

Resource and Group Name Patterns:
- The user description may express resource and group relationships in different ways:
  - Standard pattern: "resource_name in group_name" (e.g., "service_cpm_minute in metricsMinute")
  - Alternative pattern: "resource_name of group_name" (e.g., "service_cpm_minute of metricsMinute")
  - Possessive pattern: "group_name's resource_name" (e.g., "metricsMinute's service_cpm_minute")
- Resource names often contain underscores (e.g., service_cpm_minute, service_instance_cpm_minute)
- Group names can be camelCase (e.g., metricsMinute) or underscore-separated (e.g., sw_metric)
- If no group name is mentioned, use "default"
- If no resource name is mentioned, use common defaults based on resource type:
  - STREAM: "sw"
  - MEASURE: "service_cpm"
  - TRACE: "sw"
  - PROPERTY: "sw"
- All these patterns should generate the same BydbQL format: SELECT ... FROM RESOURCE_TYPE resource_name IN group_name ...

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
   - Use: SELECT fields FROM RESOURCE_TYPE resource_name IN group_name [TIME clause] [AGGREGATE BY clause] [ORDER BY clause]
   - Example: SELECT * FROM MEASURE cpu_usage IN default TIME > '-1h' AGGREGATE BY SUM ORDER BY value DESC

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

CRITICAL Clause Ordering Rules (applies to ALL query types):
- Clause order MUST be: TIME (if present), then AGGREGATE BY (if present), then ORDER BY (if present)
- AGGREGATE BY must ALWAYS come BEFORE ORDER BY

User description: "${description}"${typeof args.resource_type === 'string' && args.resource_type ? `\n\nIMPORTANT: Resource type is explicitly provided: ${args.resource_type.toUpperCase()}. Use this value instead of extracting from description.` : ''}${typeof args.resource_name === 'string' && args.resource_name ? `\n\nIMPORTANT: Resource name is explicitly provided: ${args.resource_name}. Use this value instead of extracting from description.` : ''}${typeof args.group === 'string' && args.group ? `\n\nIMPORTANT: Group name is explicitly provided: ${args.group}. Use this value instead of extracting from description.` : ''}

CRITICAL Preservation Rules:
- Use explicitly provided values (if any) with HIGHEST PRIORITY. Only extract from description if values are NOT explicitly provided.
- Analyze the description to extract ALL of the following (only if not explicitly provided):
  - Resource type (STREAM, MEASURE, TRACE, or PROPERTY)
  - Resource name (or use defaults if not found)
  - Group name (or use "default" if not found)
  - AGGREGATE BY clause (if mentioned in natural language or explicitly)
  - ORDER BY clause (if mentioned in natural language or explicitly)
- If the user description contains a TIME clause, you MUST preserve it exactly as provided
- If the user description contains an AGGREGATE BY clause, you MUST preserve it in the generated query exactly as provided
- If the user description contains an ORDER BY clause, you MUST preserve it in the generated query exactly as provided
- CRITICAL FORMAT SELECTION: 
  - Check if the description contains ranking keywords ("top", "highest", "lowest", "best", "worst") followed by a NUMBER (e.g., "top 10", "top 5", "top-N", "topN", "show top 10", "highest 5", "lowest 3", "best 10")
  - IMPORTANT: The word "show" alone does NOT indicate TOPN - only ranking keywords + number (e.g., "top N", "highest N", "lowest N") indicate TOPN
  - Extract the resource type from the description (STREAM, MEASURE, TRACE, or PROPERTY)
  - Extract the resource name from the description (or use defaults if not found)
  - Extract the group name from the description (or use "default" if not found)
  - Extract AGGREGATE BY clause from natural language or explicit clause in description
  - Extract ORDER BY clause from natural language or explicit clause in description
  - If YES (contains ranking keyword + number) AND resource type is MEASURE: Use "SHOW TOP N FROM MEASURE measure_name IN group_name TIME time_condition [AGGREGATE BY agg_function] [ORDER BY [value] [ASC|DESC]]"
  - If NO (no ranking keyword + number) OR resource type is not MEASURE: Use "SELECT fields FROM RESOURCE_TYPE resource_name IN group_name [TIME clause] [AGGREGATE BY clause] [ORDER BY clause]"

CRITICAL JSON Response Requirements - Use these EXACT values in your response:
${typeof args.resource_type === 'string' && args.resource_type ? `- "type": MUST be "${args.resource_type.toUpperCase()}" (explicitly provided)` : '- "type": Extract from description (STREAM, MEASURE, TRACE, or PROPERTY) or default to STREAM'}
${typeof args.resource_name === 'string' && args.resource_name ? `- "name": MUST be "${args.resource_name}" (explicitly provided)` : '- "name": Extract from description or use defaults based on resource type'}
${typeof args.group === 'string' && args.group ? `- "group": MUST be "${args.group}" (explicitly provided)` : '- "group": Extract from description'}
- "bydbql": Generate the complete BydbQL query using the type, name, and group values specified above along with TIME, AGGREGATE BY, and ORDER BY clauses extracted from description
- "explanations": Brief explanation of what the query does and how values were determined (mention if values were explicitly provided or extracted)

Return a JSON object with the following structure:
{
  "bydbql": "the complete BydbQL query using the values specified above",
  "group": ${typeof args.group === 'string' && args.group ? `"${args.group}"` : '"extract from description"'},
  "name": ${typeof args.resource_name === 'string' && args.resource_name ? `"${args.resource_name}"` : '"extract from description"'},
  "type": ${typeof args.resource_type === 'string' && args.resource_type ? `"${args.resource_type.toUpperCase()}"` : '"extract from description (STREAM/MEASURE/TRACE/PROPERTY)"'},
  "explanations": "brief explanation"
}

IMPORTANT: If values are explicitly provided above, you MUST use those exact values in your JSON response. Do NOT extract different values from the description if values are explicitly provided. Return ONLY the JSON object, no markdown formatting or additional text.`;
}
