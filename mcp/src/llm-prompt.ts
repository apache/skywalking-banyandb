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
export function generateQueryPrompt(
  description: string,
  resourceType: string,
  resourceName: string,
  group: string,
  aggregateByClause?: string | null,
  orderByClause?: string | null,
): string {
  return `You are a BydbQL query generator. Convert the following natural language description into a valid BydbQL query.

BydbQL Syntax:
- Resource types: STREAM, MEASURE, TRACE, PROPERTY
- TIME clause examples: TIME >= '-1h', TIME > '-1d', TIME BETWEEN '-24h' AND '-1h'
- Use TIME > for "from last X" (e.g., "from last day" = TIME > '-1d'), TIME >= for "since" or "in the last X"
- For "from last day", use TIME > '-1d' (not TIME >=)

CRITICAL: Choose the correct query format based on the description:

1. If the description contains "TOP" or similar words (e.g., "top 10", "top 5", "highest N", "show top N", "top N", "top-N", "topN") AND the resource type is MEASURE:
   - Use: SHOW TOP N FROM MEASURE measure_name IN group_name TIME time_condition [AGGREGATE BY agg_function] [ORDER BY [value] [ASC|DESC]]
   - Example: SHOW TOP 10 FROM MEASURE cpu_usage IN default TIME > '-1h' AGGREGATE BY SUM ORDER BY value DESC

2. If the description does NOT contain "TOP" or similar words, OR the resource type is not MEASURE:
   - Use: SELECT fields FROM RESOURCE_TYPE resource_name IN group_name [TIME clause] [AGGREGATE BY clause] [ORDER BY clause]
   - Example: SELECT * FROM MEASURE cpu_usage IN default TIME > '-1h' AGGREGATE BY SUM ORDER BY value DESC

AGGREGATE BY clause:
- Syntax: AGGREGATE BY SUM | MEAN | COUNT | MAX | MIN
- Used to aggregate data points over the time range (SUM for totals, MAX for maximum values, MIN for minimum values, MEAN/AVG for averages, COUNT for counts)
- Examples: AGGREGATE BY SUM, AGGREGATE BY MAX, AGGREGATE BY MEAN

ORDER BY clause:
- Syntax: ORDER BY field [ASC|DESC] or ORDER BY TIME [ASC|DESC] (TIME is shorthand for timestamps)
- Fields are flexible: You can use any field from the resource for ordering. Common examples include: latency, start_time, timestamp, timestamp_millis, duration, value
- Examples: ORDER BY latency DESC, ORDER BY start_time ASC, ORDER BY TIME DESC
- For TOPN queries: ORDER BY DESC (for highest values) or ORDER BY ASC (for lowest values) - field name is optional

Top-N Query Syntax (for measures):
- Top N key (the field used for ranking) is NOT REQUIRED for measures. TOP N queries can work without specifying a key field.
- ORDER BY clause is OPTIONAL for top N queries on measures. If not specified, the default ordering will be used.
- Do NOT include LIMIT clause in TOPN queries. Use SHOW TOP N syntax instead.

CRITICAL Clause Ordering Rules (applies to ALL query types):
- Clause order MUST be: TIME (if present), then AGGREGATE BY (if present), then ORDER BY (if present)
- AGGREGATE BY must ALWAYS come BEFORE ORDER BY

User description: "${description}"

IMPORTANT: Use these EXACT values detected from the description:
- Resource type: ${resourceType.toUpperCase()}
- Resource name: ${resourceName}
- Group name: ${group}${aggregateByClause ? `\n- AGGREGATE BY clause: ${aggregateByClause}` : ''}${orderByClause ? `\n- ORDER BY clause: ${orderByClause}` : ''}

CRITICAL Preservation Rules:
- If the user description contains a TIME clause, you MUST preserve it exactly as provided
- If the user description contains an AGGREGATE BY clause, you MUST preserve it in the generated query${aggregateByClause ? `. Use this EXACT AGGREGATE BY clause: ${aggregateByClause}` : ''}
- If the user description contains an ORDER BY clause, you MUST preserve it in the generated query${orderByClause ? `. Use this EXACT ORDER BY clause: ${orderByClause}` : ''}
- CRITICAL FORMAT SELECTION: Check if the description contains "TOP" or similar words (e.g., "top 10", "top 5", "highest N", "show top N", "top N", "top-N", "topN")
  - If YES and resource type is MEASURE: Use "SHOW TOP N FROM MEASURE measure_name IN group_name TIME time_condition [AGGREGATE BY agg_function] [ORDER BY [value] [ASC|DESC]]"
  - If NO or resource type is not MEASURE: Use "SELECT fields FROM RESOURCE_TYPE resource_name IN group_name [TIME clause] [AGGREGATE BY clause] [ORDER BY clause]"

Generate ONLY the BydbQL query using these exact values. Do not change the resource name or group name. Do not include explanations or markdown formatting.`;
}
