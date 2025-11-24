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
- SELECT fields FROM RESOURCE_TYPE resource_name IN group_name [TIME clause] [AGGREGATE BY clause] [ORDER BY clause]
- Resource types: STREAM, MEASURE, TRACE, PROPERTY
- TIME clause examples: TIME >= '-1h', TIME > '-30m', TIME BETWEEN '-24h' AND '-1h'
- Use TIME > for "after" or "more than", TIME >= for "from" or "since"

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
- SHOW TOP N FROM MEASURE measure_name IN group_name TIME time_condition [AGGREGATE BY agg_function] [ORDER BY [value] [ASC|DESC]]
- Do NOT include LIMIT clause in TOPN queries. Use SHOW TOP N syntax instead.

CRITICAL Clause Ordering Rules (applies to ALL query types):
- Clause order MUST be: TIME (if present), then AGGREGATE BY (if present), then ORDER BY (if present)
- AGGREGATE BY must ALWAYS come BEFORE ORDER BY

User description: "${description}"

IMPORTANT: Use these EXACT values detected from the description:
- Resource type: ${resourceType.toUpperCase()}
- Resource name: ${resourceName}
- Group name: ${group}${aggregateByClause ? `\n- AGGREGATE BY clause: ${aggregateByClause}` : ""}${orderByClause ? `\n- ORDER BY clause: ${orderByClause}` : ""}

CRITICAL Preservation Rules:
- If the user description contains a TIME clause, you MUST preserve it exactly as provided
- If the user description contains an AGGREGATE BY clause, you MUST preserve it in the generated query${aggregateByClause ? `. Use this EXACT AGGREGATE BY clause: ${aggregateByClause}` : ""}
- If the user description contains an ORDER BY clause, you MUST preserve it in the generated query${orderByClause ? `. Use this EXACT ORDER BY clause: ${orderByClause}` : ""}

Generate ONLY the BydbQL query using these exact values. Do not change the resource name or group name. Do not include explanations or markdown formatting.`;
}
