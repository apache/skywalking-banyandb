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

import OpenAI from "openai";

/**
 * QueryGenerator converts natural language descriptions to BydbQL queries.
 * Supports both LLM-based generation (when API key is provided) and pattern-based fallback.
 */
export class QueryGenerator {
  private openaiClient: OpenAI | null = null;

  constructor(apiKey?: string) {
    if (apiKey) {
      this.openaiClient = new OpenAI({
        apiKey: apiKey,
        timeout: 20000, // 20 seconds timeout for OpenAI API calls
      });
    }
  }
  private timePatterns: RegExp[] = [
    /(last|past|recent)\s+(\d+)\s*(hour|hours|hr|hrs|h)/i,
    /(last|past|recent)\s+(\d+)\s*(minute|minutes|min|mins|m)/i,
    /(last|past|recent)\s+(\d+)\s*(day|days|d)/i,
    /(last|past|recent)\s+(\d+)\s*(week|weeks|w)/i,
    /(today|yesterday|now)/i,
  ];

  private resourcePatterns: Map<string, RegExp> = new Map([
    ["stream", /(log|logs|stream|streams|event|events)/i],
    ["measure", /(metric|metrics|measure|measures|stat|stats|statistics)/i],
    ["trace", /(trace|traces|span|spans|tracing)/i],
    ["property", /(property|properties|metadata|config)/i],
  ]);

  private filterPatterns: Map<string, RegExp> = new Map([
    ["error", /(error|errors|failed|failure|exception|exceptions)/i],
    ["warning", /(warn|warning|warnings)/i],
    [
      "service",
      /(service|services|app|application|apps)\s+(?:named|called|is|are)?\s*['"]?([a-zA-Z0-9_-]+)['"]?/i,
    ],
  ]);

  /**
   * Generate a BydbQL query from a natural language description.
   */
  async generateQuery(
    description: string,
    args: Record<string, any>
  ): Promise<string> { 
    // Use LLM if available, otherwise fall back to pattern matching
    if (this.openaiClient) {
      try {
        return await this.generateQueryWithLLM(description, args);
      } catch (error) {
        // Fall through to pattern-based generation
      }
    }
    return this.generateQueryWithPatterns(description, args);
  }

  /**
   * Generate query using LLM (OpenAI).
   */
  private async generateQueryWithLLM(
    description: string,
    args: Record<string, any>
  ): Promise<string> {
    if (!this.openaiClient) {
      throw new Error("OpenAI client not initialized");
    }

    const resourceType = args.resource_type || this.detectResourceType(description, args) || "stream";
    const resourceName = args.resource_name || this.detectResourceName(description) || "sw";
    const group = args.group || this.detectGroup(description) || "default";

    const prompt = `You are a BydbQL query generator. Convert the following natural language description into a valid BydbQL query.

BydbQL Syntax:
- SELECT fields FROM RESOURCE_TYPE resource_name IN group_name [TIME clause] [WHERE clause] [LIMIT n]
- Resource types: STREAM, MEASURE, TRACE, PROPERTY
- TIME clause examples: TIME >= '-1h', TIME > '-30m', TIME BETWEEN '-24h' AND '-1h'
- WHERE clause examples: WHERE status = 'error', WHERE service_id = 'webapp'
- Default LIMIT is 100
- Use TIME > for "after" or "more than", TIME >= for "from" or "since"

User description: "${description}"

IMPORTANT: Use these EXACT values detected from the description:
- Resource type: ${resourceType.toUpperCase()}
- Resource name: ${resourceName}
- Group name: ${group}

Generate ONLY the BydbQL query using these exact values. Do not change the resource name or group name. Do not include explanations or markdown formatting.`;

    const completion = await Promise.race([
      this.openaiClient.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [
          {
            role: "system",
            content: "You are a BydbQL query generator. Always return only the query, no explanations.",
          },
          {
            role: "user",
            content: prompt,
          },
        ],
        temperature: 0.1,
        max_tokens: 200,
      }),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("OpenAI API timeout after 20 seconds")), 20000)
      ),
    ]);

    const query = completion.choices[0]?.message?.content?.trim();
    if (!query) {
      throw new Error("Empty response from LLM");
    }

    // Clean up the response (remove markdown code blocks if present)
    return query.replace(/^```(?:bydbql|sql)?\n?/i, "").replace(/\n?```$/i, "").trim();
  }

  /**
   * Generate query using pattern matching (fallback method).
   */
  private generateQueryWithPatterns(
    description: string,
    args: Record<string, any>
  ): string {
    // Determine resource type
    const resourceType = this.detectResourceType(description, args) || "stream";

    // Extract resource name if provided
    let resourceName =
      args.resource_name || this.detectResourceName(description);
    if (!resourceName) {
      // Use common defaults
      switch (resourceType) {
        case "stream":
          resourceName = "sw";
          break;
        case "measure":
          resourceName = "service_cpm";
          break;
        case "trace":
          resourceName = "sw";
          break;
        case "property":
          resourceName = "sw";
          break;
      }
    }

    // Extract group
    const group = args.group || this.detectGroup(description) || "default";
    
    // Build time clause
    const timeClause = this.buildTimeClause(description);

    // Build WHERE clause
    const whereClause = this.buildWhereClause(description);

    // Build SELECT clause
    const selectClause = this.buildSelectClause(description, resourceType);

    // Construct the query
    let query = `SELECT ${selectClause} FROM ${resourceType.toUpperCase()} ${resourceName} IN ${group}`;

    if (timeClause) {
      query += ` ${timeClause}`;
    }

    if (whereClause) {
      query += ` ${whereClause}`;
    }

    query += " LIMIT 100";

    return query;
  }

  /**
   * Detect the resource type from the description or args.
   */
  private detectResourceType(
    description: string,
    args: Record<string, any>
  ): string | null {
    // Check args first
    if (args.resource_type) {
      return args.resource_type.toLowerCase();
    }

    // Detect from description
    const lowerDescription = description.toLowerCase();
    for (const [type, pattern] of this.resourcePatterns) {
      if (pattern.test(lowerDescription)) {
        return type;
      }
    }

    return null;
  }

  /**
   * Try to detect resource name from description.
   */
  private detectResourceName(description: string): string | null {
    // Common words that shouldn't be treated as resource names
    const commonWords = new Set(['service', 'services', 'metric', 'metrics', 'measure', 'measures', 
                                 'stream', 'streams', 'trace', 'traces', 'property', 'properties',
                                 'from', 'query', 'get', 'show', 'fetch', 'select', 'where', 'data',
                                 'last', 'past', 'recent', 'hour', 'hours', 'minute', 'minutes',
                                 'day', 'days', 'week', 'weeks', 'today', 'yesterday', 'now']);
    
    // First, try to detect group name to exclude it from resource name detection
    const groupMatch = description.match(/\b(?:in|group)\s+['"]?([a-zA-Z][a-zA-Z0-9_]*_[a-zA-Z0-9_]+)['"]?/i) ||
                       description.match(/\b(?:in|group)\s+['"]?([a-zA-Z][a-zA-Z0-9_]+)['"]?/i);
    const groupName = groupMatch ? groupMatch[1] : null;
    
    const patterns = [
      // Pattern for common measure names like service_cpm_minute, service_instance_cpm_minute, etc. (most specific, check first)
      /\b([a-zA-Z][a-zA-Z0-9_]*_(?:cpm|rpm|apdex|sla|percentile)_(?:minute|hour|day))\b/i,
      // Pattern: resource name right before "in" keyword: "service_cpm_minute in sw_metric" (check early to catch this pattern)
      /\b([a-zA-Z][a-zA-Z0-9_-]+)\s+in\s+[a-zA-Z]/i,
      // Explicit resource type patterns: "from measure service_cpm_minute" or "from stream log"
      /(?:from|in|of|query|get|show|fetch)\s+(?:the\s+)?(?:stream|measure|trace|property)\s+['"]?([a-zA-Z0-9_-]+)['"]?/i,
      // Resource type followed by name: "measure service_cpm_minute" or "stream log"
      /(?:stream|measure|trace|property)\s+['"]?([a-zA-Z0-9_-]+)['"]?/i,
      // Pattern for underscore-separated names (at least 1 underscore suggests a resource name)
      // But exclude group names
      /\b([a-zA-Z][a-zA-Z0-9_]*_[a-zA-Z0-9_]+)\b/i,
      // Simple resource names like "log", "metrics", etc. (but not common words) - check last
      /\b(?:query|get|show|fetch|from)\s+['"]?([a-zA-Z][a-zA-Z0-9_-]{1,})['"]?(?:\s+in|\s+from|$)/i,
    ];

    for (let i = 0; i < patterns.length; i++) {
      const pattern = patterns[i];
      const matches = description.match(pattern);
      if (matches && matches[1]) {
        const resourceName = matches[1];
        // Skip if this matches the group name
        if (groupName && resourceName.toLowerCase() === groupName.toLowerCase()) {
          continue;
        }
        // Filter out common words that aren't resource names
        if (!commonWords.has(resourceName.toLowerCase())) {
          return resourceName;
        }
      }
    }

    return null;
  }

  /**
   * Try to detect group name from description.
   */
  private detectGroup(description: string): string | null {
    const patterns = [
      // Pattern: "in sw_metric" or "group sw_metric" - most specific, check first
      // Match group names with underscores: sw_metric, sw_recordsLog, etc.
      /\b(?:in|group)\s+['"]?([a-zA-Z][a-zA-Z0-9_]*_[a-zA-Z0-9_]+)['"]?/i,
      // Pattern: "in sw_metric" or "group sw_metric" - also match simple names
      /\b(?:in|group)\s+['"]?([a-zA-Z][a-zA-Z0-9_]+)['"]?/i,
      // Pattern: "from group sw_metric" 
      /\bfrom\s+group\s+['"]?([a-zA-Z][a-zA-Z0-9_]*_[a-zA-Z0-9_]+)['"]?/i,
      // Pattern: "from group sw_metric" - also match simple names
      /\bfrom\s+group\s+['"]?([a-zA-Z][a-zA-Z0-9_]+)['"]?/i,
    ];

    for (let i = 0; i < patterns.length; i++) {
      const pattern = patterns[i];
      const matches = description.match(pattern);
      if (matches && matches[1]) {
        const groupName = matches[1];
        // Filter out common words that aren't group names
        const commonWords = new Set(['default', 'from', 'query', 'select', 'where', 'time', 
                                     'stream', 'measure', 'trace', 'property', 'data', 'the']);
        if (!commonWords.has(groupName.toLowerCase())) {
          return groupName;
        }
      }
    }

    return null;
  }

  /**
   * Build a TIME clause from the description.
   */
  private buildTimeClause(description: string): string {
    const lowerDescription = description.toLowerCase();

    // Check for relative time patterns
    for (const pattern of this.timePatterns) {
      const matches = lowerDescription.match(pattern);
      if (matches) {
        if (matches[0] === "now" || matches[0] === "today") {
          return "TIME >= '-1h'";
        }
        if (matches[0] === "yesterday") {
          return "TIME BETWEEN '-24h' AND '-1h'";
        }

        // Extract number and unit
        if (matches.length >= 3) {
          const unit = matches[matches.length - 1];
          const numberStr = matches[matches.length - 2];
          const number = parseInt(numberStr, 10);
          let duration: string;

          switch (unit) {
            case "h":
            case "hour":
            case "hours":
            case "hr":
            case "hrs":
              duration = `-${number}h`;
              break;
            case "m":
            case "minute":
            case "minutes":
            case "min":
            case "mins":
              duration = `-${number}m`;
              break;
            case "d":
            case "day":
            case "days":
              duration = `-${number}d`;
              break;
            case "w":
            case "week":
            case "weeks":
              duration = `-${number * 7}d`;
              break;
            default:
              duration = "-1h";
          }

          return `TIME >= '${duration}'`;
        }
      }
    }

    // Default to last hour
    return "TIME >= '-1h'";
  }

  /**
   * Build a WHERE clause from the description.
   */
  private buildWhereClause(description: string): string {
    const conditions: string[] = [];
    const lowerDescription = description.toLowerCase();

    // Check for error patterns
    if (this.filterPatterns.get("error")!.test(lowerDescription)) {
      conditions.push("status = 'error'");
    }

    // Check for warning patterns
    if (this.filterPatterns.get("warning")!.test(lowerDescription)) {
      conditions.push("status = 'warning'");
    }

    // Check for service patterns
    const serviceMatch = lowerDescription.match(
      this.filterPatterns.get("service")!
    );
    if (serviceMatch && serviceMatch[2]) {
      conditions.push(`service_id = '${serviceMatch[2]}'`);
    }

    if (conditions.length === 0) {
      return "";
    }

    return "WHERE " + conditions.join(" AND ");
  }

  /**
   * Build a SELECT clause from the description.
   */
  private buildSelectClause(description: string, resourceType: string): string {
    const lowerDescription = description.toLowerCase();

    // Check for specific field requests
    if (
      lowerDescription.includes("count") ||
      lowerDescription.includes("number")
    ) {
      if (resourceType === "measure") {
        return "COUNT(*)";
      }
    }

    // Default to select all
    return "*";
  }
}

