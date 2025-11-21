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
import { generateQueryPrompt } from "./llm-prompt.js";

/**
 * QueryGenerator converts natural language descriptions to BydbQL queries.
 * Supports both LLM-based generation (when API key is provided) and pattern-based fallback.
 */
export class QueryGenerator {
  private static readonly OPENAI_API_TIMEOUT_MS = 20000; // 20 seconds timeout for TARS API calls

  private openaiClient: OpenAI | null = null;

  constructor(apiKey?: string, baseURL?: string) {
    // Validate API key format before creating client
    if (apiKey && apiKey.trim().length > 0) {
      const trimmedKey = apiKey.trim();
      if (trimmedKey.length < 10) {
        console.error(
          "[QueryGenerator] Warning: API key appears to be too short. LLM query generation may fail.",
        );
      }
      this.openaiClient = new OpenAI({
        apiKey: trimmedKey,
        ...(baseURL && { baseURL }),
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

  /**
   * Generate a BydbQL query from a natural language description.
   */
  async generateQuery(
    description: string,
    args: Record<string, any>,
  ): Promise<string> {
    // Use LLM if available, otherwise fall back to pattern matching
    if (this.openaiClient) {
      try {
        return await this.generateQueryWithLLM(description, args);
      } catch (error: any) {
        // Check for API key authentication errors
        if (
          error?.status === 401 ||
          error?.message?.includes("401") ||
          error?.message?.includes("Invalid API key")
        ) {
          console.error(
            "[QueryGenerator] API key authentication failed. Falling back to pattern-based generation.",
          );
          console.error(
            "[QueryGenerator] Error details:",
            error.message || error,
          );
          // Disable LLM client to prevent repeated failures
          this.openaiClient = null;
        } else {
          // For other errors (timeout, network, etc.), log but don't disable
          console.error(
            "[QueryGenerator] Error generating query with LLM:",
            error.message || error,
          );
        }
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
    args: Record<string, any>,
  ): Promise<string> {
    if (!this.openaiClient) {
      throw new Error("OpenAI client not initialized");
    }
    const resourceType =
      args.resource_type ||
      this.detectResourceType(description, args) ||
      "stream";
    const resourceName =
      args.resource_name || this.detectResourceName(description);
    const group = args.group || this.detectGroup(description);

    // Extract existing clauses if present
    const aggregateByClause =
      this.extractExistingAggregateByClause(description);
    const orderByClause = this.extractExistingOrderByClause(description);

    const prompt = generateQueryPrompt(
      description,
      resourceType,
      resourceName,
      group,
      aggregateByClause,
      orderByClause,
    );

    const completion = await Promise.race([
      this.openaiClient.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [
          {
            role: "system",
            content:
              "You are a BydbQL query generator. Always return only the query, no explanations.",
          },
          {
            role: "user",
            content: prompt,
          },
        ],
      }),
      new Promise<never>((_, reject) =>
        setTimeout(
          () =>
            reject(
              new Error(
                `TARS API timeout after ${QueryGenerator.OPENAI_API_TIMEOUT_MS / 1000} seconds`,
              ),
            ),
          QueryGenerator.OPENAI_API_TIMEOUT_MS,
        ),
      ),
    ]);

    const query = completion.choices[0]?.message?.content?.trim();
    if (!query) {
      throw new Error("Empty response from LLM");
    }

    // Clean up the response (remove markdown code blocks if present)
    return query
      .replace(/^```(?:bydbql|sql)?\n?/i, "")
      .replace(/\n?```$/i, "")
      .trim();
  }

  /**
   * Generate query using pattern matching (fallback method).
   */
  private generateQueryWithPatterns(
    description: string,
    args: Record<string, any>,
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

    // Build ORDER BY clause
    const orderByClause = this.buildOrderByClause(description);

    // Build AGGREGATE BY clause
    const aggregateByClause = this.buildAggregateByClause(description);

    // Build SELECT clause
    const selectClause = this.buildSelectClause(description, resourceType);

    // Construct the query
    let query = `SELECT ${selectClause} FROM ${resourceType.toUpperCase()} ${resourceName} IN ${group}`;

    if (timeClause) {
      query += ` ${timeClause}`;
    }
    if (aggregateByClause) {
      query += ` ${aggregateByClause}`;
    }
    if (orderByClause) {
      query += ` ${orderByClause}`;
    }

    return query;
  }

  /**
   * Detect the resource type from the description or args.
   */
  private detectResourceType(
    description: string,
    args: Record<string, any>,
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
    const commonWords = new Set([
      "service",
      "services",
      "metric",
      "metrics",
      "measure",
      "measures",
      "stream",
      "streams",
      "trace",
      "traces",
      "property",
      "properties",
      "from",
      "query",
      "get",
      "show",
      "fetch",
      "select",
      "where",
      "data",
      "last",
      "past",
      "recent",
      "hour",
      "hours",
      "minute",
      "minutes",
      "day",
      "days",
      "week",
      "weeks",
      "today",
      "yesterday",
      "now",
    ]);

    // First, try to detect group name to exclude it from resource name detection
    const groupMatch =
      description.match(
        /\b(?:in|group)\s+['"]?([a-zA-Z][a-zA-Z0-9_]*_[a-zA-Z0-9_]+)['"]?/i,
      ) ||
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
        if (
          groupName &&
          resourceName.toLowerCase() === groupName.toLowerCase()
        ) {
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
        const commonWords = new Set([
          "from",
          "query",
          "select",
          "where",
          "time",
          "stream",
          "measure",
          "trace",
          "property",
          "data",
          "the",
        ]);
        if (!commonWords.has(groupName.toLowerCase())) {
          return groupName;
        }
      }
    }

    return null;
  }

  /**
   * Extract existing TIME clause from the description if present.
   */
  private extractExistingTimeClause(description: string): string | null {
    // Pattern to match TIME clauses: TIME [operator] '[value]' or TIME BETWEEN '[value1]' AND '[value2]'
    // Match patterns like: TIME > '-24h', TIME >= '-1h', TIME BETWEEN '-24h' AND '-1h'
    const timeClausePatterns = [
      // TIME BETWEEN pattern
      /\bTIME\s+BETWEEN\s+['"]([^'"]+)['"]\s+AND\s+['"]([^'"]+)['"]/i,
      // TIME with comparison operators
      /\bTIME\s+(>=|<=|>|<|=)\s+['"]([^'"]+)['"]/i,
    ];

    for (const pattern of timeClausePatterns) {
      const match = description.match(pattern);
      if (match) {
        if (match[0].includes("BETWEEN")) {
          // TIME BETWEEN pattern
          return `TIME BETWEEN '${match[1]}' AND '${match[2]}'`;
        } else {
          // TIME with comparison operator
          return `TIME ${match[1]} '${match[2]}'`;
        }
      }
    }
    return null;
  }

  /**
   * Build a TIME clause from the description.
   */
  private buildTimeClause(description: string): string {
    // First, check if there's already a TIME clause in the input
    const existingTimeClause = this.extractExistingTimeClause(description);
    if (existingTimeClause) {
      return existingTimeClause;
    }

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

  /**
   * Build an ORDER BY clause from the description.
   */
  private buildOrderByClause(description: string): string {
    // First, check if there's already an ORDER BY clause in the input
    const existingOrderBy = this.extractExistingOrderByClause(description);
    if (existingOrderBy) {
      return existingOrderBy;
    }

    const lowerDescription = description.toLowerCase();

    // Explicit "order by" patterns (highest priority)
    const orderByMatch = description.match(
      /order\s+by\s+(\w+)(?:\s+(desc|asc|descending|ascending))?/i,
    );
    if (orderByMatch) {
      const field = orderByMatch[1];
      const direction = orderByMatch[2]
        ? orderByMatch[2].toLowerCase().startsWith("desc")
          ? "DESC"
          : "ASC"
        : "DESC"; // Default to DESC if not specified
      return `ORDER BY ${field} ${direction}`;
    }

    // "sort by" patterns
    const sortByMatch = description.match(
      /sort\s+by\s+(\w+)(?:\s+(desc|asc|descending|ascending))?/i,
    );
    if (sortByMatch) {
      const field = sortByMatch[1];
      const direction = sortByMatch[2]
        ? sortByMatch[2].toLowerCase().startsWith("desc")
          ? "DESC"
          : "ASC"
        : "DESC";
      return `ORDER BY ${field} ${direction}`;
    }

    // Natural language patterns for ordering
    const highestMatch = description.match(
      /(highest|largest|biggest|longest|slowest|top)\s+(?:by\s+)?(\w+)/i,
    );
    if (highestMatch) {
      return `ORDER BY ${highestMatch[2]} DESC`;
    }

    const lowestMatch = description.match(
      /(lowest|smallest|shortest|fastest|bottom)\s+(?:by\s+)?(\w+)/i,
    );
    if (lowestMatch) {
      return `ORDER BY ${lowestMatch[2]} ASC`;
    }

    // Check for common field names that suggest ordering
    const commonOrderFields = [
      "latency",
      "duration",
      "start_time",
      "timestamp",
      "time",
      "value",
      "response_time",
    ];

    for (const field of commonOrderFields) {
      // Check if the field is mentioned with ordering context
      const fieldPattern = new RegExp(
        `(?:order|sort|highest|lowest|largest|smallest|top|bottom).*?${field}|${field}.*?(?:desc|asc|descending|ascending|highest|lowest)`,
        "i",
      );
      if (fieldPattern.test(lowerDescription)) {
        // Determine direction based on context
        let direction: "ASC" | "DESC" = "DESC";
        if (
          lowerDescription.includes("lowest") ||
          lowerDescription.includes("smallest") ||
          lowerDescription.includes("shortest") ||
          lowerDescription.includes("fastest") ||
          lowerDescription.includes("bottom") ||
          lowerDescription.includes("asc")
        ) {
          direction = "ASC";
        }
        return `ORDER BY ${field} ${direction}`;
      }
    }

    return "";
  }

  /**
   * Extract existing ORDER BY clause from the description if present.
   */
  private extractExistingOrderByClause(description: string): string | null {
    // Pattern to match ORDER BY clauses: ORDER BY [field] [ASC|DESC]
    // Match patterns like: ORDER BY value DESC, ORDER BY latency ASC, ORDER BY DESC, etc.
    const orderByPatterns = [
      // ORDER BY field DESC/ASC (check this first to avoid matching field names as direction)
      /\bORDER\s+BY\s+(\w+)\s+(DESC|ASC|DESCENDING|ASCENDING)\b/i,
      // ORDER BY DESC/ASC (for TOPN queries - preserve as-is without field name)
      /\bORDER\s+BY\s+(DESC|ASC|DESCENDING|ASCENDING)\b/i,
    ];

    for (const pattern of orderByPatterns) {
      const match = description.match(pattern);
      if (match) {
        if (match[2]) {
          // Has field name (first pattern matched)
          const field = match[1];
          const direction = match[2].toUpperCase().startsWith("DESC")
            ? "DESC"
            : "ASC";
          return `ORDER BY ${field} ${direction}`;
        } else if (
          match[1] &&
          (match[1].toUpperCase() === "DESC" ||
            match[1].toUpperCase() === "ASC" ||
            match[1].toUpperCase() === "DESCENDING" ||
            match[1].toUpperCase() === "ASCENDING")
        ) {
          // Only direction (for TOPN queries) - preserve as "ORDER BY DESC" or "ORDER BY ASC"
          const direction = match[1].toUpperCase().startsWith("DESC")
            ? "DESC"
            : "ASC";
          return `ORDER BY ${direction}`;
        }
      }
    }

    return null;
  }

  /**
   * Extract existing AGGREGATE BY clause from the description if present.
   */
  private extractExistingAggregateByClause(description: string): string | null {
    // Pattern to match AGGREGATE BY clauses: AGGREGATE BY [FUNCTION]
    // Match patterns like: AGGREGATE BY SUM, AGGREGATE BY MAX, AGGREGATE BY MEAN, etc.
    const aggregateByPattern =
      /\bAGGREGATE\s+BY\s+(SUM|MEAN|COUNT|MAX|MIN|AVG)\b/i;

    const match = description.match(aggregateByPattern);
    if (match) {
      const functionName = match[1].toUpperCase();
      // Normalize AVG to MEAN (both are valid, but MEAN is the standard in BydbQL)
      const normalizedFunction = functionName === "AVG" ? "MEAN" : functionName;
      return `AGGREGATE BY ${normalizedFunction}`;
    }

    return null;
  }

  /**
   * Build an AGGREGATE BY clause from the description.
   */
  private buildAggregateByClause(description: string): string {
    // First, check if there's already an AGGREGATE BY clause in the input
    const existingAggregateBy =
      this.extractExistingAggregateByClause(description);
    if (existingAggregateBy) {
      return existingAggregateBy;
    }

    const lowerDescription = description.toLowerCase();

    // Check for explicit "aggregate by" patterns first (highest priority)
    const explicitMatch = description.match(
      /\baggregate\s+by\s+(sum|mean|count|max|min|avg)\b/i,
    );
    if (explicitMatch && explicitMatch[1]) {
      const functionName = explicitMatch[1].toUpperCase();
      const normalizedFunction = functionName === "AVG" ? "MEAN" : functionName;
      return `AGGREGATE BY ${normalizedFunction}`;
    }

    // Natural language patterns for aggregation
    const aggregatePatterns = [
      { pattern: /\b(sum|total|totals|summing)\b/i, func: "SUM" },
      { pattern: /\b(max|maximum|maximize|highest\s+value)\b/i, func: "MAX" },
      { pattern: /\b(min|minimum|minimize|lowest\s+value)\b/i, func: "MIN" },
      { pattern: /\b(mean|average|avg|averaging)\b/i, func: "MEAN" },
      { pattern: /\b(count|counting|number\s+of)\b/i, func: "COUNT" },
    ];

    // Check natural language patterns
    for (const { pattern, func } of aggregatePatterns) {
      if (pattern.test(lowerDescription)) {
        return `AGGREGATE BY ${func}`;
      }
    }

    return "";
  }
}
