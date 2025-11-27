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

import OpenAI from 'openai';
import { generateQueryPrompt } from './llm-prompt.js';
import { PatternMatcher } from './pattern-matcher.js';
import type { QueryGeneratorResult, ResourcesByGroup } from './types.js';

/**
 * QueryGenerator converts natural language descriptions to BydbQL queries.
 * Supports both LLM-based generation (when API key is provided) and pattern-based fallback.
 */
export class QueryGenerator {
  private static readonly OPENAI_API_TIMEOUT_MS = 20000; // 20 seconds timeout for LLM API calls

  private openaiClient: OpenAI | null = null;
  private patternMatcher: PatternMatcher;

  constructor(apiKey?: string, baseURL?: string) {
    // Validate API key format before creating client
    if (apiKey && apiKey.trim().length > 0) {
      const trimmedKey = apiKey.trim();
      if (trimmedKey.length < 10) {
        console.error('[QueryGenerator] Warning: API key appears to be too short. LLM query generation may fail.');
      }
      this.openaiClient = new OpenAI({
        apiKey: trimmedKey,
        ...(baseURL && { baseURL }),
      });
    }
    this.patternMatcher = new PatternMatcher();
  }

  /**
   * Generate a BydbQL query from a natural language description.
   */
  async generateQuery(
    description: string,
    args: Record<string, unknown>,
    groups: string[] = [],
    resourcesByGroup: ResourcesByGroup = {},
  ): Promise<QueryGeneratorResult> {
    // Use LLM if available, otherwise fall back to pattern matching
    if (this.openaiClient) {
      try {
        return await this.generateQueryWithLLM(description, args, groups, resourcesByGroup);
      } catch (error: unknown) {
        // Check for API key authentication errors
        const errorObj = error as { status?: number; message?: string };
        if (
          errorObj?.status === 401 ||
          errorObj?.message?.includes('401') ||
          errorObj?.message?.includes('Invalid API key')
        ) {
          console.error('[QueryGenerator] API key authentication failed. Falling back to pattern-based generation.');
          console.error('[QueryGenerator] Error details:', errorObj.message || String(error));
          // Disable LLM client to prevent repeated failures
          this.openaiClient = null;
        } else {
          // For other errors (timeout, network, etc.), log but don't disable
          console.error('[QueryGenerator] Error generating query with LLM:', errorObj.message || String(error));
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
    args: Record<string, unknown>,
    groups: string[] = [],
    resourcesByGroup: ResourcesByGroup = {},
  ): Promise<QueryGeneratorResult> {
    if (!this.openaiClient) {
      throw new Error('OpenAI client not initialized');
    }
    const prompt = generateQueryPrompt(description, args, groups, resourcesByGroup);

    const completion = await Promise.race([
      this.openaiClient.chat.completions.create({
        model: 'gpt-4o-mini',
        messages: [
          {
            role: 'system',
            content:
              'You are a BydbQL query generator. Return a JSON object with the following fields: bydbql (the BydbQL query), group (group name), name (resource name), type (resource type), and explanations (brief explanation of the query).',
          },
          {
            role: 'user',
            content: prompt,
          },
        ],
        response_format: { type: 'json_object' },
      }),
      new Promise<never>((_, reject) =>
        setTimeout(
          () => reject(new Error(`LLM API timeout after ${QueryGenerator.OPENAI_API_TIMEOUT_MS / 1000} seconds`)),
          QueryGenerator.OPENAI_API_TIMEOUT_MS,
        ),
      ),
    ]);

    const responseContent = completion.choices[0]?.message?.content?.trim();
    if (!responseContent) {
      throw new Error('Empty response from LLM');
    }

    // Parse JSON response
    let parsedResponse: {
      bydbql?: string;
      group?: string;
      name?: string;
      type?: string;
      explanations?: string;
    };

    try {
      parsedResponse = JSON.parse(responseContent);
    } catch (error) {
      console.error('JSON parsing failed:', error);
      // Fallback: try to extract query from plain text if JSON parsing fails
      const cleanedQuery = responseContent
        .replace(/^```(?:bydbql|sql|json)?\n?/i, '')
        .replace(/\n?```$/i, '')
        .trim();
      const result: QueryGeneratorResult = {
        description,
        query: cleanedQuery,
      };
      const resourceType = (args.resource_type as string) || undefined;
      const resourceName = (args.resource_name as string) || undefined;
      const group = (args.group as string) || undefined;
      if (resourceType) result.resourceType = resourceType;
      if (resourceName) result.resourceName = resourceName;
      if (group) result.group = group;
      return result;
    }

    const query = parsedResponse.bydbql?.trim() || '';
    const group = (args.group as string) || parsedResponse.group?.trim() || undefined;
    const resourceName = (args.resource_name as string) || parsedResponse.name?.trim() || undefined;
    const resourceType = (args.resource_type as string) || parsedResponse.type?.toLowerCase().trim() || undefined;
    const explanations = parsedResponse.explanations?.trim() || undefined;

    // Clean up the query (remove markdown code blocks if present)
    const cleanedQuery = query
      .replace(/^```(?:bydbql|sql)?\n?/i, '')
      .replace(/\n?```$/i, '')
      .trim();

    // Return parameters used for query generation - only include fields that are present
    const result: QueryGeneratorResult = {
      description,
      query: cleanedQuery,
    };

    if (resourceType) {
      result.resourceType = resourceType;
    }
    if (resourceName) {
      result.resourceName = resourceName;
    }
    if (group) {
      result.group = group;
    }
    if (explanations) {
      result.explanations = explanations;
    }

    return result;
  }

  /**
   * Generate query using pattern matching (fallback method).
   */
  private generateQueryWithPatterns(description: string, args: Record<string, unknown>): QueryGeneratorResult {
    // Determine resource type
    const resourceType = this.patternMatcher.detectResourceType(description, args) || 'stream';

    // Extract resource name if provided
    let resourceName =
      (typeof args.resource_name === 'string' ? args.resource_name : null) ||
      this.patternMatcher.detectResourceName(description);
    if (!resourceName) {
      // Use common defaults
      switch (resourceType) {
        case 'stream':
          resourceName = 'sw';
          break;
        case 'measure':
          resourceName = 'service_cpm';
          break;
        case 'trace':
          resourceName = 'sw';
          break;
        case 'property':
          resourceName = 'sw';
          break;
      }
    }

    // Extract group
    const group =
      (typeof args.group === 'string' ? args.group : null) ||
      this.patternMatcher.detectGroup(description) ||
      'default';

    // Build time clause
    const timeClause = this.patternMatcher.buildTimeClause(description);

    // Build ORDER BY clause
    let orderByClause = this.patternMatcher.buildOrderByClause(description);

    // Build AGGREGATE BY clause
    const aggregateByClause = this.patternMatcher.buildAggregateByClause(description);

    // Build LIMIT clause (must come after ORDER BY)
    const limitClause = this.patternMatcher.buildLimitClause(description);

    // If LIMIT is present but ORDER BY is not, add default ORDER BY for meaningful results
    // "last N" typically means "most recent N", so order by time DESC
    if (limitClause && !orderByClause) {
      // Determine appropriate time field based on resource type
      let timeField = 'timestamp_millis'; // default for TRACE
      if (resourceType === 'stream') {
        timeField = 'timestamp';
      } else if (resourceType === 'measure') {
        timeField = 'timestamp';
      }

      // Check if description says "first N" (ascending) vs "last N" (descending)
      const lowerDescription = description.toLowerCase();
      if (lowerDescription.includes('first')) {
        orderByClause = `ORDER BY ${timeField} ASC`;
      } else {
        // Default to DESC for "last N" patterns
        orderByClause = `ORDER BY ${timeField} DESC`;
      }
    }

    // Build SELECT clause
    const selectClause = this.patternMatcher.buildSelectClause(description, resourceType);

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
    if (limitClause) {
      query += ` ${limitClause}`;
    }

    const result: QueryGeneratorResult = {
      description,
      query,
    };

    if (resourceType) {
      result.resourceType = resourceType;
    }
    if (resourceName) {
      result.resourceName = resourceName;
    }
    if (group) {
      result.group = group;
    }

    return result;
  }
}

// Re-export types for convenience
export type { QueryGeneratorResult, ResourcesByGroup } from './types.js';

