/**
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. Apache Software
 * Foundation (ASF) licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { z } from 'zod';

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { CallToolRequestSchema, ListToolsRequestSchema } from '@modelcontextprotocol/sdk/types.js';
import { BanyanDBClient, ResourceMetadata } from '../client/index.js';
import { MAX_TOOL_RESPONSE_LENGTH } from '../config.js';
import { loadQueryContext } from '../query/context.js';
import { generateBydbQL } from '../query/llm-prompt.js';
import { normalizeQueryHints, validateListGroupsArgs, validateQueryHints } from '../query/validation.js';

export function truncateText(text: string, maxLength: number): string {
  if (text.length <= maxLength) {
    return text;
  }

  return `${text.slice(0, maxLength)}\n... [truncated ${text.length - maxLength} characters]`;
}

const generateBydbQLPromptSchema = {
  description: z
    .string()
    .describe(
      "Natural language description of the query (e.g., 'list the last 30 minutes service_cpm_minute', 'show the last 30 zipkin spans order by time')",
    ),
  resource_type: z
    .enum(['stream', 'measure', 'trace', 'property'])
    .optional()
    .describe('Optional resource type hint: stream, measure, trace, or property'),
  resource_name: z.string().optional().describe('Optional resource name hint (stream/measure/trace/property name)'),
  group: z.string().optional().describe('Optional group hint, for example the properties group'),
} as const;

function buildListToolsResponse() {
  return {
    tools: [
      {
        name: 'list_groups_schemas',
        description:
          'List available resources in BanyanDB (groups, streams, measures, traces, properties). Use this to discover what resources exist before querying.',
        inputSchema: {
          type: 'object',
          properties: {
            resource_type: {
              type: 'string',
              description: 'Type of resource to list: groups, streams, measures, traces, or properties',
              enum: ['groups', 'streams', 'measures', 'traces', 'properties'],
            },
            group: {
              type: 'string',
              description: 'Group name (required for streams, measures, traces, and properties)',
            },
          },
          required: ['resource_type'],
        },
      },
      {
        name: 'list_resources_bydbql',
        description: 'Fetch streams, measures, traces, or properties from BanyanDB using a BydbQL query.',
        inputSchema: {
          type: 'object',
          properties: {
            BydbQL: {
              type: 'string',
              description: 'BydbQL query to execute against BanyanDB.',
            },
            resource_type: {
              type: 'string',
              description: 'Optional resource type hint: stream, measure, trace, or property',
              enum: ['stream', 'measure', 'trace', 'property'],
            },
            resource_name: {
              type: 'string',
              description: 'Optional resource name hint (stream/measure/trace/property name)',
            },
            group: {
              type: 'string',
              description: 'Optional group hint, for example the properties group',
            },
          },
          required: ['BydbQL'],
        },
      },
      {
        name: 'get_generate_bydbql_prompt',
        description:
          'Return the full prompt text used by generate_BydbQL, including live BanyanDB schema hints. This lets external projects retrieve the prompt text directly.',
        inputSchema: {
          type: 'object',
          properties: {
            description: {
              type: 'string',
              description:
                "Natural language description of the query (e.g., 'list the last 30 minutes service_cpm_minute', 'show the last 30 zipkin spans order by time')",
            },
            resource_type: {
              type: 'string',
              description: 'Optional resource type hint: stream, measure, trace, or property',
              enum: ['stream', 'measure', 'trace', 'property'],
            },
            resource_name: {
              type: 'string',
              description: 'Optional resource name hint (stream/measure/trace/property name)',
            },
            group: {
              type: 'string',
              description: 'Optional group hint, for example the properties group',
            },
          },
          required: ['description'],
        },
      },
    ],
  };
}

async function handleListGroupsSchemas(banyandbClient: BanyanDBClient, args: unknown) {
  const { resourceType, group } = validateListGroupsArgs(args);

  let result: string;

  if (resourceType === 'groups') {
    const groups = await banyandbClient.listGroups();
    const groupNames = groups
      .map((groupResource) => groupResource.metadata?.name || '')
      .filter((groupName) => groupName !== '');
    result = `Available Groups (${groupNames.length}):\n${groupNames.join('\n')}`;
    if (groupNames.length === 0) {
      result += '\n\nNo groups found. BanyanDB may be empty or not configured.';
    }
  } else {
    if (!group) {
      throw new Error(`group is required for listing ${resourceType}`);
    }

    const resourceFetchers: Record<string, () => Promise<ResourceMetadata[]>> = {
      streams: () => banyandbClient.listStreams(group),
      measures: () => banyandbClient.listMeasures(group),
      traces: () => banyandbClient.listTraces(group),
      properties: () => banyandbClient.listProperties(group),
    };

    const fetcher = resourceFetchers[resourceType];
    if (!fetcher) {
      throw new Error(
        `Invalid resource_type "${resourceType}". Must be one of: groups, streams, measures, traces, properties`,
      );
    }

    const resources = await fetcher();
    const resourceNames = resources
      .map((resource) => resource.metadata?.name || '')
      .filter((resourceName) => resourceName !== '');
    const resourceLabel = resourceType.charAt(0).toUpperCase() + resourceType.slice(1);

    result = `Available ${resourceLabel} in group "${group}" (${resourceNames.length}):\n${resourceNames.join('\n')}`;
    if (resourceNames.length === 0) {
      result += `\n\nNo ${resourceType} found in group "${group}".`;
    }
  }

  return { content: [{ type: 'text', text: result }] };
}

async function handleListResourcesBydbql(banyandbClient: BanyanDBClient, args: unknown) {
  const queryHints = validateQueryHints(normalizeQueryHints(args));
  if (!queryHints.BydbQL) {
    throw new Error('BydbQL is required');
  }

  try {
    const result = await banyandbClient.query(queryHints.BydbQL);
    const debugParts: string[] = [];

    if (queryHints.resource_type) debugParts.push(`Resource Type: ${queryHints.resource_type}`);
    if (queryHints.resource_name) debugParts.push(`Resource Name: ${queryHints.resource_name}`);
    if (queryHints.group) debugParts.push(`Group: ${queryHints.group}`);

    const debugInfo = debugParts.length > 0 ? `\n\n=== Debug Information ===\n${debugParts.join('\n')}` : '';
    const text = truncateText(
      `=== Query Result ===\n\n${result}\n\n=== BydbQL Query ===\n${queryHints.BydbQL}${debugInfo}`,
      MAX_TOOL_RESPONSE_LENGTH,
    );

    return { content: [{ type: 'text', text }] };
  } catch (error) {
    if (error instanceof Error) {
      if (error.message.includes('timeout') || error.message.includes('Timeout')) {
        return {
          content: [
            {
              type: 'text',
              text:
                `Query timeout: ${error.message}\n\n` +
                `Possible causes:\n` +
                `- BanyanDB is not running or not accessible\n` +
                `- Network connectivity issues\n` +
                `- BanyanDB is overloaded or slow\n\n` +
                `Try:\n` +
                `1. Verify BanyanDB is running: curl http://localhost:17913/api/healthz\n` +
                `2. Check network connectivity\n` +
                `3. Use list_groups_schemas to verify BanyanDB is accessible`,
            },
          ],
        };
      }
      if (
        error.message.includes('not found') ||
        error.message.includes('does not exist') ||
        error.message.includes('Empty response')
      ) {
        return {
          content: [
            {
              type: 'text',
              text:
                `Query failed: ${error.message}\n\n` +
                `Tip: Use the list_groups_schemas tool to discover available resources:\n` +
                `- First list groups: list_groups_schemas with resource_type="groups"\n` +
                `- Then list streams, measures, traces, or properties for a target group\n` +
                `- Then query using a natural language description and optional resource_type, resource_name, or group hints.`,
            },
          ],
        };
      }
      throw error;
    }
    throw new Error(`Query execution failed: ${String(error)}`);
  }
}

async function handleGetGenerateBydbqlPrompt(banyandbClient: BanyanDBClient, args: unknown) {
  const queryHints = validateQueryHints(normalizeQueryHints(args));
  if (!queryHints.description) {
    throw new Error('description is required');
  }

  const { groups, resourcesByGroup } = await loadQueryContext(banyandbClient);
  const prompt = generateBydbQL(queryHints.description, queryHints, groups, resourcesByGroup);

  return { content: [{ type: 'text', text: prompt }] };
}

export function createMcpServer(banyandbClient: BanyanDBClient): McpServer {
  const server = new McpServer(
    {
      name: 'banyandb-mcp',
      version: '1.0.0',
    },
    {
      capabilities: {
        tools: {},
      },
    },
  );

  const registerPrompt = server.registerPrompt.bind(server) as unknown as (
    name: string,
    config: { description: string; argsSchema: unknown },
    cb: (args: { description: string; resource_type?: string; resource_name?: string; group?: string }) => Promise<{
      messages: Array<{
        role: 'user';
        content: {
          type: 'text';
          text: string;
        };
      }>;
    }>,
  ) => unknown;

  registerPrompt(
    'generate_BydbQL',
    {
      description:
        'Generate the prompt/context needed to derive correct BydbQL from natural language and BanyanDB schema hints. Use list_groups_schemas first to discover available resources.',
      argsSchema: generateBydbQLPromptSchema,
    },
    async (args) => {
      const validatedHints = validateQueryHints({
        description: args.description,
        resource_type: args.resource_type,
        resource_name: args.resource_name,
        group: args.group,
      });
      const description = validatedHints.description;
      if (!description) {
        throw new Error('description is required');
      }

      const { groups, resourcesByGroup } = await loadQueryContext(banyandbClient);
      const prompt = generateBydbQL(description, validatedHints, groups, resourcesByGroup);

      return {
        messages: [
          {
            role: 'user',
            content: {
              type: 'text',
              text: prompt,
            },
          },
        ],
      };
    },
  );

  server.server.setRequestHandler(ListToolsRequestSchema, async () => buildListToolsResponse());
  server.server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;

    if (name === 'list_groups_schemas') {
      return handleListGroupsSchemas(banyandbClient, args);
    }
    if (name === 'list_resources_bydbql') {
      return handleListResourcesBydbql(banyandbClient, args);
    }
    if (name === 'get_generate_bydbql_prompt') {
      return handleGetGenerateBydbqlPrompt(banyandbClient, args);
    }

    throw new Error(`Unknown tool: ${name}`);
  });

  return server;
}
