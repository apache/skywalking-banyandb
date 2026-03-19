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

import { createServer } from 'node:http';

import dotenv from 'dotenv';
import { z } from 'zod';

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { CallToolRequestSchema, ListToolsRequestSchema } from '@modelcontextprotocol/sdk/types.js';
import { BanyanDBClient, ResourceMetadata } from './client/index.js';
import { generateBydbQL } from './query/llm-prompt.js';
import { ResourcesByGroup } from './query/types.js';
import { log, setupGlobalErrorHandlers } from './utils/logger.js';

// Load environment variables first
dotenv.config();

// Set up global error handlers early to catch all errors
setupGlobalErrorHandlers();

const BANYANDB_ADDRESS = process.env.BANYANDB_ADDRESS || 'localhost:17900';
const TRANSPORT = process.env.TRANSPORT || 'stdio';
const mcpPortRaw = parseInt(process.env.MCP_PORT || '3000', 10);
if (!Number.isFinite(mcpPortRaw) || mcpPortRaw <= 0 || mcpPortRaw > 65535) {
  log.error(`Invalid MCP_PORT value "${process.env.MCP_PORT}": must be an integer between 1 and 65535. Defaulting to 3000.`);
}
const MCP_PORT = Number.isFinite(mcpPortRaw) && mcpPortRaw > 0 && mcpPortRaw <= 65535 ? mcpPortRaw : 3000;

// Prompt schema for generate_BydbQL — used with registerPrompt which requires a type cast
// due to MCP SDK 1.x Zod v3/v4 compatibility layer causing TS2589 deep type instantiation.
const generateBydbQLPromptSchema = {
  description: z.string().describe(
    "Natural language description of the query (e.g., 'list the last 30 minutes service_cpm_minute', 'show the last 30 zipkin spans order by time')",
  ),
  resource_type: z.enum(['stream', 'measure', 'trace', 'property']).optional().describe('Optional resource type hint: stream, measure, trace, or property'),
  resource_name: z.string().optional().describe('Optional resource name hint (stream/measure/trace/property name)'),
  group: z.string().optional().describe('Optional group hint, for example the properties group'),
} as const;

type QueryHints = {
  description?: string;
  BydbQL?: string;
  resource_type?: string;
  resource_name?: string;
  group?: string;
};

function normalizeQueryHints(args: unknown): QueryHints {
  if (!args || typeof args !== 'object') {
    return {};
  }

  const rawArgs = args as Record<string, unknown>;
  return {
    description: typeof rawArgs.description === 'string' ? rawArgs.description.trim() : undefined,
    BydbQL: typeof rawArgs.BydbQL === 'string' ? rawArgs.BydbQL.trim() : typeof rawArgs.bydbql === 'string' ? rawArgs.bydbql.trim() : undefined,
    resource_type: typeof rawArgs.resource_type === 'string' ? rawArgs.resource_type.trim() : undefined,
    resource_name: typeof rawArgs.resource_name === 'string' ? rawArgs.resource_name.trim() : undefined,
    group: typeof rawArgs.group === 'string' ? rawArgs.group.trim() : undefined,
  };
}

async function loadQueryContext(banyandbClient: BanyanDBClient): Promise<{ groups: string[]; resourcesByGroup: ResourcesByGroup }> {
  let groups: string[] = [];
  try {
    const groupsList = await banyandbClient.listGroups();
    groups = groupsList.map((groupResource) => groupResource.metadata?.name || '').filter((groupName) => groupName !== '');
  } catch (error) {
    log.warn('Failed to fetch groups, continuing without group information:', error instanceof Error ? error.message : String(error));
  }

  const resourcesByGroup: ResourcesByGroup = {};
  for (const group of groups) {
    try {
      const [streams, measures, traces, properties, topNItems, indexRule] = await Promise.all([
        banyandbClient.listStreams(group).catch(() => []),
        banyandbClient.listMeasures(group).catch(() => []),
        banyandbClient.listTraces(group).catch(() => []),
        banyandbClient.listProperties(group).catch(() => []),
        banyandbClient.listTopN(group).catch(() => []),
        banyandbClient.listIndexRule(group).catch(() => []),
      ]);

      resourcesByGroup[group] = {
        streams: streams.map((resource) => resource.metadata?.name || '').filter((resourceName) => resourceName !== ''),
        measures: measures.map((resource) => resource.metadata?.name || '').filter((resourceName) => resourceName !== ''),
        traces: traces.map((resource) => resource.metadata?.name || '').filter((resourceName) => resourceName !== ''),
        properties: properties.map((resource) => resource.metadata?.name || '').filter((resourceName) => resourceName !== ''),
        topNItems: topNItems.map((resource) => resource.metadata?.name || '').filter((resourceName) => resourceName !== ''),
        indexRule: indexRule.filter((resource) => !resource.noSort && resource.metadata?.name).map((resource) => resource.metadata?.name || ''),
      };
    } catch (error) {
      log.warn(`Failed to fetch resources for group "${group}", continuing:`, error instanceof Error ? error.message : String(error));
      resourcesByGroup[group] = { streams: [], measures: [], traces: [], properties: [], topNItems: [], indexRule: [] };
    }
  }

  return { groups, resourcesByGroup };
}

function createMcpServer(banyandbClient: BanyanDBClient): McpServer {
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
    config: {
      description: string;
      argsSchema: unknown;
    },
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
      const description = args.description.trim();
      if (!description) {
        throw new Error('description is required');
      }

      const { groups, resourcesByGroup } = await loadQueryContext(banyandbClient);
      const prompt = generateBydbQL(description, args, groups, resourcesByGroup);

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

  // List available tools
  server.server.setRequestHandler(ListToolsRequestSchema, async () => {
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
  });

  // Handle tool calls
  server.server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;

    if (name === 'list_groups_schemas') {
      const resourceType = (args as Record<string, unknown>)?.resource_type as string | undefined;
      if (!resourceType) {
        throw new Error('resource_type is required and must be one of: groups, streams, measures, traces, properties');
      }

      let result: string;

      if (resourceType === 'groups') {
        const groups = await banyandbClient.listGroups();
        const groupNames = groups.map((g) => g.metadata?.name || '').filter((n) => n !== '');
        result = `Available Groups (${groupNames.length}):\n${groupNames.join('\n')}`;
        if (groupNames.length === 0) {
          result += '\n\nNo groups found. BanyanDB may be empty or not configured.';
        }
      } else {
        const group = (args as Record<string, unknown>)?.group as string | undefined;
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
          throw new Error(`Invalid resource_type "${resourceType}". Must be one of: groups, streams, measures, traces, properties`);
        }

        const resources = await fetcher();
        const resourceNames = resources.map((r) => r.metadata?.name || '').filter((n) => n !== '');
        const resourceLabel = resourceType.charAt(0).toUpperCase() + resourceType.slice(1);

        result = `Available ${resourceLabel} in group "${group}" (${resourceNames.length}):\n${resourceNames.join('\n')}`;
        if (resourceNames.length === 0) {
          result += `\n\nNo ${resourceType} found in group "${group}".`;
        }
      }

      return { content: [{ type: 'text', text: result }] };
    }

    if (name === 'list_resources_bydbql') {
      const queryHints = normalizeQueryHints(args);
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
        const text = `=== Query Result ===\n\n${result}\n\n=== BydbQL Query ===\n${queryHints.BydbQL}${debugInfo}`;

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
          if (error.message.includes('not found') || error.message.includes('does not exist') || error.message.includes('Empty response')) {
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

    if (name === 'get_generate_bydbql_prompt') {
      const queryHints = normalizeQueryHints(args);
      if (!queryHints.description) {
        throw new Error('description is required');
      }

      const { groups, resourcesByGroup } = await loadQueryContext(banyandbClient);
      const prompt = generateBydbQL(queryHints.description, queryHints, groups, resourcesByGroup);

      return { content: [{ type: 'text', text: prompt }] };
    }

    throw new Error(`Unknown tool: ${name}`);
  });

  return server;
}

async function main() {
  const banyandbClient = new BanyanDBClient(BANYANDB_ADDRESS);

  log.info('Using built-in BydbQL prompt generation with natural language support and BanyanDB schema hints. Use the list_groups_schemas tool to discover available resources and guide your queries.');

  if (TRANSPORT === 'http') {
    const httpServer = createServer((req, res) => {
      const { pathname } = new URL(req.url ?? '', 'http://localhost');
      if (pathname !== '/mcp') {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Not found' }));
        return;
      }

      let body = '';
      req.on('data', (chunk) => { body += chunk; });
      req.on('end', async () => {
        let parsedBody: unknown;
        if (body) {
          try {
            parsedBody = JSON.parse(body);
          } catch {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Invalid JSON in request body' }));
            return;
          }
        }
        try {
          const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
          const mcpServer = createMcpServer(banyandbClient);
          await mcpServer.connect(transport);
          await transport.handleRequest(req, res, parsedBody);
        } catch (error) {
          if (!res.headersSent) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: error instanceof Error ? error.message : String(error) }));
          }
        }
      });
    });

    httpServer.listen(MCP_PORT, () => {
      log.info(`BanyanDB MCP HTTP server listening on :${MCP_PORT}/mcp`);
      log.info(`Connecting to BanyanDB at ${BANYANDB_ADDRESS}`);
    });
  } else {
    const mcpServer = createMcpServer(banyandbClient);
    const transport = new StdioServerTransport();
    await mcpServer.connect(transport);

    log.info('BanyanDB MCP server started');
    log.info(`Connecting to BanyanDB at ${BANYANDB_ADDRESS}`);
  }
}

main().catch((error) => {
  log.error('Fatal error:', error instanceof Error ? error.message : String(error));
  if (error instanceof Error && error.stack) {
    log.error('Stack trace:', error.stack);
  }
  process.exit(1);
});
