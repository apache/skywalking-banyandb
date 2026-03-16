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
import { BanyanDBClient, ResourceMetadata } from './client/banyandb-client.js';
import { generateBydbQL } from './query/llm-prompt.js';
import { QueryGeneratorResult, ResourcesByGroup } from './query/types.js';
import { log, setupGlobalErrorHandlers } from './utils/logger.js';

// Load environment variables first
dotenv.config();

// Set up global error handlers early to catch all errors
setupGlobalErrorHandlers();

const BANYANDB_ADDRESS = process.env.BANYANDB_ADDRESS || 'localhost:17900';
const TRANSPORT = process.env.TRANSPORT || 'stdio';
const MCP_PORT = parseInt(process.env.MCP_PORT || '3000', 10);

const generateBydbQLPromptSchema = {
  description: z.string().describe(
    "Natural language description of the query (e.g., 'list the last 30 minutes service_cpm_minute', 'show the last 30 zipkin spans order by time')",
  ),
  resource_type: z.enum(['stream', 'measure', 'trace', 'property']).optional().describe('Optional resource type hint: stream, measure, trace, or property'),
  resource_name: z.string().optional().describe('Optional resource name hint (stream/measure/trace/property name)'),
  group: z.string().optional().describe('Optional group hint, for example the properties group'),
} as const;

type GenerateBydbQLPromptArgs = {
  description: string;
  resource_type?: 'stream' | 'measure' | 'trace' | 'property';
  resource_name?: string;
  group?: string;
};

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
    BydbQL: typeof rawArgs.BydbQL === 'string' ? rawArgs.BydbQL.trim() : undefined,
    resource_type: typeof rawArgs.resource_type === 'string' ? rawArgs.resource_type.trim() : undefined,
    resource_name: typeof rawArgs.resource_name === 'string' ? rawArgs.resource_name.trim() : undefined,
    group: typeof rawArgs.group === 'string' ? rawArgs.group.trim() : undefined,
  };
}

function buildQueryResult(args: QueryHints): QueryGeneratorResult {
  if (args.BydbQL) {
    return {
      description: 'Execute provided BydbQL query',
      query: args.BydbQL,
      resourceType: args.resource_type,
      resourceName: args.resource_name,
      group: args.group,
    };
  }

  throw new Error('BydbQL is required');
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
    cb: (args: GenerateBydbQLPromptArgs) => Promise<{
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
    'generateBydbQL',
    {
      description:
        'Generate the prompt/context needed to derive correct BydbQL from natural language and BanyanDB schema hints. Use list_groups_schemas first to discover available resources.',
      argsSchema: generateBydbQLPromptSchema,
    },
    async (args) => {
      const queryHints = normalizeQueryHints(args);
      if (!queryHints.description) {
        throw new Error('description is required');
      }

      const { groups, resourcesByGroup } = await loadQueryContext(banyandbClient);
      const prompt = generateBydbQL(queryHints.description, queryHints, groups, resourcesByGroup);

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
          description:
            'Fetch streams, measures, traces, or properties from BanyanDB using a BydbQL query.',
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
      ],
    };
  });

  // Handle tool calls
  server.server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;

    if (name === 'list_groups_schemas') {
      if (!args || typeof args !== 'object') {
        throw new Error('Invalid arguments: arguments object is required');
      }

      const resourceType = args.resource_type as string | undefined;
      const validResourceTypes = ['groups', 'streams', 'measures', 'traces', 'properties'];

      if (!resourceType || typeof resourceType !== 'string') {
        throw new Error(`resource_type is required and must be one of: ${validResourceTypes.join(', ')}`);
      }

      if (!validResourceTypes.includes(resourceType)) {
        throw new Error(`Invalid resource_type "${resourceType}". Must be one of: ${validResourceTypes.join(', ')}`);
      }

      try {
        let result: string;

        if (resourceType === 'groups') {
          const groups = await banyandbClient.listGroups();
          const groupNames = groups.map((g) => g.metadata?.name || 'unknown').filter((n) => n !== 'unknown');
          result = `Available Groups (${groupNames.length}):\n${groupNames.join('\n')}`;
          if (groupNames.length === 0) {
            result += '\n\nNo groups found. BanyanDB may be empty or not configured.';
          }
        } else {
          const group = args?.group as string;
          if (!group) {
            throw new Error(`group is required for listing ${resourceType}`);
          }

          let resources: ResourceMetadata[] = [];
          let resourceName = '';

          switch (resourceType) {
            case 'streams':
              resources = await banyandbClient.listStreams(group);
              resourceName = 'Streams';
              break;
            case 'measures':
              resources = await banyandbClient.listMeasures(group);
              resourceName = 'Measures';
              break;
            case 'traces':
              resources = await banyandbClient.listTraces(group);
              resourceName = 'Traces';
              break;
            case 'properties':
              resources = await banyandbClient.listProperties(group);
              resourceName = 'Properties';
              break;
            default:
              throw new Error(`Unknown resource type: ${resourceType}`);
          }

          const resourceNames = resources.map((r) => r.metadata?.name || 'unknown').filter((n) => n !== 'unknown');

          result = `Available ${resourceName} in group "${group}" (${resourceNames.length}):\n${resourceNames.join('\n')}`;
          if (resourceNames.length === 0) {
            result += `\n\nNo ${resourceType} found in group "${group}".`;
          }
        }

        return {
          content: [
            {
              type: 'text',
              text: result,
            },
          ],
        };
      } catch (error) {
        if (error instanceof Error) {
          throw error;
        }
        throw new Error(`Failed to list resources: ${String(error)}`);
      }
    }

    if (name === 'list_resources_bydbql') {
      const queryHints = normalizeQueryHints(args);
      let bydbqlQueryResult: QueryGeneratorResult;
      try {
        bydbqlQueryResult = buildQueryResult(queryHints);
      } catch (error) {
        if (error instanceof Error) {
          throw new Error(`Failed to build BydbQL query: ${error.message}`);
        }
        throw new Error(`Failed to build BydbQL query: ${String(error)}`);
      }

      try {
        const result = await banyandbClient.query(bydbqlQueryResult.query);
        const debugParts: string[] = [];

        if (bydbqlQueryResult.resourceType) {
          debugParts.push(`Resource Type: ${bydbqlQueryResult.resourceType}`);
        }
        if (bydbqlQueryResult.resourceName) {
          debugParts.push(`Resource Name: ${bydbqlQueryResult.resourceName}`);
        }
        if (bydbqlQueryResult.group) {
          debugParts.push(`Group: ${bydbqlQueryResult.group}`);
        }

        const debugInfo = debugParts.length > 0 ? `\n\n=== Debug Information ===\n${debugParts.join('\n')}` : '';
        const explanations = bydbqlQueryResult.explanations
          ? `\n\n=== Explanations ===\n${bydbqlQueryResult.explanations}`
          : '';
        const resultWithQuery = `=== Query Result ===\n\n${result}\n\n=== BydbQL Query ===\n${bydbqlQueryResult.query}${debugInfo}${explanations}`;

        return {
          content: [
            {
              type: 'text',
              text: resultWithQuery,
            },
          ],
        };
      } catch (error) {
        if (error instanceof Error) {
          // Check if it's a timeout error
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
          // Check if it's a resource not found error
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

    throw new Error(`Unknown tool: ${name}`);
  });

  return server;
}

async function main() {
  const banyandbClient = new BanyanDBClient(BANYANDB_ADDRESS);

  log.info('Using built-in BydbQL prompt generation without external LLM connectivity');

  if (TRANSPORT === 'http') {
    const httpServer = createServer((req, res) => {
      if (req.url !== '/mcp') {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Not found' }));
        return;
      }

      let body = '';
      req.on('data', (chunk) => { body += chunk; });
      req.on('end', async () => {
        try {
          const parsedBody = body ? JSON.parse(body) : undefined;
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
