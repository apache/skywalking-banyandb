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

import { createServer, IncomingMessage } from 'node:http';

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
const MCP_HOST = process.env.MCP_HOST || '127.0.0.1';
const MCP_AUTH_TOKEN = process.env.MCP_AUTH_TOKEN?.trim() || '';
const mcpPortRaw = parseInt(process.env.MCP_PORT || '3000', 10);
if (!Number.isFinite(mcpPortRaw) || mcpPortRaw <= 0 || mcpPortRaw > 65535) {
  throw new Error(`Invalid MCP_PORT value "${process.env.MCP_PORT}": must be an integer between 1 and 65535.`);
}
const MCP_PORT = mcpPortRaw;
const mcpMaxBodyBytesRaw = parseInt(process.env.MCP_MAX_BODY_BYTES || `${1024 * 1024}`, 10);
if (!Number.isFinite(mcpMaxBodyBytesRaw) || mcpMaxBodyBytesRaw <= 0) {
  throw new Error(`Invalid MCP_MAX_BODY_BYTES value "${process.env.MCP_MAX_BODY_BYTES}": must be a positive integer.`);
}
const MCP_MAX_BODY_BYTES = mcpMaxBodyBytesRaw;
const maxErrorMessageLength = 1024;
const maxToolResponseLength = 64 * 1024;
const mcpRateLimitWindowMsRaw = parseInt(process.env.MCP_RATE_LIMIT_WINDOW_MS || '60000', 10);
if (!Number.isFinite(mcpRateLimitWindowMsRaw) || mcpRateLimitWindowMsRaw <= 0) {
  throw new Error(`Invalid MCP_RATE_LIMIT_WINDOW_MS value "${process.env.MCP_RATE_LIMIT_WINDOW_MS}": must be a positive integer.`);
}
const MCP_RATE_LIMIT_WINDOW_MS = mcpRateLimitWindowMsRaw;
const mcpRateLimitMaxRequestsRaw = parseInt(process.env.MCP_RATE_LIMIT_MAX_REQUESTS || '60', 10);
if (!Number.isFinite(mcpRateLimitMaxRequestsRaw) || mcpRateLimitMaxRequestsRaw <= 0) {
  throw new Error(`Invalid MCP_RATE_LIMIT_MAX_REQUESTS value "${process.env.MCP_RATE_LIMIT_MAX_REQUESTS}": must be a positive integer.`);
}
const MCP_RATE_LIMIT_MAX_REQUESTS = mcpRateLimitMaxRequestsRaw;
const maxDescriptionLength = 2048;
const maxIdentifierLength = 256;
const maxBydbQLLength = 4096;
const identifierPattern = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/;
const allowedResourceTypes = ['groups', 'streams', 'measures', 'traces', 'properties'] as const;
const allowedQueryHintResourceTypes = ['stream', 'measure', 'trace', 'property'] as const;
const disallowedQueryTokenPatterns = [/[;]/, /--/, /\/\*/, /\*\//];
const controlCharacterPattern = /[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/;
const allowedQueryPrefixPatterns = [/^\s*SELECT\b/i, /^\s*SHOW\s+TOP\b/i];

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

type RateLimitEntry = {
  count: number;
  windowStart: number;
};

const rateLimitByClient = new Map<string, RateLimitEntry>();

function truncateText(text: string, maxLength: number): string {
  if (text.length <= maxLength) {
    return text;
  }

  return `${text.slice(0, maxLength)}\n... [truncated ${text.length - maxLength} characters]`;
}

function getAuthorizationToken(req: IncomingMessage): string {
  const authHeader = req.headers.authorization;
  if (!authHeader) {
    return '';
  }

  const bearerPrefix = 'Bearer ';
  return authHeader.startsWith(bearerPrefix) ? authHeader.slice(bearerPrefix.length).trim() : '';
}

function isLoopbackHost(host: string): boolean {
  return host === '127.0.0.1' || host === 'localhost' || host === '::1';
}

function getClientIdentifier(req: IncomingMessage): string {
  const forwardedFor = req.headers['x-forwarded-for'];
  if (typeof forwardedFor === 'string' && forwardedFor.trim()) {
    return forwardedFor.split(',')[0].trim();
  }

  return req.socket.remoteAddress || 'unknown';
}

function isRateLimited(clientId: string, now: number): boolean {
  const currentEntry = rateLimitByClient.get(clientId);
  if (!currentEntry || now - currentEntry.windowStart >= MCP_RATE_LIMIT_WINDOW_MS) {
    rateLimitByClient.set(clientId, { count: 1, windowStart: now });
    return false;
  }

  if (currentEntry.count >= MCP_RATE_LIMIT_MAX_REQUESTS) {
    return true;
  }

  currentEntry.count += 1;
  return false;
}

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

function validateTextInput(fieldName: string, rawValue: string, maxLength: number): string {
  const value = rawValue.trim();
  if (!value) {
    throw new Error(`${fieldName} cannot be empty`);
  }
  if (value.length > maxLength) {
    throw new Error(`${fieldName} exceeds the maximum length of ${maxLength} characters`);
  }
  if (controlCharacterPattern.test(value)) {
    throw new Error(`${fieldName} contains unsupported control characters`);
  }

  return value;
}

function validateIdentifier(fieldName: string, rawValue: string): string {
  const value = validateTextInput(fieldName, rawValue, maxIdentifierLength);
  if (!identifierPattern.test(value)) {
    throw new Error(`${fieldName} contains unsupported characters`);
  }

  return value;
}

function validateResourceType(rawValue: string): (typeof allowedResourceTypes)[number] {
  const value = validateTextInput('resource_type', rawValue, 32).toLowerCase();
  if (!allowedResourceTypes.includes(value as (typeof allowedResourceTypes)[number])) {
    throw new Error('resource_type must be one of: groups, streams, measures, traces, properties');
  }

  return value as (typeof allowedResourceTypes)[number];
}

function validateQueryHintResourceType(rawValue: string): (typeof allowedQueryHintResourceTypes)[number] {
  const value = validateTextInput('resource_type', rawValue, 32).toLowerCase();
  if (!allowedQueryHintResourceTypes.includes(value as (typeof allowedQueryHintResourceTypes)[number])) {
    throw new Error('resource_type must be one of: stream, measure, trace, property');
  }

  return value as (typeof allowedQueryHintResourceTypes)[number];
}

function validateBydbQL(rawValue: string): string {
  const value = validateTextInput('BydbQL', rawValue, maxBydbQLLength);
  if (!allowedQueryPrefixPatterns.some((pattern) => pattern.test(value))) {
    throw new Error('BydbQL must be a read-only SELECT or SHOW TOP query');
  }
  for (const disallowedPattern of disallowedQueryTokenPatterns) {
    if (disallowedPattern.test(value)) {
      throw new Error('BydbQL contains unsupported multi-statement or comment syntax');
    }
  }

  return value;
}

function validateListGroupsArgs(args: unknown): { resourceType: (typeof allowedResourceTypes)[number]; group?: string } {
  if (!args || typeof args !== 'object') {
    throw new Error('tool arguments must be an object');
  }

  const rawArgs = args as Record<string, unknown>;
  const rawResourceType = rawArgs.resource_type;
  if (typeof rawResourceType !== 'string') {
    throw new Error('resource_type is required and must be one of: groups, streams, measures, traces, properties');
  }

  const resourceType = validateResourceType(rawResourceType);
  if (resourceType === 'groups') {
    return { resourceType };
  }

  const rawGroup = rawArgs.group;
  if (typeof rawGroup !== 'string') {
    throw new Error(`group is required for listing ${resourceType}`);
  }

  return {
    resourceType,
    group: validateIdentifier('group', rawGroup),
  };
}

function validateQueryHints(queryHints: QueryHints): QueryHints {
  const validatedHints: QueryHints = {};

  if (queryHints.description) {
    validatedHints.description = validateTextInput('description', queryHints.description, maxDescriptionLength);
  }
  if (queryHints.BydbQL) {
    validatedHints.BydbQL = validateBydbQL(queryHints.BydbQL);
  }
  if (queryHints.resource_type) {
    validatedHints.resource_type = validateQueryHintResourceType(queryHints.resource_type);
  }
  if (queryHints.resource_name) {
    validatedHints.resource_name = validateIdentifier('resource_name', queryHints.resource_name);
  }
  if (queryHints.group) {
    validatedHints.group = validateIdentifier('group', queryHints.group);
  }

  return validatedHints;
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
      const { resourceType, group } = validateListGroupsArgs(args);

      let result: string;

      if (resourceType === 'groups') {
        const groups = await banyandbClient.listGroups();
        const groupNames = groups.map((g) => g.metadata?.name || '').filter((n) => n !== '');
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
        const text = truncateText(`=== Query Result ===\n\n${result}\n\n=== BydbQL Query ===\n${queryHints.BydbQL}${debugInfo}`, maxToolResponseLength);

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
      const queryHints = validateQueryHints(normalizeQueryHints(args));
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
    if (!isLoopbackHost(MCP_HOST) && !MCP_AUTH_TOKEN) {
      throw new Error('MCP_AUTH_TOKEN is required when MCP_HOST is not a loopback address.');
    }

    const httpServer = createServer((req, res) => {
      const { pathname } = new URL(req.url ?? '', 'http://localhost');
      if (pathname !== '/mcp') {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Not found' }));
        return;
      }

      if (req.method !== 'POST') {
        res.writeHead(405, {
          'Content-Type': 'application/json',
          Allow: 'POST',
        });
        res.end(JSON.stringify({ error: 'Method not allowed' }));
        return;
      }

      if (MCP_AUTH_TOKEN && getAuthorizationToken(req) !== MCP_AUTH_TOKEN) {
        res.writeHead(401, {
          'Content-Type': 'application/json',
          'WWW-Authenticate': 'Bearer',
        });
        res.end(JSON.stringify({ error: 'Unauthorized' }));
        return;
      }

      const clientId = getClientIdentifier(req);
      if (isRateLimited(clientId, Date.now())) {
        res.writeHead(429, {
          'Content-Type': 'application/json',
          'Retry-After': `${Math.ceil(MCP_RATE_LIMIT_WINDOW_MS / 1000)}`,
        });
        res.end(JSON.stringify({ error: 'Too many requests' }));
        return;
      }

      let body = '';
      let requestTooLarge = false;
      req.on('data', (chunk: Buffer) => {
        body += chunk.toString();
        if (body.length > MCP_MAX_BODY_BYTES) {
          requestTooLarge = true;
          res.writeHead(413, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: `Request body too large. Limit is ${MCP_MAX_BODY_BYTES} bytes.` }));
          req.destroy();
        }
      });
      req.on('end', async () => {
        if (requestTooLarge) {
          return;
        }

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
            res.end(JSON.stringify({ error: truncateText(error instanceof Error ? error.message : String(error), maxErrorMessageLength) }));
          }
        }
      });
    });

    httpServer.listen(MCP_PORT, MCP_HOST, () => {
      log.info(`BanyanDB MCP HTTP server listening on ${MCP_HOST}:${MCP_PORT}/mcp`);
      log.info(`Connecting to BanyanDB at ${BANYANDB_ADDRESS}`);
      log.info(`HTTP auth ${MCP_AUTH_TOKEN ? 'enabled' : 'disabled'}`);
      log.info(`HTTP rate limit ${MCP_RATE_LIMIT_MAX_REQUESTS} requests per ${MCP_RATE_LIMIT_WINDOW_MS}ms`);
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
