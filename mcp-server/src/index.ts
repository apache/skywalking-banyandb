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

import dotenv from "dotenv";

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { BanyanDBClient } from "./banyandb-client.js";
import { QueryGenerator } from "./query-generator.js";
import { log, setupGlobalErrorHandlers } from "./logger.js";

// Load environment variables first
dotenv.config();

// Set up global error handlers early to catch all errors
setupGlobalErrorHandlers();

const BANYANDB_ADDRESS = process.env.BANYANDB_ADDRESS || "localhost:17900";
const LLM_API_KEY = process.env.LLM_API_KEY;
const LLM_BASE_URL = process.env.LLM_BASE_URL;

async function main() {
  // Create MCP server
  const server = new McpServer(
    {
      name: "banyandb-mcp-server",
      version: "1.0.0",
    },
    {
      capabilities: {
        tools: {},
      },
    }
  );

  // Initialize BanyanDB client
  const banyandbClient = new BanyanDBClient(BANYANDB_ADDRESS);
  
  // Validate API key before creating QueryGenerator
  const validApiKey = LLM_API_KEY && LLM_API_KEY.trim().length > 0 ? LLM_API_KEY.trim() : undefined;
  const validBaseURL = LLM_BASE_URL && LLM_BASE_URL.trim().length > 0 ? LLM_BASE_URL.trim() : undefined;
  const queryGenerator = new QueryGenerator(validApiKey, validBaseURL);

  if (validApiKey) {
    log.info("LLM query generation enabled (using LLM API)");
  } else {
    log.info("LLM query generation disabled, using pattern matching");
    if (LLM_API_KEY !== undefined) {
      log.warn("LLM_API_KEY is set but appears to be empty or invalid");
    }
  }

  // List available tools
  server.server.setRequestHandler(ListToolsRequestSchema, async () => {
    return {
      tools: [
        {
          name: "list_groups_schemas",
          description:
            "List available resources in BanyanDB (groups, streams, measures, traces, properties). Use this to discover what resources exist before querying.",
          inputSchema: {
            type: "object",
            properties: {
              resource_type: {
                type: "string",
                description: "Type of resource to list: groups, streams, measures, traces, or properties",
                enum: ["groups", "streams", "measures", "traces", "properties"],
              },
              group: {
                type: "string",
                description: "Group name (required for streams, measures, traces, and properties)",
              },
            },
            required: ["resource_type"],
          },
        },
        {
          name: "list_resources_bydbql",
          description:
            "Query BanyanDB data using natural language description. Supports querying streams, measures, traces, and properties. Use list_groups_schemas first to discover available resources.",
          inputSchema: {
            type: "object",
            properties: {
              description: {
                type: "string",
                description:
                  "Natural language description of the query (e.g., 'Show me all error logs from the last hour', 'Get CPU metrics for service webapp')",
              },
              resource_type: {
                type: "string",
                description: "Optional resource type: stream, measure, trace, or property",
                enum: ["stream", "measure", "trace", "property"],
              },
              resource_name: {
                type: "string",
                description:
                  "Optional resource name (stream/measure/trace/property name)",
              },
              group: {
                type: "string",
                description: "Optional group name",
              },
            },
            required: ["description"],
          },
        },
      ],
    };
  });

  // Handle tool calls
  server.server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;

    if (name === "list_groups_schemas") {
      if (!args || typeof args !== "object") {
        throw new Error("Invalid arguments: arguments object is required");
      }
      
      const resourceType = args.resource_type as string | undefined;
      const validResourceTypes = ["groups", "streams", "measures", "traces", "properties"];
      
      if (!resourceType || typeof resourceType !== "string") {
        throw new Error(
          `resource_type is required and must be one of: ${validResourceTypes.join(", ")}`
        );
      }
      
      if (!validResourceTypes.includes(resourceType)) {
        throw new Error(
          `Invalid resource_type "${resourceType}". Must be one of: ${validResourceTypes.join(", ")}`
        );
      }

      try {
        let result: string;

        if (resourceType === "groups") {
          const groups = await banyandbClient.listGroups();
          const groupNames = groups
            .map((g) => g.metadata?.name || "unknown")
            .filter((n) => n !== "unknown");
          result = `Available Groups (${groupNames.length}):\n${groupNames.join("\n")}`;
          if (groupNames.length === 0) {
            result += "\n\nNo groups found. BanyanDB may be empty or not configured.";
          }
        } else {
          const group = args?.group as string;
          if (!group) {
            throw new Error(`group is required for listing ${resourceType}`);
          }

          let resources: any[] = [];
          let resourceName = "";

          switch (resourceType) {
            case "streams":
              resources = await banyandbClient.listStreams(group);
              resourceName = "Streams";
              break;
            case "measures":
              resources = await banyandbClient.listMeasures(group);
              resourceName = "Measures";
              break;
            case "traces":
              resources = await banyandbClient.listTraces(group);
              resourceName = "Traces";
              break;
            case "properties":
              resources = await banyandbClient.listProperties(group);
              resourceName = "Properties";
              break;
            default:
              throw new Error(`Unknown resource type: ${resourceType}`);
          }

          const resourceNames = resources
            .map((r) => r.metadata?.name || "unknown")
            .filter((n) => n !== "unknown");

          result = `Available ${resourceName} in group "${group}" (${resourceNames.length}):\n${resourceNames.join("\n")}`;
          if (resourceNames.length === 0) {
            result += `\n\nNo ${resourceType} found in group "${group}".`;
          }
        }

        return {
          content: [
            {
              type: "text",
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

    if (name === "list_resources_bydbql") {
      const description = args?.description as string;
      if (!description) {
        throw new Error("description is required");
      }

      let bydbqlQuery: string;
      try {
        // Generate BydbQL query from natural language description
        bydbqlQuery = await queryGenerator.generateQuery(description, args || {});
      } catch (error) {
        if (error instanceof Error && 
            (error.message.includes("timeout") || error.message.includes("Timeout"))) {
          return {
            content: [
              {
                type: "text",
                text:                       `Query generation timeout: ${error.message}\n\n` +
                      `The LLM query generation timed out. Falling back to pattern-based generation...\n` +
                      `If this persists, try:\n` +
                      `1. Check your API key and network connectivity\n` +
                      `2. Use more specific query descriptions\n` +
                      `3. Set LLM_API_KEY environment variable if not already set`,
              },
            ],
          };
        }
        throw error;
      }

      try {
        // Execute query via BanyanDB client
        const result = await banyandbClient.query(bydbqlQuery);

        return {
          content: [
            {
              type: "text",
              text: result,
            },
          ],
        };
      } catch (error) {
        if (error instanceof Error) {
          // Check if it's a timeout error
          if (error.message.includes("timeout") || error.message.includes("Timeout")) {
            return {
              content: [
                {
                  type: "text",
                  text: `Query timeout: ${error.message}\n\n` +
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
          if (error.message.includes("not found") || 
              error.message.includes("does not exist") ||
              error.message.includes("Empty response")) {
            const resourceType = args?.resource_type || "resource";
            const group = args?.group || "default";
            
            return {
              content: [
                {
                  type: "text",
                  text: `Query failed: ${error.message}\n\n` +
                        `Tip: Use the list_groups_schemas tool to discover available resources:\n` +
                        `- First list groups: list_groups_schemas with resource_type="groups"\n` +
                        `- Then list ${resourceType}s: list_groups_schemas with resource_type="${resourceType}s" and group="${group}"\n` +
                        `- Then query using the discovered resource names.`,
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

  // Start the server
  const transport = new StdioServerTransport();
  await server.connect(transport);

  log.info("BanyanDB MCP server started");
  log.info(`Connecting to BanyanDB at ${BANYANDB_ADDRESS}`);
}

main().catch((error) => {
  log.error("Fatal error:", error instanceof Error ? error.message : String(error));
  if (error instanceof Error && error.stack) {
    log.error("Stack trace:", error.stack);
  }
  process.exit(1);
});

