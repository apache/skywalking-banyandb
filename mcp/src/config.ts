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

export const BANYANDB_ADDRESS = process.env.BANYANDB_ADDRESS || 'localhost:17900';
export const TRANSPORT = process.env.TRANSPORT || 'stdio';
export const MCP_HOST = process.env.MCP_HOST || '127.0.0.1';
export const MCP_AUTH_TOKEN = process.env.MCP_AUTH_TOKEN?.trim() || '';

function parsePositiveInteger(rawValue: string | undefined, defaultValue: string, fieldName: string): number {
  const parsedValue = parseInt(rawValue || defaultValue, 10);
  if (!Number.isFinite(parsedValue) || parsedValue <= 0) {
    throw new Error(`Invalid ${fieldName} value "${rawValue}": must be a positive integer.`);
  }

  return parsedValue;
}

const mcpPortRaw = parseInt(process.env.MCP_PORT || '3000', 10);
if (!Number.isFinite(mcpPortRaw) || mcpPortRaw <= 0 || mcpPortRaw > 65535) {
  throw new Error(`Invalid MCP_PORT value "${process.env.MCP_PORT}": must be an integer between 1 and 65535.`);
}

export const MCP_PORT = mcpPortRaw;
export const MCP_MAX_BODY_BYTES = parsePositiveInteger(process.env.MCP_MAX_BODY_BYTES, `${1024 * 1024}`, 'MCP_MAX_BODY_BYTES');
export const MAX_ERROR_MESSAGE_LENGTH = 1024;
export const MAX_TOOL_RESPONSE_LENGTH = 64 * 1024;
export const MCP_RATE_LIMIT_WINDOW_MS = parsePositiveInteger(process.env.MCP_RATE_LIMIT_WINDOW_MS, '60000', 'MCP_RATE_LIMIT_WINDOW_MS');
export const MCP_RATE_LIMIT_MAX_REQUESTS = parsePositiveInteger(process.env.MCP_RATE_LIMIT_MAX_REQUESTS, '60', 'MCP_RATE_LIMIT_MAX_REQUESTS');
export const MCP_RATE_LIMIT_MAX_CLIENTS = parsePositiveInteger(process.env.MCP_RATE_LIMIT_MAX_CLIENTS, '10000', 'MCP_RATE_LIMIT_MAX_CLIENTS');
