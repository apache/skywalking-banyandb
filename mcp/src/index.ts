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

import dotenv from 'dotenv';

import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { BanyanDBClient } from './client/index.js';
import { BANYANDB_ADDRESS, MCP_AUTH_TOKEN, MCP_HOST, TRANSPORT } from './config.js';
import { isLoopbackHost, startHttpServer } from './server/http.js';
import { createMcpServer } from './server/mcp.js';
import { log, setupGlobalErrorHandlers } from './utils/logger.js';

dotenv.config();
setupGlobalErrorHandlers();

async function main() {
  const banyandbClient = new BanyanDBClient(BANYANDB_ADDRESS);

  log.info('Using built-in BydbQL prompt generation with natural language support and BanyanDB schema hints. Use the list_groups_schemas tool to discover available resources and guide your queries.');

  if (TRANSPORT === 'http') {
    if (!isLoopbackHost(MCP_HOST) && !MCP_AUTH_TOKEN) {
      throw new Error('MCP_AUTH_TOKEN is required when MCP_HOST is not a loopback address.');
    }

    startHttpServer(banyandbClient);
    return;
  }

  const mcpServer = createMcpServer(banyandbClient);
  const transport = new StdioServerTransport();
  await mcpServer.connect(transport);

  log.info('BanyanDB MCP server started');
  log.info(`Connecting to BanyanDB at ${BANYANDB_ADDRESS}`);
}

main().catch((error) => {
  log.error('Fatal error:', error instanceof Error ? error.message : String(error));
  if (error instanceof Error && error.stack) {
    log.error('Stack trace:', error.stack);
  }
  process.exit(1);
});
