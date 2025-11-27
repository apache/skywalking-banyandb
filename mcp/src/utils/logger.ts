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

/**
 * Logging helper for MCP servers.
 *
 * Note: All logging uses console.error (stderr) because stdout is reserved
 * for the MCP protocol (JSON-RPC messages). Using console.log would corrupt
 * the protocol stream.
 */
export const log = {
  info: (...args: unknown[]) => console.error('[MCP] [INFO]', ...args),
  warn: (...args: unknown[]) => console.error('[MCP] [WARN]', ...args),
  error: (...args: unknown[]) => console.error('[MCP] [ERROR]', ...args),
};

/**
 * Sets up global error handlers to ensure all errors are visible.
 * This should be called early in the application startup, before any async operations.
 */
export function setupGlobalErrorHandlers(): void {
  process.on('uncaughtException', (error) => {
    log.error('Uncaught Exception:', error.message);
    if (error.stack) {
      log.error('Stack trace:', error.stack);
    }
    process.exit(1);
  });

  process.on('unhandledRejection', (reason, promise) => {
    log.error('Unhandled Rejection at:', promise);
    log.error('Reason:', reason instanceof Error ? reason.message : String(reason));
    if (reason instanceof Error && reason.stack) {
      log.error('Stack trace:', reason.stack);
    }
    process.exit(1);
  });
}

