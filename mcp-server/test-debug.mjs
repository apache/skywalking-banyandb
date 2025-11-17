#!/usr/bin/env node

/**
 * Test script to trigger breakpoints in the MCP server
 * 
 * Usage:
 * 1. Start your server with: npx tsx --inspect src/index.ts
 * 2. Open Chrome DevTools at chrome://inspect
 * 3. Set breakpoints in your code
 * 4. Run this script: node test-debug.mjs
 */

import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const server = spawn('npx', ['tsx', '--inspect', 'src/index.ts'], {
  cwd: __dirname,
  stdio: ['pipe', 'pipe', 'inherit'],
  env: { 
    ...process.env, 
    BANYANDB_ADDRESS: process.env.BANYANDB_ADDRESS || 'localhost:17900'
  }
});

let messageId = 1;
let initialized = false;

function sendMessage(method, params, isNotification = false) {
  const message = {
    jsonrpc: '2.0',
    method: method,
    params: params
  };
  
  if (!isNotification) {
    message.id = messageId++;
  }
  
  console.log(`\n📤 Sending: ${method}${isNotification ? ' (notification)' : ''}`);
  server.stdin.write(JSON.stringify(message) + '\n');
}

// Buffer for incomplete JSON messages
let buffer = '';

// Handle responses
server.stdout.on('data', (data) => {
  buffer += data.toString();
  const lines = buffer.split('\n');
  
  // Keep the last incomplete line in buffer
  buffer = lines.pop() || '';
  
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    
    try {
      const response = JSON.parse(trimmed);
      
      if (response.id !== undefined) {
        // This is a response to a request
        if (response.result) {
          console.log('✅ Response:', JSON.stringify(response.result, null, 2));
          
          // After initialize response, send initialized notification
          if (response.result.serverInfo && !initialized) {
            initialized = true;
            setTimeout(() => {
              sendMessage('initialized', {}, true);
            }, 100);
          }
        } else if (response.error) {
          console.error('❌ Error:', JSON.stringify(response.error, null, 2));
        }
      } else {
        // This might be a notification from server
        console.log('📨 Notification:', JSON.stringify(response, null, 2));
      }
    } catch (e) {
      // Not JSON, probably log output (stderr redirected to stdout)
      if (trimmed) {
        process.stdout.write(trimmed + '\n');
      }
    }
  }
});

// Initialize MCP connection
setTimeout(() => {
  console.log('🚀 Starting MCP test...\n');
  
  // Initialize request
  sendMessage('initialize', {
    protocolVersion: '2024-11-05',
    capabilities: {},
    clientInfo: {
      name: 'test-debug',
      version: '1.0.0'
    }
  });
  
  // List tools (after initialization completes)
  setTimeout(() => {
    if (initialized) {
      sendMessage('tools/list', {});
    }
  }, 1000);
  
  // Call list_groups_schemas
  setTimeout(() => {
    if (initialized) {
      sendMessage('tools/call', {
        name: 'list_groups_schemas',
        arguments: {
          resource_type: 'groups'
        }
      });
    }
  }, 2000);
  
  // Call query_banyandb
  setTimeout(() => {
    if (initialized) {
      sendMessage('tools/call', {
        name: 'query_banyandb',
        arguments: {
          description: 'Show me all error logs from the last hour'
        }
      });
    }
  }, 3000);
  
  // Exit after a delay
  setTimeout(() => {
    console.log('\n✅ Test complete. Check Chrome DevTools for breakpoints!');
    server.kill();
    process.exit(0);
  }, 6000);
  
}, 500);

// Handle errors
server.on('error', (error) => {
  console.error('❌ Server error:', error);
  process.exit(1);
});

server.on('exit', (code) => {
  if (code !== 0 && code !== null) {
    console.error(`\n❌ Server exited with code ${code}`);
  }
});

