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

import { spawnSync } from 'node:child_process';
import { mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const toolsDir = path.dirname(fileURLToPath(import.meta.url));
const mcpRootDir = path.resolve(toolsDir, '..');
const binaryName = process.platform === 'win32' ? 'bydbql-parse.exe' : 'bydbql-parse';
const outputDir = path.join(toolsDir, 'bin');
const outputPath = path.join(outputDir, binaryName);

mkdirSync(outputDir, { recursive: true });

const result = spawnSync('go', ['build', '-o', outputPath, './tools/bydbql-parse'], {
  cwd: mcpRootDir,
  env: {
    ...process.env,
    GOCACHE: process.env.GOCACHE || path.join(tmpdir(), 'banyandb-mcp-go-build-cache'),
    GOMODCACHE: process.env.GOMODCACHE || path.join(tmpdir(), 'banyandb-mcp-go-mod-cache'),
    GOPATH: process.env.GOPATH || path.join(tmpdir(), 'banyandb-mcp-go-path'),
    GOFLAGS: process.env.GOFLAGS || '-mod=readonly',
  },
  stdio: 'inherit',
});

process.exit(result.status ?? 1);
