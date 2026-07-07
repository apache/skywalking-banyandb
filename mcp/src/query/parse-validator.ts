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

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const validatorTimeoutMs = 10000;
const validatorBinaryName = process.platform === 'win32' ? 'bydbql-parse.exe' : 'bydbql-parse';

export type BydbQLParseValidationResult = {
  valid: boolean;
  message: string;
  queryType?: string;
  syntaxOnly: boolean;
  warnings?: string[];
};

function mcpRootDir(): string {
  const modulePath = fileURLToPath(import.meta.url);
  return path.resolve(path.dirname(modulePath), '..', '..');
}

// Resolves the prebuilt validator binary path. The binary is a build artifact
// produced by `npm run build:validator`; it is intentionally not shelled out to
// `go run` at query time so the plugin's runtime does not depend on a Go
// toolchain and does not pay a cold-compile cost inside the validation timeout.
export function validatorBinaryPath(rootDir: string = mcpRootDir()): string {
  return path.join(rootDir, 'tools', 'bin', validatorBinaryName);
}

export async function validateBydbQLSyntax(query: string): Promise<BydbQLParseValidationResult> {
  const binaryPath = validatorBinaryPath();
  if (!existsSync(binaryPath)) {
    throw new Error(
      `BydbQL validator binary not found at ${binaryPath}. ` +
        'Build it once with `npm run build:validator` (requires the Go toolchain) before starting the MCP server.',
    );
  }

  return executeValidator(binaryPath, query);
}

async function executeValidator(binaryPath: string, query: string): Promise<BydbQLParseValidationResult> {
  return new Promise((resolve, reject) => {
    const childProcess = spawn(binaryPath, [], {
      cwd: path.dirname(binaryPath),
      env: process.env,
      stdio: ['pipe', 'pipe', 'pipe'],
    });

    let stdout = '';
    let stderr = '';
    let completed = false;

    const timeout = setTimeout(() => {
      if (completed) {
        return;
      }
      completed = true;
      childProcess.kill('SIGKILL');
      reject(new Error('BydbQL parser validation timed out'));
    }, validatorTimeoutMs);

    childProcess.stdout.on('data', (chunk: Buffer) => {
      stdout += chunk.toString('utf-8');
    });

    childProcess.stderr.on('data', (chunk: Buffer) => {
      stderr += chunk.toString('utf-8');
    });

    childProcess.on('error', (error) => {
      if (completed) {
        return;
      }
      completed = true;
      clearTimeout(timeout);
      reject(error);
    });

    childProcess.on('close', (code) => {
      if (completed) {
        return;
      }
      completed = true;
      clearTimeout(timeout);

      if (!stdout.trim()) {
        reject(new Error(stderr.trim() || `BydbQL parser exited with code ${code ?? 'unknown'}`));
        return;
      }

      try {
        resolve(JSON.parse(stdout) as BydbQLParseValidationResult);
      } catch (error) {
        reject(
          new Error(
            `failed to parse BydbQL parser response: ${error instanceof Error ? error.message : String(error)}\n${stdout}`,
          ),
        );
      }
    });

    childProcess.stdin.write(JSON.stringify({ query }));
    childProcess.stdin.end();
  });
}
