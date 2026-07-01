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
import { tmpdir } from 'node:os';
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

type ValidatorCommand = {
  command: string;
  args: string[];
  cwd: string;
};

function mcpRootDir(): string {
  const modulePath = fileURLToPath(import.meta.url);
  return path.resolve(path.dirname(modulePath), '..', '..');
}

function validatorCommands(rootDir: string): ValidatorCommand[] {
  const binaryPath = path.join(rootDir, 'tools', 'bin', validatorBinaryName);
  if (existsSync(binaryPath)) {
    return [
      { command: binaryPath, args: [], cwd: rootDir },
      { command: 'go', args: ['run', './tools/bydbql-parse'], cwd: rootDir },
    ];
  }

  return [{ command: 'go', args: ['run', './tools/bydbql-parse'], cwd: rootDir }];
}

function validationEnvironment(): NodeJS.ProcessEnv {
  return {
    ...process.env,
    GOCACHE: process.env.GOCACHE || path.join(tmpdir(), 'banyandb-mcp-go-build-cache'),
    GOMODCACHE: process.env.GOMODCACHE || path.join(tmpdir(), 'banyandb-mcp-go-mod-cache'),
    GOPATH: process.env.GOPATH || path.join(tmpdir(), 'banyandb-mcp-go-path'),
    GOFLAGS: process.env.GOFLAGS || '-mod=readonly',
  };
}

export async function validateBydbQLSyntax(query: string): Promise<BydbQLParseValidationResult> {
  const rootDir = mcpRootDir();
  const validators = validatorCommands(rootDir);
  let lastError: Error | undefined;

  for (const validator of validators) {
    try {
      return await executeValidator(validator, query);
    } catch (executionError) {
      lastError = executionError instanceof Error ? executionError : new Error(String(executionError));
    }
  }

  throw lastError ?? new Error('BydbQL parser validation failed');
}

async function executeValidator(validator: ValidatorCommand, query: string): Promise<BydbQLParseValidationResult> {
  return new Promise((resolve, reject) => {
    const childProcess = spawn(validator.command, validator.args, {
      cwd: validator.cwd,
      env: validationEnvironment(),
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
