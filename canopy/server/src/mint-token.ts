/*
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

// Mints an API token: prints it once and appends its hash entry to a tokens file.
//   node dist/src/mint-token.js --file tokens.yaml --id t1 --username alice [--description "ci"]

import { appendToken } from './lib/token-file.js';

function arg(name: string): string | undefined {
  const index = process.argv.indexOf(`--${name}`);
  return index >= 0 ? process.argv[index + 1] : undefined;
}

function main(): void {
  const file = arg('file');
  const id = arg('id');
  const username = arg('username');
  if (!file || !id || !username) {
    console.error('usage: mint-token --file <tokens.yaml|json> --id <id> --username <user> [--description <text>]');
    process.exit(2);
  }
  try {
    const token = appendToken(file, id, username, arg('description'));
    console.log(token);
    console.error(`Hash entry written to ${file}. The token above is shown only once.`);
  } catch (err) {
    console.error(err instanceof Error ? err.message : String(err));
    process.exit(1);
  }
}

main();
