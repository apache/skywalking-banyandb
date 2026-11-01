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

import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import yaml from 'js-yaml';
import { mintToken } from './tokens.js';

/**
 * Mints a token and appends its hash entry to a YAML/JSON tokens file (created if missing).
 * An existing file must be empty, a list, or a mapping with a `tokens` list; anything else
 * is refused untouched. Returns the token, which is never stored.
 */
export function appendToken(file: string, id: string, username: string, description?: string): string {
  const isJson = file.endsWith('.json');
  let doc: unknown = [];
  if (existsSync(file)) {
    const raw = readFileSync(file, 'utf-8');
    const parsed: unknown = raw.trim() === '' ? null : (isJson ? JSON.parse(raw) : yaml.load(raw));
    if (parsed !== null && parsed !== undefined) doc = parsed;
  }
  const wrapper = typeof doc === 'object' && doc !== null && !Array.isArray(doc) ? doc as Record<string, unknown> : undefined;
  const list = wrapper ? wrapper.tokens : doc;
  if (!Array.isArray(list)) {
    throw new Error(`${file} is not a tokens file (expected a list, or a mapping with a "tokens" list); refusing to overwrite it`);
  }
  if (list.some(entry => typeof entry === 'object' && entry !== null && (entry as { id?: unknown }).id === id)) {
    throw new Error(`token id "${id}" already exists in ${file}`);
  }
  const { token, entry } = mintToken(id, username, description);
  const updated = wrapper ? { ...wrapper, tokens: [...list, { ...entry }] } : [...list, { ...entry }];
  writeFileSync(file, isJson ? `${JSON.stringify(updated, null, 2)}\n` : yaml.dump(updated), { mode: 0o600 });
  return token;
}
