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

import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const toolsDir = path.dirname(fileURLToPath(import.meta.url));
const repoRootDir = path.resolve(toolsDir, '..', '..');
const claudeManifestPath = path.join(repoRootDir, '.claude-plugin', 'plugin.json');
const codexManifestPath = path.join(repoRootDir, '.codex-plugin', 'plugin.json');

function readManifest(manifestPath) {
  return JSON.parse(readFileSync(manifestPath, 'utf8'));
}

function withoutVersion(manifest) {
  const normalizedManifest = { ...manifest };
  delete normalizedManifest.version;
  return normalizedManifest;
}

function normalize(value) {
  if (Array.isArray(value)) {
    return value.map((item) => normalize(item));
  }
  if (value !== null && typeof value === 'object') {
    return Object.keys(value)
      .sort()
      .reduce((normalizedValue, key) => {
        normalizedValue[key] = normalize(value[key]);
        return normalizedValue;
      }, {});
  }
  return value;
}

function stableJSON(value) {
  return JSON.stringify(normalize(value));
}

const claudeManifest = readManifest(claudeManifestPath);
const codexManifest = readManifest(codexManifestPath);
let hasError = false;

function reportError(message) {
  console.error(`ERROR: ${message}`);
  hasError = true;
}

if (stableJSON(withoutVersion(claudeManifest)) !== stableJSON(withoutVersion(codexManifest))) {
  reportError('.claude-plugin/plugin.json and .codex-plugin/plugin.json must match except for version.');
}

if (typeof claudeManifest.version !== 'string') {
  reportError('.claude-plugin/plugin.json must define a string version.');
}

if (typeof codexManifest.version !== 'string') {
  reportError('.codex-plugin/plugin.json must define a string version.');
} else {
  const codexVersionParts = codexManifest.version.split('+');
  const codexBaseVersion = codexVersionParts[0];
  const codexBuildMetadata = codexVersionParts.slice(1).join('+');
  if (codexBaseVersion !== claudeManifest.version || !codexBuildMetadata.startsWith('codex.')) {
    reportError('.codex-plugin/plugin.json version must use the Claude version plus a +codex.<timestamp> suffix.');
  }
}

if (hasError) {
  process.exit(1);
}

console.log('Plugin manifests are synchronized; only version differs.');
