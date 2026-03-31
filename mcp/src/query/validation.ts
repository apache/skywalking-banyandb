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

const maxDescriptionLength = 2048;
const maxIdentifierLength = 256;
const maxBydbQLLength = 4096;
const identifierPattern = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/;
const allowedResourceTypes = ['groups', 'streams', 'measures', 'traces', 'properties'] as const;
const allowedQueryHintResourceTypes = ['stream', 'measure', 'trace', 'property'] as const;
const disallowedQueryTokenPatterns = [/[;]/, /--/, /\/\*/, /\*\//];
const allowedQueryPrefixPatterns = [/^\s*SELECT\b/i, /^\s*SHOW\s+TOP\b/i];

export type QueryHints = {
  description?: string;
  BydbQL?: string;
  resource_type?: string;
  resource_name?: string;
  group?: string;
};

export type ListGroupsArgs = {
  resourceType: (typeof allowedResourceTypes)[number];
  group?: string;
};

function containsControlCharacters(value: string): boolean {
  for (const char of value) {
    const charCode = char.charCodeAt(0);
    if (
      (charCode >= 0x00 && charCode <= 0x08) ||
      charCode === 0x0b ||
      charCode === 0x0c ||
      (charCode >= 0x0e && charCode <= 0x1f) ||
      charCode === 0x7f
    ) {
      return true;
    }
  }

  return false;
}

function validateTextInput(fieldName: string, rawValue: string, maxLength: number): string {
  const value = rawValue.trim();
  if (!value) {
    throw new Error(`${fieldName} cannot be empty`);
  }
  if (value.length > maxLength) {
    throw new Error(`${fieldName} exceeds the maximum length of ${maxLength} characters`);
  }
  if (containsControlCharacters(value)) {
    throw new Error(`${fieldName} contains unsupported control characters`);
  }

  return value;
}

function validateIdentifier(fieldName: string, rawValue: string): string {
  const value = validateTextInput(fieldName, rawValue, maxIdentifierLength);
  if (!identifierPattern.test(value)) {
    throw new Error(`${fieldName} contains unsupported characters`);
  }

  return value;
}

function validateResourceType(rawValue: string): (typeof allowedResourceTypes)[number] {
  const value = validateTextInput('resource_type', rawValue, 32).toLowerCase();
  if (!allowedResourceTypes.includes(value as (typeof allowedResourceTypes)[number])) {
    throw new Error('resource_type must be one of: groups, streams, measures, traces, properties');
  }

  return value as (typeof allowedResourceTypes)[number];
}

function validateQueryHintResourceType(rawValue: string): (typeof allowedQueryHintResourceTypes)[number] {
  const value = validateTextInput('resource_type', rawValue, 32).toLowerCase();
  if (!allowedQueryHintResourceTypes.includes(value as (typeof allowedQueryHintResourceTypes)[number])) {
    throw new Error('resource_type must be one of: stream, measure, trace, property');
  }

  return value as (typeof allowedQueryHintResourceTypes)[number];
}

function validateBydbQL(rawValue: string): string {
  const value = validateTextInput('BydbQL', rawValue, maxBydbQLLength);
  if (!allowedQueryPrefixPatterns.some((pattern) => pattern.test(value))) {
    throw new Error('BydbQL must be a read-only SELECT or SHOW TOP query');
  }
  for (const disallowedPattern of disallowedQueryTokenPatterns) {
    if (disallowedPattern.test(value)) {
      throw new Error('BydbQL contains unsupported multi-statement or comment syntax');
    }
  }

  return value;
}

export function normalizeQueryHints(args: unknown): QueryHints {
  if (!args || typeof args !== 'object') {
    return {};
  }

  const rawArgs = args as Record<string, unknown>;
  return {
    description: typeof rawArgs.description === 'string' ? rawArgs.description.trim() : undefined,
    BydbQL: typeof rawArgs.BydbQL === 'string' ? rawArgs.BydbQL.trim() : undefined,
    resource_type: typeof rawArgs.resource_type === 'string' ? rawArgs.resource_type.trim() : undefined,
    resource_name: typeof rawArgs.resource_name === 'string' ? rawArgs.resource_name.trim() : undefined,
    group: typeof rawArgs.group === 'string' ? rawArgs.group.trim() : undefined,
  };
}

export function validateListGroupsArgs(args: unknown): ListGroupsArgs {
  if (!args || typeof args !== 'object') {
    throw new Error('tool arguments must be an object');
  }

  const rawArgs = args as Record<string, unknown>;
  const rawResourceType = rawArgs.resource_type;
  if (typeof rawResourceType !== 'string') {
    throw new Error('resource_type is required and must be one of: groups, streams, measures, traces, properties');
  }

  const resourceType = validateResourceType(rawResourceType);
  if (resourceType === 'groups') {
    return { resourceType };
  }

  const rawGroup = rawArgs.group;
  if (typeof rawGroup !== 'string') {
    throw new Error(`group is required for listing ${resourceType}`);
  }

  return {
    resourceType,
    group: validateIdentifier('group', rawGroup),
  };
}

export function validateQueryHints(queryHints: QueryHints): QueryHints {
  const validatedHints: QueryHints = {};

  if (queryHints.description) {
    validatedHints.description = validateTextInput('description', queryHints.description, maxDescriptionLength);
  }
  if (queryHints.BydbQL) {
    validatedHints.BydbQL = validateBydbQL(queryHints.BydbQL);
  }
  if (queryHints.resource_type) {
    validatedHints.resource_type = validateQueryHintResourceType(queryHints.resource_type);
  }
  if (queryHints.resource_name) {
    validatedHints.resource_name = validateIdentifier('resource_name', queryHints.resource_name);
  }
  if (queryHints.group) {
    validatedHints.group = validateIdentifier('group', queryHints.group);
  }

  return validatedHints;
}
