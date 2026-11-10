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

import type { IncomingMessage } from 'node:http';

import { describe, expect, it } from 'vitest';

import { getAuthorizationToken } from './http.js';

function withAuthorization(value?: string): Pick<IncomingMessage, 'headers'> {
  return { headers: value === undefined ? {} : { authorization: value } };
}

describe('getAuthorizationToken', () => {
  it('reads the token from a Bearer header', () => {
    expect(getAuthorizationToken(withAuthorization('Bearer secret-token'))).toBe('secret-token');
  });

  it('accepts the scheme name in any case', () => {
    expect(getAuthorizationToken(withAuthorization('bearer secret-token'))).toBe('secret-token');
    expect(getAuthorizationToken(withAuthorization('BEARER secret-token'))).toBe('secret-token');
  });

  it('ignores extra whitespace around the scheme and token', () => {
    expect(getAuthorizationToken(withAuthorization('  Bearer   secret-token  '))).toBe('secret-token');
  });

  it('returns an empty token when the header is missing or not a Bearer credential', () => {
    expect(getAuthorizationToken(withAuthorization())).toBe('');
    expect(getAuthorizationToken(withAuthorization('Basic dXNlcjpwYXNz'))).toBe('');
    expect(getAuthorizationToken(withAuthorization('Bearer'))).toBe('');
    expect(getAuthorizationToken(withAuthorization('Bearersecret-token'))).toBe('');
  });
});
