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

import type { FastifyRequest, FastifyReply } from 'fastify';

export type Role = 'admin' | 'readonly';

// The single place where configured role names map onto Canopy's two roles.
// Horizon's `viewer` is read-only; `maintainer`, `operator` and `admin` can write.
const ROLE_ALIASES: Readonly<Record<string, Role>> = {
  readonly: 'readonly',
  viewer: 'readonly',
  admin: 'admin',
  maintainer: 'admin',
  operator: 'admin',
};

/** Maps a configured role name to a Canopy role, or undefined when it is unknown. */
export function mapRole(value: unknown): Role | undefined {
  return typeof value === 'string' && Object.hasOwn(ROLE_ALIASES, value) ? ROLE_ALIASES[value] : undefined;
}

const READ_METHODS = new Set(['GET', 'HEAD', 'OPTIONS']);

export function isReadOnlySession(request: FastifyRequest): boolean {
  return request.principal?.role === 'readonly';
}

export async function enforceRole(request: FastifyRequest, reply: FastifyReply): Promise<void> {
  if (!READ_METHODS.has(request.method) && isReadOnlySession(request)) {
    await reply.status(403).send({
      error: 'forbidden',
      message: 'Your account has read-only access. This operation requires an admin role.',
    });
  }
}
