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

import { randomBytes } from 'node:crypto';

/**
 * How a session was established when it is not a local-user login. SSO sessions keep the email
 * their role is re-resolved from; break-glass sessions resolve against the break-glass users.
 */
export type SessionOrigin = { readonly kind: 'sso'; readonly email?: string } | { readonly kind: 'breakglass' };

export interface SessionRecord {
  readonly username: string;
  readonly banyanVersion: string | null;
  readonly origin?: SessionOrigin;
  expiresAt: number;
}

export interface SessionStoreOptions {
  readonly ttlMs: number;
  readonly maxSessions?: number;
  readonly now?: () => number;
}

const DEFAULT_MAX_SESSIONS = 10000;

/** In-memory server-side session store with sliding expiry. It holds the username, never the role. */
export class SessionStore {
  private readonly sessions = new Map<string, SessionRecord>();
  private readonly ttlMs: number;
  private readonly maxSessions: number;
  private readonly now: () => number;

  constructor(options: SessionStoreOptions) {
    this.ttlMs = options.ttlMs;
    this.maxSessions = options.maxSessions ?? DEFAULT_MAX_SESSIONS;
    this.now = options.now ?? Date.now;
  }

  get size(): number {
    return this.sessions.size;
  }

  /** Creates a session and returns its id (256 random bits). */
  create(username: string, banyanVersion: string | null, origin?: SessionOrigin): string {
    this.sweep();
    while (this.sessions.size >= this.maxSessions) {
      const oldest = this.sessions.keys().next();
      if (oldest.done) break;
      this.sessions.delete(oldest.value);
    }
    const id = randomBytes(32).toString('base64url');
    this.sessions.set(id, { username, banyanVersion, origin, expiresAt: this.now() + this.ttlMs });
    return id;
  }

  /** Returns the live session and extends its expiry, or undefined when it is unknown or expired. */
  touch(id: string | undefined): SessionRecord | undefined {
    if (!id) return undefined;
    const record = this.sessions.get(id);
    if (!record) return undefined;
    const now = this.now();
    if (record.expiresAt <= now) {
      this.sessions.delete(id);
      return undefined;
    }
    record.expiresAt = now + this.ttlMs;
    // Re-insert so the Map's iteration order stays least-recently-used first.
    this.sessions.delete(id);
    this.sessions.set(id, record);
    return record;
  }

  destroy(id: string | undefined): void {
    if (id) this.sessions.delete(id);
  }

  /** Drops every expired session. */
  sweep(): void {
    const now = this.now();
    for (const [id, record] of this.sessions) {
      if (record.expiresAt <= now) this.sessions.delete(id);
    }
  }
}
