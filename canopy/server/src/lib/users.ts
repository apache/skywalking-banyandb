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

import type { AuthConfig, CanopyUser } from '../config.js';

const RELOAD_INTERVAL_MS = 5_000;

/**
 * The live user list. When the users come from files it re-reads them at most every
 * few seconds, so removing a user or changing a role takes effect without a restart.
 * A failed re-read (file mid-write, parse error) keeps the last good list.
 */
export class UserDirectory {
  private current: readonly CanopyUser[];
  private lastReload: number;

  constructor(
    private readonly local: AuthConfig['local'],
    private readonly onReloadError: (err: unknown) => void = () => undefined,
    private readonly now: () => number = Date.now,
  ) {
    this.current = local.users;
    this.lastReload = now();
  }

  find(username: string): CanopyUser | undefined {
    return this.users().find(u => u.username === username);
  }

  private users(): readonly CanopyUser[] {
    const reload = this.local.reload;
    if (!reload) return this.local.users;
    const now = this.now();
    if (now - this.lastReload >= RELOAD_INTERVAL_MS) {
      this.lastReload = now;
      try {
        this.current = reload();
      } catch (err) {
        this.onReloadError(err);
      }
    }
    return this.current;
  }
}
