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

import type { AuthConfig, BreakGlassConfig, SsoRolesConfig } from '../config.js';

const RELOAD_INTERVAL_MS = 5_000;

/**
 * The live SSO role mapping and break-glass accounts. When they come from a file it re-reads
 * them at most every few seconds, so a removed role or a disabled break-glass account stops
 * working without a restart. A failed re-read keeps the last good policy.
 */
export class AuthPolicyDirectory {
  private roleMap: SsoRolesConfig | undefined;
  private breakGlassConfig: BreakGlassConfig | undefined;
  private lastReload: number;

  constructor(
    private readonly auth: AuthConfig,
    private readonly onReloadError: (err: unknown) => void = () => undefined,
    private readonly now: () => number = Date.now,
  ) {
    this.roleMap = auth.sso?.roles;
    this.breakGlassConfig = auth.breakGlass;
    this.lastReload = now();
  }

  roles(): SsoRolesConfig | undefined {
    this.refresh();
    return this.roleMap;
  }

  /** The break-glass accounts, or undefined when break-glass is not enabled. */
  breakGlass(): BreakGlassConfig | undefined {
    this.refresh();
    return this.breakGlassConfig?.enabled ? this.breakGlassConfig : undefined;
  }

  private refresh(): void {
    const reload = this.auth.reloadPolicy;
    if (!reload) return;
    const now = this.now();
    if (now - this.lastReload < RELOAD_INTERVAL_MS) return;
    this.lastReload = now;
    try {
      const policy = reload();
      this.roleMap = policy.roles;
      this.breakGlassConfig = policy.breakGlass;
    } catch (err) {
      this.onReloadError(err);
    }
  }
}
