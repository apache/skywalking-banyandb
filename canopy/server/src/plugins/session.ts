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

import type { FastifyInstance } from 'fastify';
import secureSession from '@fastify/secure-session';
import type { Config } from '../config.js';

declare module '@fastify/secure-session' {
  interface SessionData {
    user: string;
    role: 'admin' | 'readonly';
    endpoint: string;
    banyanVersion: string | null;
  }
}

export async function registerSession(app: FastifyInstance, config: Config): Promise<void> {
  await app.register(secureSession, {
    secret: config.sessionSecret,
    salt: 'canopy-sess-salt',
    cookie: {
      path: '/',
      httpOnly: true,
      sameSite: 'strict',
      secure: process.env.NODE_ENV === 'production',
      maxAge: 8 * 60 * 60, // 8 hours TTL
    },
  });
}
