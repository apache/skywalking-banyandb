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

import React from 'react';

interface CanopyMarkProps {
  size?: number;
  glow?: boolean;
  className?: string;
}

export function CanopyMark({ size = 64, glow = false, className }: CanopyMarkProps) {
  return (
    <svg
      className={className}
      width={size}
      height={size}
      viewBox="0 0 64 64"
      style={{
        color: 'var(--accent)',
        filter: glow ? 'drop-shadow(0 0 28px var(--accent-glow))' : undefined,
        flexShrink: 0,
      }}
    >
      <circle cx="32" cy="32" r="26.2" fill="none" stroke="currentColor" strokeWidth="2.6" />
      <path d="M40.5 9.4 A 24 24 0 1 0 40.5 54.6 A 30 30 0 0 1 40.5 9.4 Z" fill="currentColor" opacity="0.24" />
      <path d="M17.5 33 C 12.6 33, 11.2 26.4, 15.6 24 C 13.4 18.2, 19.4 13.4, 24.6 16.6 C 26.8 11.4, 35.4 11.4, 38 16.6 C 43.2 13.6, 49.4 18.4, 46.8 24.2 C 51.2 26.6, 49.6 33, 44.8 33 Z" fill="currentColor" />
      <path d="M32 31.5 L 32 50.5" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" />
      <g fill="none" stroke="currentColor" strokeWidth="2.6" strokeLinecap="round">
        <path d="M32 40 C 28.6 39.6, 25.8 38.4, 23.6 36.2" />
        <path d="M32 43.6 C 35.4 43.2, 38.2 42, 40.4 39.8" />
      </g>
      <circle cx="22.4" cy="35.4" r="2.3" fill="none" stroke="currentColor" strokeWidth="2.6" />
      <circle cx="41.6" cy="39" r="2.3" fill="none" stroke="currentColor" strokeWidth="2.6" />
    </svg>
  );
}
