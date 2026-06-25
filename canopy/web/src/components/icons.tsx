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

interface IconProps { size?: number; }

function Ic({ size = 18, sw = 1.6, children }: { size?: number; sw?: number; children: React.ReactNode }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none"
      stroke="currentColor" strokeWidth={sw} strokeLinecap="round" strokeLinejoin="round">
      {children}
    </svg>
  );
}

export function IconHome({ size = 18 }: IconProps) {
  return (
    <Ic size={size}>
      <path d="M3 10.5 12 3l9 7.5" />
      <path d="M5 9.5V20a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1V9.5" />
      <path d="M9.5 21v-6h5v6" />
    </Ic>
  );
}

export function IconMetadata({ size = 18 }: IconProps) {
  return (
    <Ic size={size}>
      <path d="M12 3 3 7.5 12 12l9-4.5L12 3Z" />
      <path d="M3 12.5 12 17l9-4.5" />
      <path d="M3 17 12 21.5 21 17" />
    </Ic>
  );
}

export function IconMeasures({ size = 18 }: IconProps) {
  return (
    <Ic size={size}>
      <path d="M3 12h4l2-6 4 13 2-7h6" />
    </Ic>
  );
}

export function IconStreams({ size = 18 }: IconProps) {
  return (
    <Ic size={size}>
      <path d="M3 8c2.5 0 2.5 2.4 5 2.4S10.5 8 13 8s2.5 2.4 5 2.4S20.5 8 21 8" />
      <path d="M3 15c2.5 0 2.5 2.4 5 2.4S10.5 15 13 15s2.5 2.4 5 2.4 2.5-2.4 3-2.4" />
    </Ic>
  );
}

export function IconTraces({ size = 18 }: IconProps) {
  return (
    <Ic size={size}>
      <circle cx="5" cy="6" r="2" />
      <circle cx="5" cy="18" r="2" />
      <circle cx="19" cy="12" r="2" />
      <path d="M7 6h6a4 4 0 0 1 4 4v.4M7 18h6a4 4 0 0 0 4-4v-.4" />
    </Ic>
  );
}

export function IconProperties({ size = 18 }: IconProps) {
  return (
    <Ic size={size}>
      <path d="M3 7.5A1.5 1.5 0 0 1 4.5 6h6l3.5 3.5a2 2 0 0 1 0 2.8L10 16.8a2 2 0 0 1-2.8 0L3.7 13.3A1.5 1.5 0 0 1 3 12V7.5Z" />
      <circle cx="7" cy="10" r="1.1" />
      <path d="M14 8.5 18 6l3 1.5v9L18 18l-4-2.5" />
    </Ic>
  );
}

export function IconPipelines({ size = 18 }: IconProps) {
  return (
    <Ic size={size}>
      <path d="M3 5h18l-7 8v5l-4 2v-7L3 5Z" />
    </Ic>
  );
}

export function IconQuery({ size = 18 }: IconProps) {
  return (
    <Ic size={size}>
      <rect x="3" y="4" width="18" height="16" rx="2" />
      <path d="M7 9l3 3-3 3" />
      <path d="M13 15h4" />
    </Ic>
  );
}

export function IconChevron({ size = 13 }: IconProps) {
  return (
    <Ic size={size}>
      <path d="m9 6 6 6-6 6" />
    </Ic>
  );
}

export function IconCollapse({ size = 17 }: IconProps) {
  return (
    <Ic size={size}>
      <path d="m13 7-5 5 5 5" />
      <path d="m18 7-5 5 5 5" />
    </Ic>
  );
}

export function IconSignOut({ size = 15 }: IconProps) {
  return (
    <Ic size={size} sw={1.8}>
      <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" />
      <path d="m16 17 5-5-5-5" />
      <path d="M21 12H9" />
    </Ic>
  );
}

export function IconShield({ size = 15 }: IconProps) {
  return (
    <Ic size={size} sw={1.8}>
      <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10Z" />
      <path d="m9 12 2 2 4-4" />
    </Ic>
  );
}

export function IconViewer({ size = 15 }: IconProps) {
  return (
    <Ic size={size} sw={1.8}>
      <path d="M2 12s3-8 10-8 10 8 10 8-3 8-10 8-10-8-10-8Z" />
      <circle cx="12" cy="12" r="3" />
    </Ic>
  );
}

export function IconGroup({ size = 18 }: IconProps) {
  return (
    <Ic size={size}>
      <rect x="3" y="3" width="7" height="7" rx="1" />
      <rect x="14" y="3" width="7" height="7" rx="1" />
      <rect x="3" y="14" width="7" height="7" rx="1" />
      <rect x="14" y="14" width="7" height="7" rx="1" />
    </Ic>
  );
}

export function IconPlus({ size = 18 }: IconProps) {
  return (
    <Ic size={size}>
      <path d="M12 5v14M5 12h14" />
    </Ic>
  );
}

export function IconEdit({ size = 18 }: IconProps) {
  return (
    <Ic size={size}>
      <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7" />
      <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5Z" />
    </Ic>
  );
}

export function IconTrash({ size = 18 }: IconProps) {
  return (
    <Ic size={size}>
      <path d="M3 6h18M8 6V4h8v2M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6" />
    </Ic>
  );
}

export function IconSearch({ size = 18 }: IconProps) {
  return (
    <Ic size={size}>
      <circle cx="11" cy="11" r="7" />
      <path d="m21 21-4.35-4.35" />
    </Ic>
  );
}

export function IconPlay({ size = 18 }: IconProps) {
  return (
    <Ic size={size}>
      <polygon points="5 3 19 12 5 21 5 3" />
    </Ic>
  );
}

export function IconArrowRight({ size = 18 }: IconProps) {
  return (
    <Ic size={size}>
      <path d="M5 12h14M12 5l7 7-7 7" />
    </Ic>
  );
}

export function IconArrowLeft({ size = 18 }: IconProps) {
  return (
    <Ic size={size}>
      <path d="M19 12H5M12 19l-7-7 7-7" />
    </Ic>
  );
}

export function IconKey({ size = 18 }: IconProps) {
  return (
    <Ic size={size}>
      <circle cx="7.5" cy="15.5" r="5.5" />
      <path d="m21 2-9.6 9.6M15.5 7.5l3 3L22 7l-3-3" />
    </Ic>
  );
}

export function IconIndex({ size = 18 }: IconProps) {
  return (
    <Ic size={size}>
      <path d="M4 7h16M4 12h10M4 17h6" />
    </Ic>
  );
}

export function IconAlert({ size = 18 }: IconProps) {
  return (
    <Ic size={size}>
      <path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0Z" />
      <path d="M12 9v4M12 17h.01" />
    </Ic>
  );
}

export function IconEmpty({ size = 18 }: IconProps) {
  return (
    <Ic size={size}>
      <circle cx="12" cy="12" r="9" />
      <path d="M9 9h.01M15 9h.01M9 15a3 3 0 0 0 6 0" />
    </Ic>
  );
}
