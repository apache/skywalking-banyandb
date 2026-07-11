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

// ResultError.tsx — friendly error state for the query result pane.
// Turns terse BFF/DB messages ("Upstream returned 500") into a structured
// explanation with an actionable hint and a retry button.

import React, { useState } from 'react';
import { IconAlert, IconChevron } from '../../components/icons.js';

interface ResultErrorProps {
  readonly message: string;
  readonly onRetry?: () => void;
}

type ErrorKind = 'server' | 'query' | 'auth' | 'network' | 'timeRange' | 'empty' | 'unknown';

interface ErrorMeta {
  readonly title: string;
  readonly hint: string;
}

function classify(message: string): ErrorKind {
  const m = message.toLowerCase();
  if (m.includes('empty query')) return 'empty';
  if (m.includes('to must be later than from')) return 'timeRange';
  if (m.includes('401') || m.includes('403') || m.includes('unauthorized') || m.includes('forbidden') || m.includes('unauthenticated')) return 'auth';
  if (m.includes('fetch') || m.includes('network') || m.includes('failed to fetch') || m.includes('connection') || m.includes('ENOTFOUND')) return 'network';
  if (m.includes('400') || m.includes('bad request') || m.includes('cannot parse') || m.includes('syntax error') || m.includes('invalid')) return 'query';
  if (m.includes('500') || m.includes('502') || m.includes('503') || m.includes('504') || m.includes('upstream returned')) return 'server';
  return 'unknown';
}

function meta(kind: ErrorKind): ErrorMeta {
  switch (kind) {
    case 'empty':
      return {
        title: 'No query to run',
        hint: 'Build a query in the builder or write one in the code editor, then click Run.',
      };
    case 'timeRange':
      return {
        title: 'Invalid time range',
        hint: 'Set the TO timestamp to be later than the FROM timestamp.',
      };
    case 'auth':
      return {
        title: 'Access denied',
        hint: 'Your session may have expired or you may lack permission. Try logging in again.',
      };
    case 'network':
      return {
        title: 'Connection error',
        hint: 'Could not reach the server. Check your network and make sure the BanyanDB cluster is reachable.',
      };
    case 'query':
      return {
        title: 'Query error',
        hint: 'The query could not be parsed or validated. Review the syntax and the selected resource.',
      };
    case 'server':
      return {
        title: 'Server error',
        hint: 'The BanyanDB cluster or BFF returned an error. Wait a moment and try again, or check the cluster health.',
      };
    default:
      return {
        title: 'Query failed',
        hint: 'Something went wrong while running the query. Check the details below or try again.',
      };
  }
}

export function ResultError({ message, onRetry }: ResultErrorProps) {
  const [showDetails, setShowDetails] = useState(false);
  const kind = classify(message);
  const { title, hint } = meta(kind);

  return (
    <div className="empty rv-error">
      <span className="empty-ico rv-error-ico">
        <IconAlert size={28} />
      </span>
      <p className="empty-title rv-error-title">{title}</p>
      <p className="empty-text rv-error-hint">{hint}</p>
      <div className="rv-error-msg">
        <span className="rv-error-label">Error</span>
        <code className="rv-error-code">{message}</code>
      </div>
      {onRetry && (
        <button type="button" className="btn btn-primary" onClick={onRetry}>
          <span aria-hidden="true">↻</span> Retry
        </button>
      )}
      <button
        type="button"
        className="rv-error-details-btn"
        onClick={() => setShowDetails((v) => !v)}
        aria-expanded={showDetails}
      >
        <IconChevron size={11} />
        {showDetails ? 'Hide technical details' : 'Show technical details'}
      </button>
      {showDetails && (
        <div className="rv-error-details">
          <pre>{message}</pre>
        </div>
      )}
    </div>
  );
}
