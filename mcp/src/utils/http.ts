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
import { log } from './logger.js';

const Timeout = 2 * 60 * 1000;

export let globalAbortController = new AbortController();

export function abortRequestsAndUpdate() {
  globalAbortController.abort(`Request timeout ${Timeout}ms`);
  globalAbortController = new AbortController();
}

export async function httpFetch({
  url = '',
  method = 'GET',
  json,
  headers = {},
}: {
  method: string;
  json: unknown;
  headers?: Record<string, string>;
  url: string;
}) {
  const timeoutId = setTimeout(() => {
    abortRequestsAndUpdate();
  }, Timeout);

  // Only include body and Content-Type for requests that have a body
  const hasBody = json !== null && json !== undefined && method !== 'GET' && method !== 'HEAD';
  const requestHeaders: Record<string, string> = { ...headers };
  if (hasBody) {
    requestHeaders['Content-Type'] = 'application/json';
  }

  const response: Response = await fetch(url, {
    method,
    headers: requestHeaders,
    body: hasBody ? JSON.stringify(json) : undefined,
    signal: globalAbortController.signal,
  })
    .finally(() => {
      clearTimeout(timeoutId);
    });
  if (response.ok) {
    return response.json();
  } else {
    log.error('HTTP fetch failed:', response);
    return {
      errors: response,
    };
  }
}
