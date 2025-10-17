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

const Timeout = 2 * 60 * 1000;
export let globalAbortController = new AbortController();
export function abortRequestsAndUpdate() {
  globalAbortController.abort(`Request timeout ${Timeout}ms`);
  globalAbortController = new AbortController();
}
class HTTPError extends Error {
  response;

  constructor(response, detailText = '') {
    super(detailText || response.statusText);

    this.name = 'HTTPError';
    this.response = response;
  }
}

export const BasePath = `/graphql`;

export async function httpQuery({ url = '', method = 'GET', json, headers = {} }) {
  const timeoutId = setTimeout(() => {
    abortRequestsAndUpdate();
  }, Timeout);

  const response = await fetch(url, {
    method,
    headers: {
      'Content-Type': 'application/json',
      accept: 'application/json',
      ...headers,
    },
    body: JSON.stringify(json),
    signal: globalAbortController.signal,
  })
    .catch((error) => {
      throw new HTTPError(error);
    })
    .finally(() => {
      clearTimeout(timeoutId);
    });
  if (response.ok) {
    return response.json();
  } else {
    console.error(new HTTPError(response));
    return {
      error: new HTTPError(response),
    };
  }
}
