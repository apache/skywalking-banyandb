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

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { ResultError } from './ResultError.js';

describe('ResultError', () => {
  it('renders a friendly server-error state', () => {
    render(<ResultError message="Upstream returned 500" onRetry={() => {}} />);
    expect(screen.getByText('Server error')).toBeInTheDocument();
    expect(screen.getByText(/Upstream returned 500/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Retry/i })).toBeInTheDocument();
  });

  it('renders a query-error hint for syntax-like messages', () => {
    render(<ResultError message="Bad Request: cannot parse field xyz" />);
    expect(screen.getByText('Query error')).toBeInTheDocument();
  });

  it('renders an empty-query hint', () => {
    render(<ResultError message="Empty query." />);
    expect(screen.getByText('No query to run')).toBeInTheDocument();
  });

  it('calls onRetry when the retry button is clicked', () => {
    const retry = vi.fn();
    render(<ResultError message="Upstream returned 500" onRetry={retry} />);
    fireEvent.click(screen.getByRole('button', { name: /Retry/i }));
    expect(retry).toHaveBeenCalledTimes(1);
  });

  it('toggles technical details', () => {
    render(<ResultError message="Upstream returned 500" />);
    const toggle = screen.getByRole('button', { name: /Show technical details/i });
    fireEvent.click(toggle);
    expect(screen.getByRole('button', { name: /Hide technical details/i })).toBeInTheDocument();
  });
});
