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
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';

import { Pager } from './Pager.js';

describe('Pager', () => {
  it('renders nothing when total <= pageSize', () => {
    const { container } = render(
      <Pager total={10} pageSize={50} page={1} onPageChange={vi.fn()} />,
    );
    expect(container.firstChild).toBeNull();
  });

  it('renders "1 / 2" on the first page with Prev disabled', () => {
    render(<Pager total={100} pageSize={50} page={1} onPageChange={vi.fn()} label="rules" />);
    expect(screen.getByText('100 rules')).toBeInTheDocument();
    expect(screen.getByText('1 / 2')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Prev/i })).toBeDisabled();
    expect(screen.getByRole('button', { name: /Next/i })).not.toBeDisabled();
  });

  it('invokes onPageChange(2) when Next is clicked', () => {
    const onPageChange = vi.fn();
    render(<Pager total={100} pageSize={50} page={1} onPageChange={onPageChange} />);
    fireEvent.click(screen.getByRole('button', { name: /Next/i }));
    expect(onPageChange).toHaveBeenCalledWith(2);
  });

  it('invokes onPageChange(page - 1) when Prev is clicked', () => {
    const onPageChange = vi.fn();
    render(<Pager total={100} pageSize={50} page={2} onPageChange={onPageChange} />);
    fireEvent.click(screen.getByRole('button', { name: /Prev/i }));
    expect(onPageChange).toHaveBeenCalledWith(1);
  });

  it('disables Next on the last page', () => {
    render(<Pager total={100} pageSize={50} page={2} onPageChange={vi.fn()} />);
    expect(screen.getByText('2 / 2')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Next/i })).toBeDisabled();
    expect(screen.getByRole('button', { name: /Prev/i })).not.toBeDisabled();
  });

  it('omits the label suffix when label prop is omitted', () => {
    render(<Pager total={75} pageSize={50} page={1} onPageChange={vi.fn()} />);
    expect(screen.getByText('75')).toBeInTheDocument();
  });
});