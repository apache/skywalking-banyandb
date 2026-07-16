/*
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Apache Software Foundation (ASF to you under
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

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, cleanup, waitFor, act } from '@testing-library/react';
import { CopyableId } from './CopyableId.js';

describe('CopyableId', () => {
  beforeEach(() => {
    // jsdom doesn't ship a clipboard API; stub the modern path so we can
    // verify writes land there. Fall back via document.execCommand is
    // covered by the e2e probe — unit tests focus on the modern path.
    Object.assign(navigator, {
      clipboard: { writeText: vi.fn().mockResolvedValue(undefined) },
    });
  });

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  it('renders the truncated value inline and exposes a native title for hover preview', () => {
    render(<CopyableId value="4e01fcd97f514efab60ef4b462a6389b" label="trace_id" />);
    const cell = screen.getByRole('button', { name: /trace_id .*, click to copy/i });
    expect(cell).toBeInTheDocument();
    expect(cell).toHaveTextContent('4e01fcd97f514efab60ef4b462a6389b');
    expect(cell).toHaveAttribute('title', 'trace_id = 4e01fcd97f514efab60ef4b462a6389b');
  });

  it('hides the popover by default', () => {
    render(<CopyableId value="abcdef" label="trace_id" />);
    expect(document.querySelector('.ti-id-tip')).toBeNull();
  });

  it('opens the popover on hover and shows the full value + a Copy button', async () => {
    render(<CopyableId value="4e01fcd97f514efab60ef4b462a6389b" label="trace_id" />);
    const cell = screen.getByRole('button');
    fireEvent.mouseEnter(cell);
    await waitFor(() => {
      expect(document.querySelector('.ti-id-tip')).not.toBeNull();
    });
    const tip = document.querySelector('.ti-id-tip');
    expect(tip?.querySelector('.ti-id-label')?.textContent).toBe('trace_id');
    expect(tip?.querySelector('.ti-id-value')?.textContent).toBe('4e01fcd97f514efab60ef4b462a6389b');
    expect(tip?.querySelector('button.ti-id-copy')?.textContent?.trim()).toBe('Copy');
  });

  it('closes the popover ~120ms after mouseleave', () => {
    vi.useFakeTimers();
    render(<CopyableId value="abc" label="trace_id" />);
    const cell = screen.getByRole('button');
    fireEvent.mouseEnter(cell);
    expect(document.querySelector('.ti-id-tip')).not.toBeNull();
    fireEvent.mouseLeave(cell);
    // before the timer fires, tip still mounted
    expect(document.querySelector('.ti-id-tip')).not.toBeNull();
    act(() => { vi.advanceTimersByTime(150); });
    expect(document.querySelector('.ti-id-tip')).toBeNull();
  });

  it('keeps the popover open when the mouse moves from cell to tip', async () => {
    render(<CopyableId value="abc" label="trace_id" />);
    const cell = screen.getByRole('button');
    fireEvent.mouseEnter(cell);
    await waitFor(() => {
      expect(document.querySelector('.ti-id-tip')).not.toBeNull();
    });
    const tip = document.querySelector('.ti-id-tip')!;
    fireEvent.mouseLeave(cell);
    fireEvent.mouseEnter(tip);
    // tip stays open because the user's mouse is still over it
    await new Promise((r) => setTimeout(r, 150));
    expect(document.querySelector('.ti-id-tip')).not.toBeNull();
  });

  it('copies the value when the cell is clicked and flips the copy button to ✓ Copied', () => {
    vi.useFakeTimers();
    render(<CopyableId value="4e01fcd97f514efab60ef4b462a6389b" label="trace_id" />);
    const cell = screen.getByRole('button');
    fireEvent.mouseEnter(cell);
    fireEvent.click(cell);
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith('4e01fcd97f514efab60ef4b462a6389b');
    expect(document.querySelector('.ti-id-copy.is-on')?.textContent?.trim()).toBe('✓ Copied');
    act(() => { vi.advanceTimersByTime(1300); });
    expect(document.querySelector('.ti-id-copy.is-on')).toBeNull();
  });

  it('stopPropagation on the cell click so a parent row onClick does not fire', () => {
    const rowClick = vi.fn();
    render(
      <div onClick={rowClick}>
        <CopyableId value="abc" label="trace_id" />
      </div>,
    );
    fireEvent.click(screen.getByRole('button'));
    expect(rowClick).not.toHaveBeenCalled();
  });

  it('triggers copy via keyboard (Enter or Space)', () => {
    render(<CopyableId value="4e01fcd97f514efab60ef4b462a6389b" label="trace_id" />);
    const cell = screen.getByRole('button');
    fireEvent.keyDown(cell, { key: 'Enter' });
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith('4e01fcd97f514efab60ef4b462a6389b');
    fireEvent.keyDown(cell, { key: ' ' });
    expect(navigator.clipboard.writeText).toHaveBeenCalledTimes(2);
  });

  it('does nothing on click when the value is empty', () => {
    render(<CopyableId value="" label="trace_id" />);
    fireEvent.click(screen.getByRole('button'));
    expect(navigator.clipboard.writeText).not.toHaveBeenCalled();
  });
});
