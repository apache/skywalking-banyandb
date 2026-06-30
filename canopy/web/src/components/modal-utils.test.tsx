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

import React, { useState } from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { useFocusTrap, useDirtyGuard } from './modal-utils.js';

// ── Test harness: a modal that uses both hooks ──────────────────────────────

function Modal({ onEscape, onClose, isDirty }: {
  onEscape: () => void;
  onClose: () => void;
  isDirty: boolean;
}) {
  const ref = useFocusTrap(true, onEscape);
  const { guardedClose } = useDirtyGuard(isDirty, onClose);
  return (
    <div ref={ref} role="dialog" aria-label="test modal">
      <button>First</button>
      <input type="text" placeholder="middle" />
      <button>Last</button>
      <button onClick={guardedClose}>X</button>
    </div>
  );
}

// ── useFocusTrap ────────────────────────────────────────────────────────────

describe('useFocusTrap', () => {
  it('focuses the first focusable element on mount', async () => {
    render(<Modal onEscape={vi.fn()} onClose={vi.fn()} isDirty={false} />);
    await waitFor(() => {
      expect(screen.getByRole('button', { name: 'First' })).toHaveFocus();
    });
  });

  it('Tab on the last focusable element wraps to the first', () => {
    render(<Modal onEscape={vi.fn()} onClose={vi.fn()} isDirty={false} />);
    // The hook intercepts keydown on the document; use fireEvent so the
    // simulated Tab reaches our listener.
    const last = screen.getByRole('button', { name: 'X' });
    last.focus();
    expect(last).toHaveFocus();
    fireEvent.keyDown(document, { key: 'Tab' });
    expect(screen.getByRole('button', { name: 'First' })).toHaveFocus();
  });

  it('Shift+Tab on the first focusable element wraps to the last', async () => {
    const user = userEvent.setup();
    render(<Modal onEscape={vi.fn()} onClose={vi.fn()} isDirty={false} />);
    screen.getByRole('button', { name: 'First' }).focus();
    expect(screen.getByRole('button', { name: 'First' })).toHaveFocus();
    await user.tab({ shift: true });
    expect(screen.getByRole('button', { name: 'X' })).toHaveFocus();
  });

  it('Escape fires the onEscape callback', async () => {
    const onEscape = vi.fn();
    render(<Modal onEscape={onEscape} onClose={vi.fn()} isDirty={false} />);
    fireEvent.keyDown(document, { key: 'Escape' });
    expect(onEscape).toHaveBeenCalledOnce();
  });
});

// ── useDirtyGuard ───────────────────────────────────────────────────────────

describe('useDirtyGuard', () => {
  it('closes immediately when not dirty', () => {
    const onClose = vi.fn();
    render(<Modal onEscape={vi.fn()} onClose={onClose} isDirty={false} />);
    fireEvent.click(screen.getByRole('button', { name: 'X' }));
    expect(onClose).toHaveBeenCalledOnce();
  });

  it('asks for confirmation when dirty; cancels by default', () => {
    const onClose = vi.fn();
    const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(false);
    render(<Modal onEscape={vi.fn()} onClose={onClose} isDirty={true} />);
    fireEvent.click(screen.getByRole('button', { name: 'X' }));
    expect(confirmSpy).toHaveBeenCalled();
    expect(confirmSpy.mock.calls[0]?.[0]).toMatch(/unsaved changes/);
    expect(onClose).not.toHaveBeenCalled();
    confirmSpy.mockRestore();
  });

  it('closes when user confirms discard', () => {
    const onClose = vi.fn();
    const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(true);
    render(<Modal onEscape={vi.fn()} onClose={onClose} isDirty={true} />);
    fireEvent.click(screen.getByRole('button', { name: 'X' }));
    expect(onClose).toHaveBeenCalledOnce();
    confirmSpy.mockRestore();
  });
});

describe('useDirtyGuard — state transition', () => {
  it('subsequent closes are not blocked once confirmed (resetDirty path)', () => {
    // Mount with isDirty=true, confirm, then change to isDirty=false, click close → no second confirm.
    function Harness() {
      const [dirty, setDirty] = useState(true);
      const { guardedClose, resetDirty } = useDirtyGuard(dirty, () => setDirty(false));
      return (
        <div>
          <span>dirty={String(dirty)}</span>
          <button onClick={() => { resetDirty(); guardedClose(); }}>submitAndClose</button>
          <button onClick={guardedClose}>close</button>
        </div>
      );
    }
    const confirmSpy = vi.spyOn(window, 'confirm');
    render(<Harness />);
    fireEvent.click(screen.getByRole('button', { name: 'submitAndClose' }));
    // resetDirty sets confirmedRef → guardedClose fires onClose without prompting.
    expect(confirmSpy).not.toHaveBeenCalled();
    expect(screen.getByText('dirty=false')).toBeInTheDocument();
    confirmSpy.mockRestore();
  });
});
