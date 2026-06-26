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

import { useEffect, useRef, useState, useCallback } from 'react';

/**
 * useFocusTrap — Traps Tab focus within a container element.
 *
 * Behavior:
 *  - Tab on the last focusable element wraps to the first.
 *  - Shift+Tab on the first focusable element wraps to the last.
 *  - Escape fires the onEscape callback (modal close).
 *  - When active=false, the hook is a no-op.
 *
 * Returns a ref callback to attach to the modal container.
 */
export function useFocusTrap(
  active: boolean,
  onEscape: () => void,
): (node: HTMLElement | null) => void {
  const containerRef = useRef<HTMLElement | null>(null);

  const ref = useCallback((node: HTMLElement | null) => {
    containerRef.current = node;
    if (node && active) {
      // Focus the first focusable element inside the modal on mount.
      const first = node.querySelector<HTMLElement>(FOCUSABLE_SELECTOR);
      if (first) {
        // Defer to next frame so any layout-driven focus shifts settle first.
        requestAnimationFrame(() => first.focus());
      }
    }
  }, [active]);

  useEffect(() => {
    if (!active) return;
    const node = containerRef.current;
    if (!node) return;

    function onKeyDown(e: KeyboardEvent) {
      if (e.key === 'Escape') {
        e.preventDefault();
        onEscape();
        return;
      }
      if (e.key !== 'Tab') return;
      const focusables = node!.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR);
      if (focusables.length === 0) return;
      const first = focusables[0];
      const last = focusables[focusables.length - 1];
      const activeEl = document.activeElement as HTMLElement | null;
      if (e.shiftKey) {
        if (activeEl === first || !node!.contains(activeEl)) {
          e.preventDefault();
          last.focus();
        }
      } else {
        if (activeEl === last || !node!.contains(activeEl)) {
          e.preventDefault();
          first.focus();
        }
      }
    }

    document.addEventListener('keydown', onKeyDown);
    return () => document.removeEventListener('keydown', onKeyDown);
  }, [active, onEscape]);

  return ref;
}

const FOCUSABLE_SELECTOR = [
  'a[href]',
  'button:not([disabled])',
  'input:not([disabled]):not([type="hidden"])',
  'select:not([disabled])',
  'textarea:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
].join(',');

/**
 * useDirtyGuard — Tracks whether a form has unsaved changes.
 *
 * `isDirty` reflects the latest dirty state passed by the caller.
 * When the user tries to close the modal (via the onClose callback) while
 * dirty, the guard intercepts and asks for confirmation. If confirmed,
 * the close proceeds; if not, the modal stays open.
 *
 * The hook does not handle route changes — for those, wire a `beforeunload`
 * listener in the consuming component (kept out of the hook to keep side
 * effects explicit).
 */
export function useDirtyGuard(
  isDirty: boolean,
  onClose: () => void,
): { guardedClose: () => void; resetDirty: () => void } {
  // Confirmed intent to discard.
  const confirmedRef = useRef(false);
  // Snapshot of last-known dirty state (so callers can reset it after submit).
  const [, setLastDirty] = useState(isDirty);
  useEffect(() => { setLastDirty(isDirty); }, [isDirty]);

  const guardedClose = useCallback(() => {
    if (!isDirty || confirmedRef.current) {
      onClose();
      return;
    }
    const ok = window.confirm('You have unsaved changes. Discard them?');
    if (ok) {
      confirmedRef.current = true;
      onClose();
    }
  }, [isDirty, onClose]);

  const resetDirty = useCallback(() => {
    confirmedRef.current = true;
    setLastDirty(false);
  }, []);

  return { guardedClose, resetDirty };
}
