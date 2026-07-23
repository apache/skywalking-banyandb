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

// CopyableId.tsx — short-ID cell with hover-popover + click-to-copy.
//
// Renders a fixed-width inline span whose overflow is ellipsised by CSS,
// but on hover pops a body-ported panel showing the FULL value (selectable
// text) plus a Copy button. A click anywhere on the cell copies the value
// to the clipboard and flips the popover to "✓ Copied" for ~1.2 s.
//
// Triggering:
//   - Hover the cell              → open popover
//   - Click the cell              → copy + flip popover to ✓ Copied
//   - Click "Copy" in the popover → same as click (with stopPropagation
//                                   so the popover's mousedown doesn't
//                                   re-open + re-fire on the cell)
//   - Keyboard: Tab to focus,     → open popover
//              Enter or Space       → copy
//   - 120 ms hide delay on leave  → so the user can move onto the popover
//                                   itself to highlight/copy partial text
//
// The trigger click handler stopPropagation()s to keep parent row onClick
// handlers (e.g. the trace row's expand toggle) from firing on an ID click.

import React, { useCallback, useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';

interface Props {
  /** Full ID text (e.g. '4e01fcd97f514efab60ef4b462a6389b'). */
  readonly value: string;
  /** Label shown in the popover header (e.g. 'trace_id'). */
  readonly label: string;
  /** Extra className for the inline truncated span (e.g. 'tin-tid'). */
  readonly className?: string;
  /** Optional title for the native browser tooltip fallback. */
  readonly nativeTitle?: string;
  /** Popover width in px; defaults to 380 to match the bytes panel. */
  readonly popoverWidth?: number;
}

const HOVER_HIDE_DELAY_MS = 120;
const COPIED_FLASH_MS = 1200;

interface TipPos {
  readonly left: number;
  readonly top: number;
}

function copyToClipboard(text: string): boolean {
  try {
    if (typeof navigator !== 'undefined' && navigator.clipboard) {
      navigator.clipboard.writeText(text).catch(() => { /* ignore */ });
      return true;
    }
  } catch { /* ignore */ }
  try {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    document.execCommand('copy');
    document.body.removeChild(ta);
    return true;
  } catch {
    return false;
  }
}

export function CopyableId({ value, label, className, nativeTitle, popoverWidth = 380 }: Props) {
  const [open, setOpen] = useState(false);
  const [pos, setPos] = useState<TipPos | null>(null);
  const [copied, setCopied] = useState(false);
  const spanRef = useRef<HTMLSpanElement | null>(null);
  const hideTimer = useRef<number | null>(null);
  const copyTimer = useRef<number | null>(null);

  const cancelHide = useCallback(() => {
    if (hideTimer.current != null) {
      window.clearTimeout(hideTimer.current);
      hideTimer.current = null;
    }
  }, []);

  const scheduleHide = useCallback(() => {
    cancelHide();
    hideTimer.current = window.setTimeout(() => setOpen(false), HOVER_HIDE_DELAY_MS);
  }, [cancelHide]);

  const show = useCallback(() => {
    cancelHide();
    const rect = spanRef.current?.getBoundingClientRect();
    if (rect) {
      const left = Math.max(
        12,
        Math.min(rect.left + rect.width / 2, window.innerWidth - popoverWidth / 2 - 12),
      );
      setPos({ left, top: rect.top - 8 });
    }
    setOpen(true);
  }, [cancelHide, popoverWidth]);

  useEffect(() => () => {
    cancelHide();
    if (copyTimer.current != null) window.clearTimeout(copyTimer.current);
  }, [cancelHide]);

  const handleCopy = useCallback(
    (e: React.SyntheticEvent) => {
      e.stopPropagation();
      e.preventDefault();
      if (!value) return;
      if (copyToClipboard(value)) {
        setCopied(true);
        if (copyTimer.current != null) window.clearTimeout(copyTimer.current);
        copyTimer.current = window.setTimeout(() => setCopied(false), COPIED_FLASH_MS);
      }
    },
    [value],
  );

  const handleKey = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === 'Enter' || e.key === ' ') {
        handleCopy(e);
      }
    },
    [handleCopy],
  );

  const tipContents = open && pos && typeof document !== 'undefined'
    ? createPortal(
        <span
          className="ti-id-tip"
          style={{
            position: 'fixed',
            left: pos.left,
            top: pos.top,
            transform: 'translate(-50%, -100%)',
            width: popoverWidth,
          }}
          onMouseEnter={cancelHide}
          onMouseLeave={scheduleHide}
        >
          <span className="ti-id-head">
            <span className="ti-id-label mono">{label}</span>
            <button
              type="button"
              className={'ti-id-copy' + (copied ? ' is-on' : '')}
              onClick={handleCopy}
              onMouseDown={(e) => e.stopPropagation()}
              title={copied ? 'Copied' : `Copy ${label}`}
            >
              {copied ? '✓ Copied' : 'Copy'}
            </button>
          </span>
          <span className="ti-id-value mono">{value}</span>
        </span>,
        document.body,
      )
    : null;

  return (
    <>
      <span
        ref={spanRef}
        className={`mono ti-id-cell${className ? ` ${className}` : ''}`}
        title={nativeTitle ?? `${label} = ${value}`}
        onMouseEnter={show}
        onMouseLeave={scheduleHide}
        onFocus={show}
        onBlur={scheduleHide}
        onClick={handleCopy}
        onKeyDown={handleKey}
        tabIndex={0}
        role="button"
        aria-label={`${label} ${value}, click to copy`}
      >
        {value}
      </span>
      {tipContents}
    </>
  );
}
