/*
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Apache Software Foundation (ASF) to you under
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

// time.tsx — shared timestamp utilities + display components.
//
// One source of truth for parsing and formatting BanyanDB timestamps across
// the measure and trace result views. Replaces two near-identical copies of
// the s-vs-ms heuristic (raw > 1e12 ⇒ ms, else seconds) and the
// interval-aware format ladder.
//
// Pure (no DOM):
//   - parseTs handles number / numeric string / ISO-8601 string uniformly
//     and returns NaN for anything else.
//   - formatTs picks a granularity based on the optional intervalMs
//     (measure rollup windows: 30s, 1m, 5m, 1h, 1d) and a timezone that
//     defaults to 'local' (matches observer mental model) but can be 'utc'
//     for trace spans whose ISO representation is UTC.
//   - tipRows returns the rows the hover tooltip shows (ISO / Epoch / Local).
//
// Components share the same hover tip:
//   - <TimestampCell> renders a <td className="ts-cell"> for the measure table.
//   - <TimestampText> renders a <span className="slog-ts mono ts-text"> for the
//     trace row header (one column of the span inspector grid).
//   - useTimestampTooltip + TimestampTooltipPortal own the hover-delay logic
//     and the body-portal render so the tip escapes any overflow:auto container.

import React, { useCallback, useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';

/* ============================================================
 * Pure functions — testable in isolation, no DOM dependency.
 * ============================================================ */

/** Timezone for the rendered string. Trace data is natively UTC
 *  (W3C trace-context / OTLP) — pick `'utc'` to keep the displayed wall
 *  clock aligned with the ISO bytes. */
export type TimeZone = 'local' | 'utc';

/** Options for formatTs. intervalMs drives granularity only when present
 *  (its absence means "no schema-provided rollup interval"). */
export interface FormatOpts {
  readonly intervalMs?: number;
  readonly timezone?: TimeZone;
  /** Placeholder when ts is NaN. Defaults to em-dash for symmetry with
   *  Measure's table. Pass '' for trace rows that want a blank cell. */
  readonly empty?: string;
}

/** Parse a raw timestamp (number, numeric string, or ISO-8601 string) into
 *  milliseconds since epoch. Returns NaN on nullish / garbage input. */
export function parseTs(raw: unknown): number {
  if (raw == null) return NaN;
  if (typeof raw === 'number') return Number.isFinite(raw) ? (raw > 1e12 ? raw : raw * 1000) : NaN;
  if (typeof raw === 'string') {
    const trimmed = raw.trim();
    if (!trimmed) return NaN;
    const asNum = Number(trimmed);
    if (Number.isFinite(asNum)) return asNum > 1e12 ? asNum : asNum * 1000;
    const t = Date.parse(trimmed);
    return Number.isFinite(t) ? t : NaN;
  }
  return NaN;
}

/** Parse a measure interval string (e.g. "30s", "1m", "5m", "1h", "1d",
 *  "500ms", "100µs") into milliseconds. Returns undefined for missing,
 *  unrecognized, or zero intervals. */
export function parseIntervalMs(interval: string | undefined): number | undefined {
  if (!interval) return undefined;
  const match = interval.trim().match(/^(\d+(?:\.\d+)?)\s*(ns|us|µs|ms|s|m|h|d)$/i);
  if (!match) return undefined;
  const value = parseFloat(match[1]);
  const unit = match[2].toLowerCase();
  const multipliers: Record<string, number> = {
    ns: 1e-6,
    us: 1e-3,
    'µs': 1e-3,
    ms: 1,
    s: 1000,
    m: 60000,
    h: 3600000,
    d: 86400000,
  };
  const mult = multipliers[unit] ?? 0;
  const ms = value * mult;
  return Number.isFinite(ms) && ms > 0 ? ms : undefined;
}

const pad = (x: number) => String(x).padStart(2, '0');

/** Render a timestamp with interval-aware granularity. Pass
 *  { intervalMs } to pick the right precision for a measure rollup (so
 *  e.g. a 5-minute rollup renders HH:MM, not HH:MM:SS.mmm which would just
 *  be zeros). Leave intervalMs unset to render HH:MM:SS.mmm, which is the
 *  right default for raw trace spans. */
export function formatTs(ts: number, opts: FormatOpts = {}): string {
  const empty = opts.empty ?? '—';
  if (!Number.isFinite(ts)) return empty;
  const d = new Date(ts);
  const tz = opts.timezone ?? 'local';
  const hh = tz === 'utc' ? pad(d.getUTCHours()) : pad(d.getHours());
  const mi = tz === 'utc' ? pad(d.getUTCMinutes()) : pad(d.getMinutes());
  const ss = tz === 'utc' ? pad(d.getUTCSeconds()) : pad(d.getSeconds());
  const mmm = tz === 'utc'
    ? String(d.getUTCMilliseconds()).padStart(3, '0')
    : String(d.getMilliseconds()).padStart(3, '0');
  const yyyy = tz === 'utc' ? d.getUTCFullYear() : d.getFullYear();
  const mo = pad((tz === 'utc' ? d.getUTCMonth() : d.getMonth()) + 1);
  const dd = pad(tz === 'utc' ? d.getUTCDate() : d.getDate());

  const intervalMs = opts.intervalMs;
  if (!intervalMs) return `${hh}:${mi}:${ss}.${mmm}`;
  if (intervalMs < 1000) return `${hh}:${mi}:${ss}.${mmm}`;
  if (intervalMs < 60000) return `${hh}:${mi}:${ss}`;
  if (intervalMs < 3600000) return `${hh}:${mi}`;
  if (intervalMs < 86400000) return `${yyyy}-${mo}-${dd} ${hh}:00`;
  return `${yyyy}-${mo}-${dd}`;
}

/** Rows for the hover tooltip. Always ISO + Epoch + Local regardless of
 *  source timezone — the comparison is what the user wants when "when was
 *  this span really". */
export interface TipRow {
  readonly label: string;
  readonly value: string;
}

export function tipRows(ts: number): readonly TipRow[] {
  if (!Number.isFinite(ts)) return [];
  const d = new Date(ts);
  return [
    { label: 'ISO', value: d.toISOString() },
    { label: 'Epoch', value: ts.toLocaleString('en-US') },
    { label: 'Local', value: d.toLocaleString('en-US') },
  ];
}

/* ============================================================
 * Components — share the same hover behavior via useTimestampTooltip.
 * ============================================================ */

interface TipPos {
  readonly left: number;
  readonly top: number;
}

const HOVER_HIDE_DELAY_MS = 120;

interface UseTimestampTooltipResult {
  readonly onMouseEnter: (e: React.MouseEvent) => void;
  readonly onMouseLeave: () => void;
  readonly onFocus: (e: React.FocusEvent) => void;
  readonly onBlur: () => void;
  readonly tipProps: {
    readonly show: boolean;
    readonly style: React.CSSProperties;
  };
}

/** Hover handlers + tip positioning for the cell. ~120ms hide delay so
 *  the user can move the mouse from the cell onto the tip and copy text. */
export function useTimestampTooltip(): UseTimestampTooltipResult {
  const [pos, setPos] = useState<TipPos | null>(null);
  const hideTimer = useRef<number | null>(null);

  const cancelHide = useCallback(() => {
    if (hideTimer.current != null) {
      window.clearTimeout(hideTimer.current);
      hideTimer.current = null;
    }
  }, []);

  const scheduleHide = useCallback(() => {
    cancelHide();
    hideTimer.current = window.setTimeout(() => setPos(null), HOVER_HIDE_DELAY_MS);
  }, [cancelHide]);

  const show = useCallback((clientX: number, clientY: number, rect: DOMRect | undefined) => {
    cancelHide();
    if (rect) {
      setPos({
        left: rect.left + rect.width / 2,
        top: rect.top - 8,
      });
    } else {
      setPos({ left: clientX, top: clientY - 8 });
    }
  }, [cancelHide]);

  useEffect(() => () => cancelHide(), [cancelHide]);

  const handleEnter = useCallback((e: React.MouseEvent) => {
    const rect = (e.currentTarget as HTMLElement).getBoundingClientRect();
    show(e.clientX, e.clientY, rect);
  }, [show]);

  const handleFocus = useCallback((e: React.FocusEvent) => {
    const rect = (e.currentTarget as HTMLElement).getBoundingClientRect();
    show(0, 0, rect);
  }, [show]);

  return {
    onMouseEnter: handleEnter,
    onMouseLeave: scheduleHide,
    onFocus: handleFocus,
    onBlur: scheduleHide,
    tipProps: {
      show: pos != null,
      style: pos
        ? {
            position: 'fixed',
            left: pos.left,
            top: pos.top,
            transform: 'translate(-50%, -100%)',
          }
        : {},
    },
  };
}

interface PortalProps {
  readonly show: boolean;
  readonly style: React.CSSProperties;
  readonly ts: number;
  /** When true (default) the tip stays open on hover so the user can copy
   *  ISO / epoch strings. When false, the tip closes the moment the cell
   *  loses hover. */
  readonly interactive?: boolean;
  readonly onMouseEnter?: () => void;
  readonly onMouseLeave?: () => void;
}

/** Body-portal tip. Rendered conditionally (null when hidden) so it
 *  doesn't contribute to the DOM when inactive. */
export function TimestampTooltipPortal({ show, style, ts, interactive = true, onMouseEnter, onMouseLeave }: PortalProps) {
  if (!show || typeof document === 'undefined') return null;
  const rows = tipRows(ts);
  if (rows.length === 0) return null;
  return createPortal(
    <span
      className="ts-tip"
      style={style}
      onMouseEnter={interactive ? onMouseEnter : undefined}
      onMouseLeave={interactive ? onMouseLeave : undefined}
    >
      {rows.map((row) => (
        <span key={row.label} className="ts-tip-row">
          <b>{row.label}</b> {row.value}
        </span>
      ))}
    </span>,
    document.body,
  );
}

interface CellProps {
  readonly ts: number;
  readonly opts?: FormatOpts;
  /** Extra classNames appended to `mono dim ts-cell`. */
  readonly className?: string;
}

/** <td> cell with ISO + epoch hover tooltip. Used in the measure table. */
export function TimestampCell({ ts, opts, className }: CellProps) {
  const tip = useTimestampTooltip();
  const cellRef = useRef<HTMLTableCellElement | null>(null);
  const hideTimer = useRef<number | null>(null);
  const cancelHide = () => {
    if (hideTimer.current != null) {
      window.clearTimeout(hideTimer.current);
      hideTimer.current = null;
    }
  };
  const scheduleHide = () => {
    cancelHide();
    hideTimer.current = window.setTimeout(tip.onMouseLeave, HOVER_HIDE_DELAY_MS);
  };
  useEffect(() => () => cancelHide(), []);

  return (
    <>
      <td
        ref={cellRef}
        className={`mono dim ts-cell${className ? ` ${className}` : ''}`}
        onMouseEnter={tip.onMouseEnter}
        onMouseLeave={scheduleHide}
        onFocus={tip.onFocus}
        onBlur={tip.onBlur}
        tabIndex={0}
      >
        {formatTs(ts, opts)}
      </td>
      <TimestampTooltipPortal
        show={tip.tipProps.show}
        style={tip.tipProps.style}
        ts={ts}
        onMouseEnter={cancelHide}
        onMouseLeave={scheduleHide}
      />
    </>
  );
}

interface TextProps {
  readonly ts: number;
  readonly opts?: FormatOpts;
  /** Extra classNames appended to `slog-ts mono ts-text`. */
  readonly className?: string;
  /** Default true — disables the hover-tip entirely (for dense lists where
   *  the portal per row would be visually noisy). */
  readonly withTip?: boolean;
}

/** Inline span with optional ISO + epoch hover tooltip. Used in the trace
 *  row header (the timestamp column of the span inspector grid). */
export function TimestampText({ ts, opts, className, withTip = true }: TextProps) {
  const tip = useTimestampTooltip();
  const hideTimer = useRef<number | null>(null);
  const cancelHide = () => {
    if (hideTimer.current != null) {
      window.clearTimeout(hideTimer.current);
      hideTimer.current = null;
    }
  };
  const scheduleHide = () => {
    cancelHide();
    hideTimer.current = window.setTimeout(tip.onMouseLeave, HOVER_HIDE_DELAY_MS);
  };
  useEffect(() => () => cancelHide(), []);

  const onEnter = withTip ? tip.onMouseEnter : undefined;
  const onLeave = withTip ? scheduleHide : undefined;
  const onFocusEv = withTip ? tip.onFocus : undefined;
  const onBlurEv = withTip ? tip.onBlur : undefined;

  return (
    <>
      <span
        className={`slog-ts mono${withTip ? ' ts-text' : ''}${className ? ` ${className}` : ''}`}
        onMouseEnter={onEnter}
        onMouseLeave={onLeave}
        onFocus={onFocusEv}
        onBlur={onBlurEv}
        tabIndex={withTip ? 0 : undefined}
      >
        {formatTs(ts, opts)}
      </span>
      {withTip && (
        <TimestampTooltipPortal
          show={tip.tipProps.show}
          style={tip.tipProps.style}
          ts={ts}
          onMouseEnter={cancelHide}
          onMouseLeave={scheduleHide}
        />
      )}
    </>
  );
}
