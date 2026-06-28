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

import React, { useEffect, useId, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';

/** fuzzyScore — rank how well `target` matches `query` for fuzzy filter.
 * Higher = better match. 0 means no match. */
export function fuzzyScore(query: string, target: string): number {
  if (!query) return 1;
  const q = query.toLowerCase();
  const t = target.toLowerCase();
  if (t === q) return 1000;
  if (t.startsWith(q)) return 500;
  const idx = t.indexOf(q);
  if (idx >= 0) return 250 - idx;
  // Subsequence match.
  let qi = 0;
  let lastMatch = -1;
  for (let ti = 0; ti < t.length && qi < q.length; ti++) {
    if (t[ti] === q[qi]) {
      qi++;
      lastMatch = ti;
    }
  }
  if (qi < q.length) return 0;
  // Reward tight clusters of matched characters.
  return 100 - (lastMatch - q.length + 1);
}

/** fuzzyFilter — rank and filter an option list by query. */
export function fuzzyFilter(query: string, options: ReadonlyArray<string>): string[] {
  const scored = options
    .map((o) => ({ o, s: fuzzyScore(query, o) }))
    .filter((x) => x.s > 0);
  scored.sort((a, b) => b.s - a.s || a.o.localeCompare(b.o));
  return scored.map((x) => x.o);
}

// Highlight matched characters in `text` using <mark> nodes. Non-matched runs
// are buffered into a single text node so the accessible-name computation
// (which inserts whitespace between separate text nodes) doesn't fragment the
// option label.
function highlightMatch(text: string, query: string): React.ReactNode {
  if (!query) return text;
  const lower = text.toLowerCase();
  const q = query.toLowerCase();
  const idx = lower.indexOf(q);
  if (idx >= 0) {
    return (
      <>
        {text.slice(0, idx)}
        <mark className="f-combo-hl">{text.slice(idx, idx + q.length)}</mark>
        {text.slice(idx + q.length)}
      </>
    );
  }
  // Subsequence highlight — group consecutive non-matched characters into
  // one text node so the option label reads as a single string.
  const out: React.ReactNode[] = [];
  let qi = 0;
  let buffer = '';
  const flushBuffer = () => {
    if (buffer) { out.push(buffer); buffer = ''; }
  };
  for (let ti = 0; ti < text.length; ti++) {
    if (qi < q.length && text[ti].toLowerCase() === q[qi]) {
      flushBuffer();
      out.push(<mark key={ti} className="f-combo-hl">{text[ti]}</mark>);
      qi++;
    } else {
      buffer += text[ti];
    }
  }
  flushBuffer();
  return out;
}

interface BaseProps {
  options: ReadonlyArray<string>;
  placeholder?: string;
  disabled?: boolean;
  /** Shown when the option list itself is empty. */
  noOptionsHint?: string;
  /** Shown when the filter eliminates all options. */
  emptyHint?: string;
  className?: string;
  ariaLabel?: string;
  id?: string;
}

interface SingleProps extends BaseProps {
  value: string;
  onChange: (v: string) => void;
}

interface MultiProps extends BaseProps {
  value: string[];
  onChange: (v: string[]) => void;
}

// Reusable dropdown panel. Tracks its anchor via the parent-provided rect.
function DropdownPanel({
  popRef, rect, children,
}: {
  popRef: React.RefObject<HTMLDivElement>;
  rect: { left: number; top: number; width: number };
  children: React.ReactNode;
}) {
  return createPortal(
    <div ref={popRef} className="f-combo-pop"
      style={{ left: rect.left, top: rect.top, width: rect.width }}>
      {children}
    </div>,
    document.body,
  );
}

function usePopoverPosition(open: boolean, anchorRef: React.RefObject<HTMLElement>): {
  rect: { left: number; top: number; width: number } | null;
} {
  const [rect, setRect] = useState<{ left: number; top: number; width: number } | null>(null);
  useEffect(() => {
    if (!open) { setRect(null); return; }
    const update = () => {
      const r = anchorRef.current?.getBoundingClientRect();
      if (r) setRect({ left: r.left, top: r.bottom + 4, width: r.width });
    };
    update();
    window.addEventListener('scroll', update, true);
    window.addEventListener('resize', update);
    return () => {
      window.removeEventListener('scroll', update, true);
      window.removeEventListener('resize', update);
    };
  }, [open, anchorRef]);
  return { rect };
}

// Reusable outside-click + Escape-close behavior for the combobox.
function useDismiss(open: boolean, onClose: () => void, wrapRef: React.RefObject<HTMLElement>, popRef: React.RefObject<HTMLElement>) {
  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (wrapRef.current?.contains(e.target as Node)) return;
      if (popRef.current?.contains(e.target as Node)) return;
      onClose();
    };
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    document.addEventListener('mousedown', onDown);
    document.addEventListener('keydown', onKey);
    return () => {
      document.removeEventListener('mousedown', onDown);
      document.removeEventListener('keydown', onKey);
    };
  }, [open, onClose, wrapRef, popRef]);
}

/** Single-select combobox with fuzzy filter and keyboard navigation. */
export function Combobox({
  value, options, onChange, placeholder, disabled, noOptionsHint, emptyHint, className, ariaLabel, id,
}: SingleProps) {
  const inputId = id ?? useId();
  const listboxId = `${inputId}-list`;
  const wrapRef = useRef<HTMLDivElement>(null);
  const popRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const [active, setActive] = useState(0);

  const filtered = useMemo(() => fuzzyFilter(query, options), [query, options]);
  const { rect } = usePopoverPosition(open, wrapRef);

  // Reset highlight when filter changes.
  useEffect(() => setActive(0), [query]);

  const close = () => setOpen(false);
  useDismiss(open, close, wrapRef, popRef);

  function selectOption(v: string) {
    onChange(v);
    setQuery('');
    setOpen(false);
    inputRef.current?.blur();
  }

  function onKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setOpen(true);
      setActive((a) => (filtered.length === 0 ? 0 : Math.min(a + 1, filtered.length - 1)));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setOpen(true);
      setActive((a) => Math.max(a - 1, 0));
    } else if (e.key === 'Enter') {
      if (open && filtered[active] != null) {
        e.preventDefault();
        selectOption(filtered[active]);
      }
    } else if (e.key === 'Tab') {
      setOpen(false);
    }
  }

  // Show the current selection (or empty) when the dropdown is closed; show the
  // user's query while they type so the input feels responsive.
  const displayValue = open ? query : (value || '');

  return (
    <div ref={wrapRef} className={'f-combo' + (className ? ' ' + className : '')}>
      <input
        ref={inputRef}
        id={inputId}
        type="text"
        role="combobox"
        className="f-input f-combo-input mono"
        value={displayValue}
        placeholder={placeholder}
        disabled={disabled}
        aria-expanded={open}
        aria-controls={listboxId}
        aria-autocomplete="list"
        aria-activedescendant={open && filtered[active] != null ? `${inputId}-opt-${active}` : undefined}
        aria-label={ariaLabel}
        autoComplete="off"
        spellCheck={false}
        onFocus={() => { setOpen(true); setQuery(''); }}
        onChange={(e) => { setQuery(e.target.value); setOpen(true); }}
        onKeyDown={onKeyDown}
      />
      <span className="f-combo-chev">▾</span>
      {open && rect && (
        <DropdownPanel popRef={popRef} rect={rect}>
          {options.length === 0 ? (
            <div className="f-combo-empty">{noOptionsHint ?? 'No options.'}</div>
          ) : filtered.length === 0 ? (
            <div className="f-combo-empty">{emptyHint ?? 'No matches.'}</div>
          ) : (
            <ul id={listboxId} role="listbox" className="f-combo-list">
              {filtered.map((opt, i) => (
                <li key={opt}
                  id={`${inputId}-opt-${i}`}
                  role="option"
                  aria-selected={opt === value}
                  aria-label={opt}
                  className={
                    'f-combo-option'
                    + (i === active ? ' is-highlight' : '')
                    + (opt === value ? ' is-selected' : '')
                  }
                  onMouseEnter={() => setActive(i)}
                  onMouseDown={(e) => e.preventDefault()}
                  onClick={() => selectOption(opt)}>
                  {highlightMatch(opt, query)}
                </li>
              ))}
            </ul>
          )}
        </DropdownPanel>
      )}
    </div>
  );
}

/** Multi-select combobox with fuzzy filter.
 * Selected items render as removable chips above the input; the input filters
 * the remaining (unselected) options. Backspace on an empty query removes the
 * last chip, matching common multi-select UX. */
export function MultiCombobox({
  value, options, onChange, placeholder, disabled, noOptionsHint, emptyHint, className, ariaLabel, id,
}: MultiProps) {
  const inputId = id ?? useId();
  const listboxId = `${inputId}-list`;
  const wrapRef = useRef<HTMLDivElement>(null);
  const popRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const [active, setActive] = useState(0);

  const available = useMemo(() => options.filter((o) => !value.includes(o)), [options, value]);
  const filtered = useMemo(() => fuzzyFilter(query, available), [query, available]);
  const { rect } = usePopoverPosition(open, wrapRef);

  useEffect(() => setActive(0), [query]);

  const close = () => setOpen(false);
  useDismiss(open, close, wrapRef, popRef);

  function toggleOption(v: string) {
    if (value.includes(v)) {
      onChange(value.filter((x) => x !== v));
    } else {
      onChange([...value, v]);
    }
    setQuery('');
    inputRef.current?.focus();
  }

  function removeChip(v: string) {
    onChange(value.filter((x) => x !== v));
  }

  function onKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setOpen(true);
      setActive((a) => (filtered.length === 0 ? 0 : Math.min(a + 1, filtered.length - 1)));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setOpen(true);
      setActive((a) => Math.max(a - 1, 0));
    } else if (e.key === 'Enter') {
      if (open && filtered[active] != null) {
        e.preventDefault();
        toggleOption(filtered[active]);
      }
    } else if (e.key === 'Tab') {
      setOpen(false);
    } else if (e.key === 'Backspace' && query === '' && value.length > 0) {
      e.preventDefault();
      onChange(value.slice(0, -1));
    }
  }

  return (
    <div ref={wrapRef} className={'f-combo f-combo-multi' + (className ? ' ' + className : '')}>
      {value.length > 0 && (
        <div className="f-combo-chips">
          {value.map((v) => (
            <button type="button" key={v} className="picker-chip is-on f-combo-chip"
              onClick={() => removeChip(v)}>
              <span className="mono">{v}</span>
              <span className="chip-x" aria-label={`Remove ${v}`}>×</span>
            </button>
          ))}
        </div>
      )}
      <div className="f-combo-inputwrap">
        <input
          ref={inputRef}
          id={inputId}
          type="text"
          role="combobox"
          className="f-input f-combo-input mono"
          value={query}
          placeholder={value.length === 0 ? placeholder : 'Add more…'}
          disabled={disabled}
          aria-expanded={open}
          aria-controls={listboxId}
          aria-autocomplete="list"
          aria-activedescendant={open && filtered[active] != null ? `${inputId}-opt-${active}` : undefined}
          aria-label={ariaLabel}
          autoComplete="off"
          spellCheck={false}
          onFocus={() => setOpen(true)}
          onChange={(e) => { setQuery(e.target.value); setOpen(true); }}
          onKeyDown={onKeyDown}
        />
        <span className="f-combo-chev">▾</span>
      </div>
      {open && rect && (
        <DropdownPanel popRef={popRef} rect={rect}>
          {options.length === 0 ? (
            <div className="f-combo-empty">{noOptionsHint ?? 'No options.'}</div>
          ) : available.length === 0 ? (
            <div className="f-combo-empty">All selected</div>
          ) : filtered.length === 0 ? (
            <div className="f-combo-empty">{emptyHint ?? 'No matches.'}</div>
          ) : (
            <ul id={listboxId} role="listbox" className="f-combo-list">
              {filtered.map((opt, i) => (
                <li key={opt}
                  id={`${inputId}-opt-${i}`}
                  role="option"
                  aria-selected={value.includes(opt)}
                  aria-label={opt}
                  className={
                    'f-combo-option'
                    + (i === active ? ' is-highlight' : '')
                    + (value.includes(opt) ? ' is-selected' : '')
                  }
                  onMouseEnter={() => setActive(i)}
                  onMouseDown={(e) => e.preventDefault()}
                  onClick={() => toggleOption(opt)}>
                  {highlightMatch(opt, query)}
                </li>
              ))}
            </ul>
          )}
        </DropdownPanel>
      )}
    </div>
  );
}
