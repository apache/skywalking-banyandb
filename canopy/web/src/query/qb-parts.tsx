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

// qb-parts.tsx — QB* building blocks shared between the main query builder
// (QueryBuilder.tsx) and the property-scoped query console (PropertyQuery.tsx).
// QBSection was previously private to QueryBuilder.tsx; lifted here per
// docs/property-design.md §0/§10 step 2 so PropertyQuery can reuse the same
// accordion-clause chrome without duplicating it. QueryBuilder.tsx now imports
// QBSection from here instead of defining it locally.

import React from 'react';

export interface QBSectionProps {
  readonly kw: string;
  readonly sum: string;
  readonly hint?: string;
  readonly optional?: boolean;
  readonly open: boolean;
  readonly acc: boolean;
  readonly onToggle: () => void;
  readonly children: React.ReactNode;
}

/** Clause wrapper: flat while composing, accordion row after the first run. */
export function QBSection({ kw, sum, hint, optional, open, acc, onToggle, children }: QBSectionProps) {
  if (!acc) {
    return (
      <div className="qb-section">
        <div className="qb-section-h" title={hint}>
          <span>{kw}</span>
          {optional && <span className="qb-opt">optional</span>}
        </div>
        {children}
      </div>
    );
  }
  return (
    <div className={'qb-acc' + (open ? ' is-open' : '')}>
      <button type="button" className="qb-acc-head" title={hint} aria-expanded={open} onClick={onToggle}>
        <span className="qb-acc-chev">{open ? '▾' : '▸'}</span>
        <span className="qb-kw">{kw}</span>
        {!open && <span className="qb-acc-sum mono">{sum}</span>}
        <span className="qb-acc-edit">{open ? 'collapse' : 'edit'}</span>
      </button>
      {open && <div className="qb-acc-body"><div className="qb-body">{children}</div></div>}
    </div>
  );
}

export interface QBChipsProps {
  readonly value: readonly string[];
  readonly options: readonly string[];
  readonly onChange: (next: readonly string[]) => void;
  /** Label for the "everything" sentinel chip (empty selection means "all"). */
  readonly allLabel?: string;
  /** Hide the "all" sentinel chip entirely. */
  readonly hideAll?: boolean;
  /** Text shown when `options` is empty. */
  readonly emptyLabel?: string;
}

/** Multi-select chip row with an "all" sentinel (empty selection). Ported
 *  from the handoff's QBChips (query-builder.jsx) — ORed into PropertyQuery's
 *  SELECT clause for tag projection. */
export function QBChips({ value, options, onChange, allLabel, hideAll, emptyLabel }: QBChipsProps) {
  const sel = value ?? [];
  const toggle = (n: string) => onChange(sel.includes(n) ? sel.filter((x) => x !== n) : [...sel, n]);
  if (!options.length) return <span className="qb-dim">{emptyLabel ?? 'no tags on this resource'}</span>;
  return (
    <div className="qb-chips">
      {!hideAll && (
        <button type="button" className={'qb-chip' + (sel.length === 0 ? ' is-on' : '')} onClick={() => onChange([])}>
          {allLabel ?? 'all'}
        </button>
      )}
      {options.map((n) => (
        <button
          key={n}
          type="button"
          className={'qb-chip mono' + (sel.includes(n) ? ' is-on' : '')}
          onClick={() => toggle(n)}
        >
          {n}
        </button>
      ))}
    </div>
  );
}
