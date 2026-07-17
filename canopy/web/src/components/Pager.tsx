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

import React, { useMemo, useState, useEffect } from 'react';

/** Default page size across the resource and index tables. */
export const DEFAULT_PAGE_SIZE = 50;

/** Pager renders nothing when total <= pageSize. */
export function Pager({
  total, pageSize, page, onPageChange, label,
}: {
  total: number;
  pageSize: number;
  page: number;
  onPageChange: (next: number) => void;
  /** Optional label for the total count, e.g. "rules" / "measures". */
  label?: string;
}) {
  const lastPage = Math.max(1, Math.ceil(total / pageSize));
  if (total <= pageSize) return null;

  return (
    <div className="doc-pager">
      <span>
        {total}
        {label ? ` ${label}` : ''}
      </span>
      <div className="doc-pager-btns">
        <button
          className="pg-btn"
          disabled={page <= 1}
          onClick={() => onPageChange(page - 1)}
        >
          ← Prev
        </button>
        <span className="doc-pager-page" data-testid="pager-page">{page} / {lastPage}</span>
        <button
          className="pg-btn"
          disabled={page >= lastPage}
          onClick={() => onPageChange(page + 1)}
        >
          Next →
        </button>
      </div>
    </div>
  );
}

/**
 * usePagedList — client-side pagination for a fully-fetched list.
 *
 * Returns the current page slice plus the controls the caller needs to wire
 * up `<Pager>`. The caller is responsible for resetting `page` to 1 when the
 * underlying filter (e.g. search box) changes — see `useResetPage` below.
 */
export function usePagedList<T>(items: readonly T[], pageSize: number = DEFAULT_PAGE_SIZE) {
  const [page, setPage] = useState(1);

  // When `items` shrinks below the current page (e.g. user filtered down),
  // snap back to page 1 so the empty-state branches in the host page can fire.
  useEffect(() => {
    const lastPage = Math.max(1, Math.ceil(items.length / pageSize));
    if (page > lastPage) setPage(1);
  }, [items.length, pageSize, page]);

  const pageItems = useMemo(
    () => items.slice((page - 1) * pageSize, page * pageSize),
    [items, page, pageSize],
  );

  return {
    page,
    setPage,
    pageItems,
    total: items.length,
    lastPage: Math.max(1, Math.ceil(items.length / pageSize)),
  };
}

/** Reset page to 1 whenever `key` changes. */
export function useResetPage(setPage: (n: number) => void, key: unknown): void {
  useEffect(() => {
    setPage(1);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);
}