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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

(() => {
  const storageKey = 'banyandb-native-index-spec-review-v2';
  const themeKey = 'banyandb-native-index-spec-theme';
  const documentedDisposition = {
    'DEC-001': 'Accept recommendation',
    'DEC-002': 'Accept recommendation',
    'DEC-003': 'Accept recommendation',
    'DEC-004': 'Accept recommendation',
    'DEC-005': 'Accept recommendation',
    'DEC-006': 'Accept recommendation',
    'DEC-007': 'Block specification',
    'DEC-008': 'Accept recommendation',
    'DEC-009': 'Accept recommendation',
    'DEC-010': 'Accept recommendation',
    'DEC-011': 'Accept recommendation'
  };
  const page = document.body.dataset.page;
  const movedSections = {
    fields: 'write-format.html#fields',
    terms: 'write-format.html#terms',
    write: 'write-format.html#write',
    ice: 'write-format.html#ice',
    snapshot: 'write-format.html#snapshot',
    query: 'query-lifecycle.html#query',
    sort: 'query-lifecycle.html#sort',
    lifecycle: 'query-lifecycle.html#lifecycle',
    merge: 'query-lifecycle.html#merge',
    external: 'query-lifecycle.html#external',
    integrity: 'safety-verification.html#integrity',
    compatibility: 'safety-verification.html#compatibility',
    verification: 'safety-verification.html#verification',
    rollout: 'delivery-review.html#rollout',
    operations: 'delivery-review.html#operations',
    decisions: 'delivery-review.html#decisions',
    'nidx-01': 'delivery-review.html#nidx-01',
    'nidx-02': 'delivery-review.html#nidx-02',
    'nidx-03': 'delivery-review.html#nidx-03',
    'nidx-04': 'delivery-review.html#nidx-04',
    'nidx-05': 'delivery-review.html#nidx-05',
    'dec-001': 'delivery-review.html#dec-001',
    'dec-002': 'delivery-review.html#dec-002',
    'dec-003': 'delivery-review.html#dec-003',
    'dec-004': 'delivery-review.html#dec-004',
    'dec-005': 'delivery-review.html#dec-005',
    'dec-006': 'delivery-review.html#dec-006',
    'dec-007': 'delivery-review.html#dec-007',
    'dec-008': 'delivery-review.html#dec-008',
    'dec-009': 'delivery-review.html#dec-009',
    'dec-010': 'delivery-review.html#dec-010',
    'dec-011': 'delivery-review.html#dec-011',
    'review-notes': 'delivery-review.html#review-notes',
    acceptance: 'delivery-review.html#acceptance',
    traceability: 'delivery-review.html#traceability'
  };

  if (page === 'overview' && location.hash.length > 1) {
    const target = movedSections[location.hash.slice(1)];
    if (target) {
      location.replace(target);
      return;
    }
  }

  const sections = [...document.querySelectorAll('[data-searchable]')];
  const tocLinks = [...document.querySelectorAll('.toc a')];
  const searchInput = document.querySelector('#spec-search');
  const filterButtons = [...document.querySelectorAll('.filter-button')];
  const decisions = [...document.querySelectorAll('[data-decision]')];
  const reviewNotes = document.querySelector('#review-notes');
  const emptySearch = document.querySelector('#empty-search');
  let activeFilter = 'all';

  const readStoredReview = () => {
    try {
      return JSON.parse(localStorage.getItem(storageKey) || '{}');
    } catch {
      localStorage.removeItem(storageKey);
      return {};
    }
  };

  const reviewState = () => {
    const stored = readStoredReview();
    const disposition = { ...documentedDisposition, ...(stored.disposition || {}) };
    decisions.forEach(select => { disposition[select.dataset.decision] = select.value; });
    return {
      specification: 'BDB-NIDX-SPEC-001',
      revision: '0.2',
      disposition,
      notes: reviewNotes ? reviewNotes.value : (stored.notes || '')
    };
  };

  const updateReviewMeter = () => {
    const state = reviewState();
    const decisionIDs = Object.keys(documentedDisposition);
    const reviewed = decisionIDs.filter(id => state.disposition[id]).length;
    const fill = document.querySelector('#review-meter-fill');
    const label = document.querySelector('#review-meter-label');
    if (fill) fill.style.width = `${Math.min(reviewed / decisionIDs.length, 1) * 100}%`;
    if (label) label.textContent = `${reviewed} of ${decisionIDs.length} decisions dispositioned`;
  };

  const saveReview = () => {
    localStorage.setItem(storageKey, JSON.stringify(reviewState()));
    updateReviewMeter();
  };

  const loadReview = () => {
    const saved = readStoredReview();
    decisions.forEach(select => {
      const savedValue = saved.disposition?.[select.dataset.decision];
      if (savedValue !== undefined) select.value = savedValue;
    });
    if (reviewNotes && saved.notes !== undefined) reviewNotes.value = saved.notes;
    updateReviewMeter();
  };

  const applyFilters = () => {
    const query = searchInput?.value.trim().toLocaleLowerCase() || '';
    let visible = 0;
    sections.forEach(section => {
      const filterMatch = activeFilter === 'all' ||
        (activeFilter === 'normative' && section.hasAttribute('data-normative')) ||
        (activeFilter === 'open' && section.hasAttribute('data-open'));
      const searchMatch = query === '' || section.textContent.toLocaleLowerCase().includes(query);
      section.classList.toggle('hidden-by-filter', !(filterMatch && searchMatch));
      if (filterMatch && searchMatch) visible += 1;
    });
    if (emptySearch) emptySearch.style.display = visible === 0 ? 'block' : 'none';
  };

  const reviewSummaryText = () => {
    const state = reviewState();
    const lines = [`${state.specification} revision ${state.revision}`];
    Object.entries(state.disposition).forEach(([id, value]) => lines.push(`${id}: ${value || 'Not reviewed'}`));
    if (state.notes) lines.push('', 'Notes:', state.notes);
    return lines.join('\n');
  };

  searchInput?.addEventListener('input', applyFilters);
  filterButtons.forEach(button => button.addEventListener('click', () => {
    activeFilter = button.dataset.filter;
    filterButtons.forEach(candidate => candidate.setAttribute('aria-pressed', String(candidate === button)));
    applyFilters();
  }));
  decisions.forEach(select => select.addEventListener('change', saveReview));
  reviewNotes?.addEventListener('input', saveReview);

  document.querySelector('#export-review')?.addEventListener('click', () => {
    const blob = new Blob([JSON.stringify(reviewState(), null, 2)], { type: 'application/json' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = 'BDB-NIDX-SPEC-001-review.json';
    link.click();
    URL.revokeObjectURL(link.href);
  });

  document.querySelector('#copy-review')?.addEventListener('click', async event => {
    const summary = reviewSummaryText();
    let copied = false;
    if (navigator.clipboard?.writeText) {
      try {
        await navigator.clipboard.writeText(summary);
        copied = true;
      } catch {
        copied = false;
      }
    }
    if (!copied) {
      const fallback = document.createElement('textarea');
      fallback.value = summary;
      fallback.style.position = 'fixed';
      fallback.style.opacity = '0';
      document.body.append(fallback);
      fallback.select();
      document.execCommand('copy');
      fallback.remove();
    }
    const previous = event.currentTarget.textContent;
    event.currentTarget.textContent = 'Copied';
    window.setTimeout(() => { event.currentTarget.textContent = previous; }, 1200);
  });

  document.querySelector('#reset-review')?.addEventListener('click', () => {
    localStorage.removeItem(storageKey);
    decisions.forEach(select => {
      const defaultOption = [...select.options].find(option => option.defaultSelected);
      select.value = defaultOption?.value || '';
    });
    if (reviewNotes) reviewNotes.value = '';
    updateReviewMeter();
  });

  const root = document.documentElement;
  const savedTheme = localStorage.getItem(themeKey);
  if (savedTheme === 'night') root.dataset.theme = 'night';
  document.querySelector('#theme-toggle')?.addEventListener('click', () => {
    root.dataset.theme = root.dataset.theme === 'night' ? 'paper' : 'night';
    localStorage.setItem(themeKey, root.dataset.theme);
  });
  document.querySelector('#print-spec')?.addEventListener('click', () => window.print());

  const updateProgress = () => {
    const max = document.documentElement.scrollHeight - innerHeight;
    const progress = document.querySelector('#scroll-progress');
    if (progress) progress.style.width = `${max > 0 ? (scrollY / max) * 100 : 0}%`;
  };
  addEventListener('scroll', updateProgress, { passive: true });
  updateProgress();

  if ('IntersectionObserver' in window) {
    const observer = new IntersectionObserver(entries => {
      const visible = entries
        .filter(entry => entry.isIntersecting)
        .sort((left, right) => right.intersectionRatio - left.intersectionRatio)[0];
      if (!visible) return;
      tocLinks.forEach(link => {
        const linkURL = new URL(link.href);
        const samePage = linkURL.pathname === location.pathname;
        link.classList.toggle('active', samePage && linkURL.hash === `#${visible.target.id}`);
      });
    }, { rootMargin: '-18% 0px -68% 0px', threshold: [0, .2, .6] });
    sections.forEach(section => observer.observe(section));
  }

  loadReview();
})();
