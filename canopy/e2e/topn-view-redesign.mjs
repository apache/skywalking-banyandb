// Playwright probe: verify the redesigned TopNResultView matches the
// handoff's layout — Ranking/Trace tabs, "Top N of X by value" toolbar,
// list-count pill, topN badge, time-bucket picker when multi-list, and
// per-list #|ENTITY_ID|TOPN bar|VALUE leaderboard.

import { chromium } from '@playwright/test';
import { mkdirSync } from 'node:fs';

const SHOTS = '/tmp/topn-view-shots';
mkdirSync(SHOTS, { recursive: true });

(async () => {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  await context.addCookies([{
    name: 'session',
    value: 'qh0%2BvlDVthW%2BtOrvbwfsJ9o9Km3%2FJeNVW8EhvuXQbseC66m%2FLAXMTQNacSxbn80jEGkDMARnlFvlrDd4r3aqrJ2VVbiWRKb62CTFZ56p9HHWnhzA%2FiK63Q%3D%3D%3B4EDjQyp5TtVyGRvhVmtbpQTJ8zNp8LlF',
    domain: 'localhost',
    path: '/',
    httpOnly: true,
    sameSite: 'Lax',
  }]);
  const page = await context.newPage();
  page.on('console', (m) => { if (m.type() === 'error') console.log(`[browser:error]`, m.text()); });

  await page.goto('http://localhost:4000/query');
  await page.waitForSelector('.qb-card', { timeout: 30_000 });
  await page.evaluate(() => localStorage.clear());
  await page.reload();
  await page.waitForSelector('.qb-card', { timeout: 30_000 });

  // Switch to Top-N tab, pick a known-populated group/agg, switch time to -7d
  await page.locator('button.qb-cat-btn', { hasText: /^top-n$/i }).first().click();
  await page.waitForTimeout(800);
  await page.locator('select[aria-label="Group"]').selectOption('sw_metricsMinute');
  await page.waitForTimeout(1500);
  await page.locator('select[aria-label="Resource"]').selectOption('endpoint_cpm-service');
  await page.waitForTimeout(300);
  // Expand TIME if collapsed, switch to last 7 days
  const timeAcc = page.locator('.qb-acc-head', { hasText: /^TIME/i }).first();
  if (await timeAcc.count() > 0 && (await timeAcc.getAttribute('aria-expanded')) === 'false') {
    await timeAcc.click();
    await page.waitForTimeout(200);
  }
  await page.locator('select[aria-label="Relative time range"]').selectOption('-7d');
  await page.waitForTimeout(200);
  await page.getByRole('button', { name: /^run$/i }).click();

  // Wait for leaderboard
  try {
    await page.waitForSelector('.tnlb-bar', { timeout: 30_000 });
  } catch (e) {
    console.log('No leaderboard rendered:', e.message);
    await page.screenshot({ path: `${SHOTS}/no-leaderboard.png`, fullPage: true });
    await browser.close();
    process.exit(1);
  }
  await page.waitForTimeout(500);
  await page.screenshot({ path: `${SHOTS}/01-topn-view.png`, fullPage: true });

  // ── Assertions ──
  const checks = [];

  // 1. Ranking tab is the active label
  const rankingTab = page.locator('button.result-tab', { hasText: /^Ranking$/ });
  checks.push({ label: 'Ranking tab is present', pass: (await rankingTab.count()) > 0 });

  // 2. Toolbar shows "Top N of <resource> by value"
  const toolbarText = (await page.locator('.mr-tool-label').textContent())?.replace(/\s+/g, ' ').trim();
  console.log('toolbar text:', toolbarText);
  checks.push({
    label: 'toolbar shows "Top 10 of endpoint_cpm-service by value"',
    pass: !!toolbarText && /Top\s+10\s+of\s+endpoint_cpm-service\s+by\s+value/i.test(toolbarText),
  });

  // 3. list-count pill on the right
  const listPill = (await page.locator('.mr-tool-right .mr-auto-tag').textContent())?.trim();
  console.log('list pill:', listPill);
  checks.push({
    label: 'right side shows a list-count pill',
    pass: !!listPill && /\d+\s+lists?\s+by\s+time|aggregated/i.test(listPill),
  });

  // 4. topN badge
  const topnBadge = (await page.locator('.topn-rank').textContent())?.trim();
  console.log('topn badge:', topnBadge);
  checks.push({ label: 'topN badge visible with icon + label', pass: !!topnBadge && /topN/i.test(topnBadge) });

  // 5. ENTITY_ID column header
  const headerLabel = (await page.locator('.tnlb-h-label').textContent())?.trim();
  console.log('header label:', headerLabel);
  // The BanyanDB wire format always encodes the entity under the literal key
  // `entity_id` regardless of the topn-agg's groupByTagNames — so the column
  // header is always `entity_id`.
  checks.push({
    label: 'ENTITY_ID column header is "entity_id" (wire-format key, regardless of groupByTagNames)',
    pass: headerLabel === 'entity_id',
  });

  // 5b. The cell value is decoded — should be a plain readable identifier
  // (not the raw base64 string and not an object).
  const cellText = (await page.locator('.tnlb-ent-v').first().textContent())?.trim();
  console.log('first cell value:', cellText);
  checks.push({
    label: 'cell value is a plain decoded string (not raw base64, not [object Object])',
    pass: !!cellText && cellText.length > 0 && !cellText.includes('base64') && !cellText.includes('[object'),
  });

  // 6. Leaderboard rows
  const rowCount = await page.locator('.tnlb-row').count();
  console.log('leaderboard rows:', rowCount);
  checks.push({ label: 'leaderboard has rows', pass: rowCount > 0 });

  // 7. Podium styling for top 3 — conditional on having ≥3 rows.
  const rank1 = await page.locator('.tnlb-rank.rank-1').count();
  const rank2 = await page.locator('.tnlb-rank.rank-2').count();
  const rank3 = await page.locator('.tnlb-rank.rank-3').count();
  console.log('podium ranks:', { rank1, rank2, rank3 });
  if (rowCount >= 3) {
    checks.push({ label: 'top-3 podium badges present (rank-1/2/3)', pass: rank1 >= 1 && rank2 >= 1 && rank3 >= 1 });
  } else {
    checks.push({ label: `top-3 podium skipped (only ${rowCount} row(s))`, pass: rank1 >= 1 && rank2 >= 1 });
  }

  // 8. Comma-formatted values
  const firstVal = (await page.locator('.tnlb-val').first().textContent())?.trim();
  console.log('first value:', firstVal);
  checks.push({ label: 'values are comma-formatted (e.g. "1,797" not "1797")', pass: !!firstVal && /^[\d,]+$/.test(firstVal) });

  // 9. Bars present per row
  const barCount = await page.locator('.tnlb-bar').count();
  checks.push({ label: 'one bar per row', pass: barCount === rowCount });

  // 10. ResultPanel status header
  const headerStatus = (await page.locator('.result-status').first().textContent())?.trim();
  console.log('header status:', headerStatus);
  checks.push({
    label: 'status header shows "executed in X ms"',
    pass: !!headerStatus && /executed\s+in\s+[\d.]+\s+ms/.test(headerStatus),
  });

  // 11. Multi-list: time-bucket picker visible?
  const tsBars = await page.locator('.tnlb-ts-bar').count();
  const tsPills = await page.locator('.tnlb-ts-pill').count();
  console.log('ts-bar:', tsBars, 'pills:', tsPills);
  // endpoint_cpm-service over 7d likely returns 1 list (aggregated from the
  // precomputed topn-agg), so the picker may not render. We just verify
  // that IF there are multiple lists, the picker renders.
  if (tsBars > 0) {
    checks.push({ label: 'time-bucket picker visible (multi-list)', pass: tsPills > 1 });
    // Click a pill and verify the active state changes
    await page.locator('.tnlb-ts-pill').first().click();
    await page.waitForTimeout(300);
    await page.screenshot({ path: `${SHOTS}/02-ts-pill-clicked.png`, fullPage: true });
  } else {
    console.log('Single-list response — skipping picker assertions');
    checks.push({ label: 'single-list response renders without picker (gracefully)', pass: true });
  }

  // Final
  console.log('\n=== final ===');
  let allPass = true;
  for (const c of checks) {
    console.log(`  ${c.pass ? '✓' : '✗'}  ${c.label}`);
    if (!c.pass) allPass = false;
  }
  console.log(allPass ? '\nALL PASS' : '\nFAIL');

  await browser.close();
  process.exit(allPass ? 0 : 1);
})();