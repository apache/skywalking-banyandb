// Playwright probe: verify the Top-N tab's FROM row surfaces topn-aggregation
// names (not measure names) after the TopNAggregationRegistryService wiring.
//
// Runs against the live demo at http://localhost:4000/query.

import { chromium } from '@playwright/test';
import { mkdirSync } from 'node:fs';

const SHOTS = '/tmp/topn-agg-shots';
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
  page.on('console', (m) => console.log(`[browser:${m.type()}]`, m.text()));

  await page.goto('http://localhost:4000/query');
  await page.waitForSelector('.qb-card', { timeout: 30_000 });
  // Clear the persisted builder state so the test starts fresh
  // (previous runs may have left a topn-catalog/group combo whose group
  // has zero topn-aggs, which would mask the new fix).
  await page.evaluate(() => localStorage.clear());

  // Reload after clearing localStorage so the page picks up a fresh default state.
  await page.reload();
  await page.waitForSelector('.qb-card', { timeout: 30_000 });

  // Set up network listener EARLY so we capture initial fetches
  const networkCalls = [];
  page.on('response', (resp) => {
    if (resp.url().includes('topn-agg')) {
      networkCalls.push(`${resp.status()} ${resp.url()}`);
    }
  });

  await page.waitForTimeout(2500); // give the initial group + topn-agg prefetch time to settle
  console.log('topn-agg network calls (initial load):', networkCalls);
  await page.screenshot({ path: `${SHOTS}/01-initial-measure.png`, fullPage: true });

  // Log current FROM state on measure tab
  const measureOpts = await page.locator('select[aria-label="Resource"] option').allTextContents();
  console.log('measure tab resource options:', measureOpts.slice(0, 15));

  // Click the Top-N tab
  await page.locator('button.qb-cat-btn', { hasText: /^top-n$/i }).first().click();
  await page.waitForTimeout(2000); // give the topn-agg fetch time to resolve
  console.log('topn-agg network calls (after tab click):', networkCalls);
  // Dump the localStorage QB state to see what's persisted
  const persisted = await page.evaluate(() => localStorage.getItem('canopy.query.v3'));
  console.log('QB_STORE localStorage:', persisted?.slice(0, 200));
  await page.screenshot({ path: `${SHOTS}/02-topn-tab.png`, fullPage: true });

  // Capture the FROM-row inline label and the resource dropdown contents.
  const inlineKw = (await page.locator('.qb-from-row .qb-inline-kw').first().textContent())?.trim();
  console.log(`FROM inline keyword: "${inlineKw}"`);

  const resourceOptions = await page.locator('select[aria-label="Resource"] option').allTextContents();
  console.log(`Resource dropdown options (count=${resourceOptions.length}):`);
  for (const o of resourceOptions.slice(0, 20)) console.log(`  - ${o}`);
  await page.screenshot({ path: `${SHOTS}/03-topn-dropdown.png` });

  // Assertions
  const checks = [];
  checks.push({ label: 'inline label is TOPN AGG (not MEASURE)', pass: inlineKw === 'TOPN AGG' });
  // Topn-aggs have varied name suffixes: sw_metric has `top_service` (`_service`),
  // sw_metricsMinute has `endpoint_cpm-service` (`-service`). Match anything that
  // does NOT match the measure naming pattern (`*_minute` for time-bucketed
  // measures, or `_top_n_result` for the auto-generated registry table).
  checks.push({
    label: 'dropdown contains a topn-agg name (not just a measure)',
    pass: resourceOptions.some((o) => o !== '— none —' && !o.endsWith('_minute') && o !== '_top_n_result'),
  });
  checks.push({
    label: 'dropdown does NOT contain measure names like service_cpm_minute',
    pass: !resourceOptions.some((o) => o.endsWith('_minute')),
  });
  checks.push({
    label: 'dropdown is non-empty',
    pass: resourceOptions.filter((o) => o && o !== '— none —').length > 0,
  });

  console.log('\n=== assertions ===');
  for (const c of checks) {
    console.log(`  ${c.pass ? '✓' : '✗'}  ${c.label}`);
  }

  // Try running the topn query end-to-end. sw_metricsMinute's endpoint_cpm-service
  // has known rows; running on the auto-picked sw_metric `top_service` would only
  // return 0 bars (no data in the 30-min window), which is a data issue, not
  // a code issue. Pick the populated topn-agg here.
  await page.locator('select[aria-label="Group"]').selectOption('sw_metricsMinute');
  await page.waitForTimeout(1500);
  // endpoint_cpm-service is the populated topn-agg in sw_metricsMinute
  const targetAgg = 'endpoint_cpm-service';
  console.log(`selecting topn-agg: ${targetAgg}`);
  await page.locator('select[aria-label="Resource"]').selectOption(targetAgg);
  await page.waitForTimeout(500);
  // Expand TIME and switch to a wider window (last 7 days) so the topn-agg
  // is likely to have data; 30-minute windows often come back empty.
  const timeAcc = page.locator('.qb-acc-head', { hasText: /^TIME/i }).first();
  if (await timeAcc.count() > 0 && (await timeAcc.getAttribute('aria-expanded')) === 'false') {
    await timeAcc.click();
    await page.waitForTimeout(200);
  }
  await page.locator('select[aria-label="Relative time range"]').selectOption('-7d');
  await page.waitForTimeout(200);
  await page.getByRole('button', { name: /^run$/i }).click();
  // Wait for the topn result view (tnlb-bar leaderboard bars in the redesigned view)
  try {
    await page.waitForSelector('.tnlb-bar', { timeout: 30_000 });
    await page.waitForTimeout(500);
    await page.screenshot({ path: `${SHOTS}/04-topn-result.png`, fullPage: true });
    const bars = await page.locator('.tnlb-bar').count();
    console.log(`Top-N leaderboard rendered with ${bars} bars`);
    checks.push({ label: 'Top-N query ran and rendered a result view', pass: true });
    checks.push({ label: `Top-N leaderboard has bars (${bars})`, pass: bars > 0 });
  } catch (e) {
    checks.push({ label: 'Top-N query ran and rendered a result view', pass: false });
    console.log(`  Top-N run failed: ${e.message}`);
    await page.screenshot({ path: `${SHOTS}/04-topn-result-failed.png`, fullPage: true });
  }

  
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