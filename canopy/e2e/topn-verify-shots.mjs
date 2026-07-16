// Capture verification screenshots of the Top-N FROM row + result view on
// the live demo at http://34.180.71.4:4000/query.

import { chromium } from '@playwright/test';
import { mkdirSync } from 'node:fs';

const SHOTS = '/tmp/topn-verify-shots';
mkdirSync(SHOTS, { recursive: true });

(async () => {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ viewport: { width: 1440, height: 1100 } });
  await context.addCookies([{
    name: 'session',
    value: 'qh0%2BvlDVthW%2BtOrvbwfsJ9o9Km3%2FJeNVW8EhvuXQbseC66m%2FLAXMTQNacSxbn80jEGkDMARnlFvlrDd4r3aqrJ2VVbiWRKb62CTFZ56p9HHWnhzA%2FiK63Q%3D%3D%3B4EDjQyp5TtVyGRvhVmtbpQTJ8zNp8LlF',
    domain: 'localhost', path: '/', httpOnly: true, sameSite: 'Lax',
  }]);
  const page = await context.newPage();

  // 1. Fresh load + clear localStorage so we start from defaults
  await page.goto('http://localhost:4000/query');
  await page.waitForSelector('.qb-card', { timeout: 30_000 });
  await page.evaluate(() => localStorage.clear());
  await page.reload();
  await page.waitForSelector('.qb-card', { timeout: 30_000 });
  await page.waitForTimeout(1500);
  await page.screenshot({ path: `${SHOTS}/01-fresh-load-measure.png`, fullPage: true });

  // 2. Click Top-N tab → capture the new FROM row (TOPN AGG dropdown)
  await page.locator('button.qb-cat-btn', { hasText: /^top-n$/i }).first().click();
  await page.waitForTimeout(1500);
  await page.screenshot({ path: `${SHOTS}/02-topn-from-row.png`, fullPage: true });

  // 3. Switch to sw_metricsMinute (has 9 topn-aggs including endpoint_cpm-service)
  await page.locator('select[aria-label="Group"]').selectOption('sw_metricsMinute');
  await page.waitForTimeout(1500);
  await page.locator('select[aria-label="Resource"]').selectOption('endpoint_cpm-service');
  await page.waitForTimeout(500);
  await page.screenshot({ path: `${SHOTS}/03-group-resource-picked.png`, fullPage: true });

  // 4. Expand TIME + set to last 7 days
  const timeAcc = page.locator('.qb-acc-head', { hasText: /^TIME/i }).first();
  if (await timeAcc.count() > 0 && (await timeAcc.getAttribute('aria-expanded')) === 'false') {
    await timeAcc.click();
    await page.waitForTimeout(200);
  }
  await page.locator('select[aria-label="Relative time range"]').selectOption('-7d');
  await page.waitForTimeout(300);

  // 5. Run the query
  await page.getByRole('button', { name: /^run$/i }).click();
  await page.waitForSelector('.tnlb-bar', { timeout: 30_000 });
  await page.waitForTimeout(800);
  await page.screenshot({ path: `${SHOTS}/04-result-single-list.png`, fullPage: true });

  // 6. Try a multi-list scenario — clear aggregation and re-run
  const aggSection = page.locator('.qb-acc-head', { hasText: /^AGGREGATE BY/i }).first();
  if (await aggSection.count() > 0 && (await aggSection.getAttribute('aria-expanded')) === 'false') {
    await aggSection.click();
    await page.waitForTimeout(200);
  }
  await page.locator('select[aria-label="Aggregate function"]').selectOption('');
  await page.waitForTimeout(200);

  // Switch to a different topn-agg that's more likely to have multi-list data
  await page.locator('select[aria-label="Resource"]').selectOption('endpoint_resp_time-service');
  await page.waitForTimeout(300);

  // Switch back to AGGREGATE BY MAX but with shorter time window (last 1h) so we may get multiple lists
  await page.locator('select[aria-label="Aggregate function"]').selectOption('MAX');
  await page.locator('select[aria-label="Relative time range"]').selectOption('-1h');
  await page.waitForTimeout(200);
  await page.getByRole('button', { name: /^run$/i }).click();

  try {
    await page.waitForSelector('.tnlb-ts-bar', { timeout: 30_000 });
    await page.waitForTimeout(800);
    await page.screenshot({ path: `${SHOTS}/05-result-multi-list.png`, fullPage: true });

    // Click a different pill to verify state change
    const pills = page.locator('.tnlb-ts-pill');
    const pillCount = await pills.count();
    if (pillCount > 2) {
      await pills.nth(pillCount - 2).click();
      await page.waitForTimeout(500);
      await page.screenshot({ path: `${SHOTS}/06-pill-clicked.png`, fullPage: true });
    }
  } catch {
    console.log('Multi-list scenario not available with this agg');
  }

  await browser.close();
  console.log('Screenshots saved to', SHOTS);
})();