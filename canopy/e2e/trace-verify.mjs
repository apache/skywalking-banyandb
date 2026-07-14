import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { chromium } from '@playwright/test';

const BASE = 'http://127.0.0.1:4000';
const TRACE_ID = 'sw-demo-trace-001';
const SHOTS = join(import.meta.dirname, 'screenshots');
mkdirSync(SHOTS, { recursive: true });

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });

try {
  await page.goto(`${BASE}/login`, { waitUntil: 'networkidle' });
  await page.waitForSelector('input#f-username', { timeout: 30_000 });
  await page.locator('input#f-username').fill('admin');
  await page.locator('input#f-password').fill('admin');
  await page.locator('button[type=submit]').click();
  await page.waitForSelector('.shell, .qb-card, nav.sidebar', { timeout: 30_000 });

  await page.goto(`${BASE}/query`, { waitUntil: 'networkidle' });
  await page.waitForSelector('.qb-card', { timeout: 30_000 });

  await page.locator('button.qb-cat-btn', { hasText: /^trace$/i }).first().click();
  await page.locator('.qb-search-input').fill('service_spans');
  await page.locator('.qb-search-item', { hasText: /service_spans/ }).first().click();

  await page.waitForTimeout(2_000);
  const valueInput = page.locator('.qb-section .qb-cond input[aria-label="Value"]').first();
  await valueInput.fill(TRACE_ID);
  await valueInput.press('Tab');
  await page.waitForTimeout(300);

  await page.locator('button.btn-primary', { hasText: /Run/i }).click();
  await page.waitForSelector('.tin-row', { timeout: 60_000 });

  await page.screenshot({ path: join(SHOTS, 'trace-result-flat.png'), fullPage: false });

  await page.locator('.tin-row').first().locator('.tin-main').click();
  await page.waitForTimeout(300);
  await page.screenshot({ path: join(SHOTS, 'trace-result-expanded.png'), fullPage: false });

  console.log('Screenshots saved to', SHOTS);
  console.log('You can now open the demo, upload /tmp/trace-seed/proto/Tracing.proto manually, and click Decode on a row.');
} catch (err) {
  console.error('FAILED:', err);
  await page.screenshot({ path: join(SHOTS, 'trace-verify-error.png'), fullPage: false });
  process.exitCode = 1;
} finally {
  await browser.close();
}
