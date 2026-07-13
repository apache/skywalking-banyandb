import { chromium } from '@playwright/test';
import { join } from 'node:path';

const BASE = 'http://127.0.0.1:4000';
const TRACE_ID = 'sw-demo-trace-001';
const SHOTS = join(process.cwd(), 'e2e', 'screenshots');
import { mkdirSync } from 'node:fs';
mkdirSync(SHOTS, { recursive: true });

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });

try {
  // Login
  await page.goto(`${BASE}/login`);
  await page.waitForSelector('input#f-username', { timeout: 30_000 });
  await page.locator('input#f-username').fill('admin');
  await page.locator('input#f-password').fill('admin');
  await page.locator('button[type=submit]').click();
  await page.waitForSelector('.shell, .qb-card, nav.sidebar', { timeout: 30_000 });

  // Query page
  await page.goto(`${BASE}/query`);
  await page.waitForLoadState('networkidle');
  await page.waitForSelector('.qb-card', { timeout: 30_000 });

  // Switch to trace catalog and pick service_spans resource
  await page.locator('button.qb-cat-btn', { hasText: /^trace$/i }).first().click();
  await page.locator('.qb-search-input').fill('service_spans');
  await page.locator('.qb-search-item', { hasText: /service_spans/ }).first().click();
  await page.waitForTimeout(500);

  // Add WHERE trace_id = ...
  await page.locator('.qb-section', { hasText: /WHERE/i }).locator('button.qb-add').first().click();
  await page.locator('.qb-cond select[aria-label="Tag"]').first().selectOption('trace_id');
  await page.locator('.qb-cond input[aria-label="Value"]').first().fill(TRACE_ID);

  // Run
  await page.locator('button.btn-primary', { hasText: /Run/i }).click();
  await page.waitForSelector('.tin-row', { timeout: 60_000 });

  await page.screenshot({ path: join(SHOTS, 'trace-result-flat.png'), fullPage: false });

  // Expand first row
  await page.locator('.tin-row').first().locator('.tin-main').click();
  await page.waitForTimeout(300);
  await page.screenshot({ path: join(SHOTS, 'trace-result-expanded.png'), fullPage: false });

  // Upload Tracing.proto
  const protoPath = '/tmp/trace-seed/proto/Tracing.proto';
  await page.locator('.mr-toolbar label input[type="file"]').setInputFiles(protoPath);
  await page.waitForTimeout(500);

  // Decode first row
  await page.locator('.tin-row').first().locator('button', { hasText: /decode/i }).click();
  await page.waitForSelector('.tdm-modal', { timeout: 10_000 });
  await page.waitForTimeout(500);
  await page.screenshot({ path: join(SHOTS, 'trace-decoder-modal.png'), fullPage: false });

  console.log('Screenshots saved to', SHOTS);
} catch (err) {
  console.error('FAILED:', err);
  await page.screenshot({ path: join(SHOTS, 'trace-verify-error.png'), fullPage: false });
  process.exitCode = 1;
} finally {
  await browser.close();
}
