import { test, expect, type Page } from '@playwright/test';
import { join } from 'node:path';

const BASE = 'http://127.0.0.1:4000';
const TRACE_ID = 'sw-demo-trace-001';

async function login(page: Page) {
  await page.goto(`${BASE}/login`);
  await page.locator('input[name=endpoint]').fill('http://127.0.0.1:17913');
  await page.locator('input[name=username]').fill('admin');
  await page.locator('input[name=password]').fill('admin');
  await page.locator('button[type=submit]').click();
  await page.waitForURL((u) => !u.pathname.startsWith('/login'), { timeout: 30_000 });
}

test('seeded SkyWalking trace renders and decodes', async ({ page }) => {
  await login(page);
  await page.goto(`${BASE}/query`);
  await page.waitForSelector('.qb-card', { timeout: 30_000 });

  await page.locator('button.qb-cat-btn', { hasText: /^trace$/i }).first().click();
  await page.locator('.qb-search-input').fill('service_spans');
  await page.locator('.qb-search-item', { hasText: /service_spans/ }).first().click();

  await page.waitForTimeout(500);

  await page.locator('.qb-section-where button.qb-add').first().click();
  await page.locator('.qb-cond select[aria-label="Tag"]').first().selectOption('trace_id');
  await page.locator('.qb-cond input[aria-label="Value"]').first().fill(TRACE_ID);

  await page.locator('button.qb-btn-primary', { hasText: /run/i }).click();
  await page.waitForSelector('.tin-row', { timeout: 60_000 });

  await page.screenshot({ path: join(process.cwd(), 'e2e', 'screenshots', 'trace-result-flat.png'), fullPage: false });

  await page.locator('.tin-row').first().locator('.tin-main').click();
  await page.waitForTimeout(300);
  await page.screenshot({ path: join(process.cwd(), 'e2e', 'screenshots', 'trace-result-expanded.png'), fullPage: false });

  const protoPath = join(process.cwd(), '..', '..', 'tmp', 'trace-seed', 'proto', 'Tracing.proto');
  await page.locator('.mr-toolbar label input[type="file"]').setInputFiles(protoPath);
  await page.waitForTimeout(500);
  await page.locator('.tin-row').first().locator('button', { hasText: /decode/i }).click();
  await page.waitForSelector('.tdm-modal', { timeout: 10_000 });
  await page.waitForTimeout(500);
  await page.screenshot({ path: join(process.cwd(), 'e2e', 'screenshots', 'trace-decoder-modal.png'), fullPage: false });

  await expect(page.locator('.tdm-modal')).toBeVisible();
});
