import { chromium } from '@playwright/test';
import { readFileSync } from 'node:fs';

(async () => {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const page = await context.newPage();

  await page.goto('http://localhost:4000/login');
  await page.waitForSelector('#f-username', { timeout: 10_000 });
  await page.fill('#f-username', 'admin');
  await page.fill('#f-password', 'admin');
  await page.click('button[type="submit"]');
  await page.waitForSelector('.sidebar', { timeout: 15_000 });
  await page.goto('http://localhost:4000/query');
  await page.waitForSelector('.qb-card', { timeout: 30_000 });

  // Search for service_spans in m4-traces.
  await page.locator('.qb-search-input').fill('service_spans');
  await page.waitForTimeout(300);
  await page.locator('.qb-search-item', { hasText: /service_spans/ }).first().click();
  await page.waitForTimeout(500);

  // Set ORDER BY timestamp if collapsed.
  const orderHead = page.locator('.qb-section:has(.qb-section-h:has-text("ORDER BY")) .qb-section-h, .qb-acc:has(.qb-acc-head:has-text("ORDER BY")) .qb-acc-head');
  if (await orderHead.count() > 0) await orderHead.first().click();
  await page.waitForTimeout(200);
  await page.locator('select[aria-label="Order field"]').selectOption('timestamp');

  await page.getByRole('button', { name: /run/i }).click();
  await page.waitForSelector('.rv-root', { timeout: 60_000 });
  await page.waitForTimeout(1000);
  await page.screenshot({ path: '/mnt/d/worktree/canopy/canopy/e2e/screenshots/decode-run.png' });

  // Expand first row.
  const firstRow = page.locator('.tin-row').first();
  if (await firstRow.count() > 0) {
    await firstRow.click();
    await page.waitForTimeout(500);
    await page.screenshot({ path: '/mnt/d/worktree/canopy/canopy/e2e/screenshots/decode-card.png' });

    // Expand the span bytes inspector inside the row.
    const sizeChip = firstRow.locator('.tin-raw-size').first();
    if (await sizeChip.count() > 0) {
      await sizeChip.click();
      await page.waitForTimeout(500);
      await page.screenshot({ path: '/mnt/d/worktree/canopy/canopy/e2e/screenshots/decode-card-expanded.png' });
      // Close the popup so the rest of the UI is reachable.
      await page.locator('.sbin-backdrop').first().click();
      await page.waitForTimeout(200);
    }

    // Open decode bytes modal.
    const decodeBtn = page.locator('button', { hasText: /Decode bytes/i }).first();
    if (await decodeBtn.count() > 0 && await decodeBtn.isEnabled()) {
      await decodeBtn.click();
      await page.waitForTimeout(500);
      await page.screenshot({ path: '/mnt/d/worktree/canopy/canopy/e2e/screenshots/decode-modal-open.png' });

      // Upload Tracing.proto.
      const protoSrc = readFileSync('/mnt/d/worktree/canopy/canopy/e2e/fixtures/skywalking/Tracing.proto', 'utf8');
      const fileInput = await page.locator('.tdm-drop input[type="file"]').first();
      await fileInput.setInputFiles({
        name: 'Tracing.proto',
        mimeType: 'text/plain',
        buffer: Buffer.from(protoSrc),
      });
      await page.waitForTimeout(800);
      await page.screenshot({ path: '/mnt/d/worktree/canopy/canopy/e2e/screenshots/decode-modal-file.png' });

      const bindBtn = page.locator('button', { hasText: /Bind decoder|Replace decoder/i }).first();
      await bindBtn.click();
      await page.waitForTimeout(500);
      await page.screenshot({ path: '/mnt/d/worktree/canopy/canopy/e2e/screenshots/decode-bound.png' });
    }
  }

  await browser.close();
})();
