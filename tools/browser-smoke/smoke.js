const { chromium } = require('playwright');

(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  page.on('console', (msg) => {
    if (msg.type() === 'error') {
      console.error(`browser console error: ${msg.text()}`);
    }
  });

  await page.goto('http://localhost:8081', { waitUntil: 'domcontentloaded', timeout: 120000 });
  await page.click('#runBtn');
  await page.waitForFunction(() => {
    const status = document.querySelector('#status');
    return status && status.textContent.startsWith('done');
  }, { timeout: 120000 });

  const status = await page.textContent('#status');
  const rows = await page.locator('#result tbody tr').count();
  console.log(`status=${status} rows=${rows}`);
  if (!status || !status.startsWith('done') || rows < 1) {
    throw new Error('browser smoke query did not produce rows');
  }

  await browser.close();
})().catch((err) => {
  console.error(err?.stack || String(err));
  process.exit(1);
});
