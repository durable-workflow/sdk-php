import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import net from 'node:net';
import path from 'node:path';
import {chromium} from 'playwright';

const buildDirectory = path.resolve(process.argv[2] ?? 'build/api');
const viewports = [
  ['desktop', {width: 1440, height: 900}],
  ['intermediate', {width: 768, height: 1024}],
  ['mobile', {width: 390, height: 844}],
  ['compact-height', {width: 640, height: 360}],
];
const pages = [
  ['root', '/index.html'],
  ['nested API', '/namespaces/durableworkflow-worker.html'],
];

async function availablePort() {
  const server = net.createServer();
  server.unref();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  assert(address && typeof address === 'object');
  await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  return address.port;
}

async function waitForServer(port) {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    try {
      await new Promise((resolve, reject) => {
        const socket = net.connect(port, '127.0.0.1');
        socket.once('connect', () => { socket.destroy(); resolve(); });
        socket.once('error', reject);
      });
      return;
    } catch (_error) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }
  throw new Error(`PHP documentation server did not start on port ${port}.`);
}

async function assertReachableControls(page, label, scope = 'body') {
  const result = await page.evaluate((scopeSelector) => {
    const selector = 'a[href], button, input:not([type="hidden"]), select, textarea, summary, [role="button"]';
    const unreachable = [];
    const root = document.querySelector(scopeSelector);
    for (const element of root?.querySelectorAll(selector) || []) {
      if (
        element.closest('.phpdocumentor-sidebar')
        && !document.querySelector('.phpdocumentor-sidebar__menu-button')?.checked
      ) continue;
      const style = getComputedStyle(element);
      const box = element.getBoundingClientRect();
      const excludedByAncestor = [...function* ancestors(node) {
        for (let parent = node.parentElement; parent; parent = parent.parentElement) yield parent;
      }(element)].some(parent => {
        const parentStyle = getComputedStyle(parent);
        return parentStyle.visibility === 'hidden'
          || parentStyle.display === 'none'
          || parentStyle.pointerEvents === 'none'
          || Number(parentStyle.opacity) === 0;
      });
      if (
        excludedByAncestor
        || style.visibility === 'hidden'
        || style.display === 'none'
        || style.pointerEvents === 'none'
        || Number(style.opacity) === 0
        || box.width < 1
        || box.height < 1
        || box.right <= 0
        || box.left >= innerWidth
        || box.bottom <= 0
        || box.top >= innerHeight
      ) continue;
      const x = Math.max(0, Math.min(innerWidth - 1, box.left + box.width / 2));
      const y = Math.max(0, Math.min(innerHeight - 1, box.top + box.height / 2));
      if (y < 0 || y >= innerHeight) continue;
      const hit = document.elementFromPoint(x, y);
      if (!(hit === element || element.contains(hit) || hit?.contains(element))) {
        unreachable.push(element.outerHTML.slice(0, 180));
      }
    }
    return {
      unreachable,
      viewportWidth: document.documentElement.clientWidth,
      documentWidth: Math.max(document.documentElement.scrollWidth, document.body.scrollWidth),
    };
  }, scope);
  assert.equal(result.documentWidth, result.viewportWidth, `${label} has horizontal overflow`);
  assert.deepEqual(result.unreachable, [], `${label} has unreachable controls`);
}

async function exercisePage(browser, origin, viewportName, viewport, pageName, pagePath) {
  const context = await browser.newContext({viewport, reducedMotion: 'reduce'});
  const page = await context.newPage();
  const errors = [];
  page.on('console', message => {
    if (message.type() === 'error') errors.push(`console: ${message.text()}`);
  });
  page.on('pageerror', error => errors.push(`page: ${error.message}`));
  const label = `${pageName} ${viewportName}`;

  try {
    const response = await page.goto(`${origin}${pagePath}`, {waitUntil: 'networkidle'});
    assert.equal(response?.status(), 200, `${label} did not render`);
    await page.locator('.phpdocumentor-content').waitFor();
    const retired = await page.evaluate(() => ({
      ui: document.querySelectorAll('.dw-analytics-consent, .dw-analytics-preferences, #durable-workflow-analytics-consent, #durable-workflow-analytics-preferences').length,
      storage: localStorage.getItem('durable-workflow.analytics-consent.v1'),
      google: [...document.scripts].filter(script => /googletagmanager|google-analytics/.test(script.src)).length,
      runtime: [...document.scripts].filter(script => script.src.endsWith('/analytics/analytics.js')).length,
    }));
    assert.deepEqual(retired, {ui: 0, storage: null, google: 0, runtime: 1}, `${label} analytics boundary`);
    assert.equal(await page.locator('.phpdocumentor-title__link').count(), 1, `${label} lost its title link`);
    assert.equal(await page.locator('.phpdocumentor-search__field').count(), 1, `${label} lost search`);
    await assertReachableControls(page, `${label} default`);

    const sidebarMenu = page.locator('.phpdocumentor-sidebar__menu-icon');
    if (await sidebarMenu.isVisible()) {
      await sidebarMenu.click();
      assert.equal(await page.locator('.phpdocumentor-sidebar__menu-button').isChecked(), true, `${label} sidebar did not open`);
      await assertReachableControls(page, `${label} open sidebar`);
      await sidebarMenu.click();
    }

    const search = page.locator('.phpdocumentor-search__field');
    await search.pressSequentially('Workflow');
    await page.locator('.phpdocumentor-search-results:not(.phpdocumentor-search-results--hidden)').waitFor();
    assert.ok(await page.locator('.phpdocumentor-search-results__entry').count() > 0, `${label} search has no results`);
    await assertReachableControls(page, `${label} open search`, '.phpdocumentor-search-results');
    assert.deepEqual(errors, [], `${label} emitted browser errors`);
  } finally {
    await context.close();
  }
}

const port = await availablePort();
const server = spawn('php', ['-S', `127.0.0.1:${port}`, '-t', buildDirectory], {stdio: 'ignore'});
let browser;
try {
  await waitForServer(port);
  browser = await chromium.launch({headless: true});
  for (const [viewportName, viewport] of viewports) {
    for (const [pageName, pagePath] of pages) {
      await exercisePage(browser, `http://127.0.0.1:${port}`, viewportName, viewport, pageName, pagePath);
    }
  }
  process.stdout.write('Validated analytics-free PHP reference controls across desktop, intermediate, mobile, and compact-height viewports.\n');
} finally {
  await browser?.close();
  server.kill('SIGTERM');
  await Promise.race([once(server, 'exit'), new Promise(resolve => setTimeout(resolve, 1000))]);
}
