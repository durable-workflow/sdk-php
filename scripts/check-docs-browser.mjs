import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import net from 'node:net';
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { chromium } from 'playwright';

// Loopback is a secure browser context for testing the real clipboard API.
const SITE_HOSTNAME = '127.0.0.1';
const siteDirectory = path.resolve(process.argv[2] ?? 'build/site');
const oneIdentity = fs.existsSync(path.join(siteDirectory, 'assets/identity.css'));

async function availablePort() {
  const server = net.createServer();
  server.unref();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  assert(address && typeof address === 'object');
  const {port} = address;
  await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  return port;
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
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }
  throw new Error('Documentation server did not start.');
}

function browserErrors(page, label) {
  const errors = [];
  page.on('console', (message) => { if (message.type() === 'error') errors.push(`${label}: ${message.text()}`); });
  page.on('pageerror', (error) => errors.push(`${label}: ${error.message}`));
  page.on('response', (response) => {
    if (response.status() >= 400) {
      errors.push(`${label}: ${response.url()} returned ${response.status()}`);
    }
  });
  return errors;
}

async function layoutState(page) {
  return page.evaluate(() => {
    const visible = (element) => {
      const style = getComputedStyle(element);
      const rect = element.getBoundingClientRect();
      return style.visibility !== 'hidden'
        && style.display !== 'none'
        && style.pointerEvents !== 'none'
        && Number(style.opacity) > 0
        && rect.width > 0
        && rect.height > 0;
    };
    const unreachable = Array.from(document.querySelectorAll('a, button, input'))
      .filter(visible)
      .filter((element) => {
        if (
          element.closest('.phpdocumentor-sidebar')
          && !document.querySelector('.phpdocumentor-sidebar__menu-button')?.checked
        ) return false;
        const rect = element.getBoundingClientRect();
        if (rect.bottom <= 0 || rect.top >= innerHeight || rect.right <= 0 || rect.left >= innerWidth) return false;
        const x = Math.max(0, Math.min(innerWidth - 1, rect.left + rect.width / 2));
        const y = Math.max(0, Math.min(innerHeight - 1, rect.top + rect.height / 2));
        const hit = document.elementFromPoint(x, y);
        return hit !== element && !element.contains(hit) && !hit?.contains(element);
      })
      .map((element) => {
        const rect = element.getBoundingClientRect();
        const x = Math.max(0, Math.min(innerWidth - 1, rect.left + rect.width / 2));
        const y = Math.max(0, Math.min(innerHeight - 1, rect.top + rect.height / 2));
        const hit = document.elementFromPoint(x, y);
        return {
          element: element.outerHTML.slice(0, 180),
          hit: hit?.outerHTML.slice(0, 180) ?? null,
          rect: rect.toJSON(),
        };
      });
    return {
      viewportWidth: document.documentElement.clientWidth,
      documentWidth: Math.max(document.documentElement.scrollWidth, document.body.scrollWidth),
      title: document.title,
      mainText: document.querySelector('main, .phpdocumentor-content')?.textContent.trim().length ?? 0,
      unreachable,
    };
  });
}

async function validateSurface(browser, origin, surface, viewportName, viewport, theme) {
  const context = await browser.newContext({viewport, colorScheme: 'dark', permissions: ['clipboard-read', 'clipboard-write']});
  const page = await context.newPage();
  const label = `${surface.name} ${viewportName} ${theme}`;
  const errors = browserErrors(page, label);
  await page.route('https://cloud.durable-workflow.com/early-access/promotion-events', route => route.fulfill({
    status: 204,
    headers: {
      'access-control-allow-origin': origin,
      'cache-control': 'no-store',
      vary: 'Origin',
    },
  }));
  // A deliberately unqualified fixture keeps status neutral in source layout checks.
  await page.route('https://status.durable-workflow.com/api/current.json', route => route.fulfill({
    status: 200, contentType: 'application/json', body: '{}',
    headers: {'access-control-allow-origin': origin, 'cache-control': 'no-store'},
  }));
  await page.route('https://cloudflareinsights.com/cdn-cgi/rum', route => route.fulfill({
    status: 204,
    headers: {
      'access-control-allow-origin': origin,
      'cache-control': 'no-store',
    },
  }));
  await page.goto(`${origin}${surface.path}`, {waitUntil: 'networkidle'});
  await page.evaluate(() => document.fonts.ready);

  if (oneIdentity) {
  assert.notEqual(await page.locator('html').getAttribute('data-theme'), 'dark', `${label} must default to light even with a dark system preference.`);
  }
  if (viewport.width <= (oneIdentity ? 920 : 800)) await page.locator('.nav-toggle').click();
  if (oneIdentity) {
  if (theme === 'dark') await page.locator('.theme-toggle').click();
  assert.equal(await page.locator('.theme-toggle').getAttribute('aria-pressed'), String(theme === 'dark'));
  assert.equal(await page.locator('body').evaluate(element => getComputedStyle(element).backgroundColor),
    theme === 'dark' ? 'rgb(15, 21, 19)' : 'rgb(246, 247, 245)', `${label} must use the shared Paper token.`);
  const sdkSummary = page.locator('.sdk-switcher summary');
  await sdkSummary.focus(); await page.keyboard.press('Enter');
  assert(await page.locator('.sdk-menu a').first().isVisible(), `${label} SDK switcher must open from the keyboard.`);
  await page.keyboard.press('Escape');
  assert.equal(await page.locator('.sdk-switcher').getAttribute('open'), null, `${label} SDK switcher must close with Escape.`);
  assert.equal(await page.evaluate(() => document.activeElement === document.querySelector('.sdk-switcher summary')), true);

  }

  if (surface.kind === 'api') {
    await page.locator('.phpdocumentor-content').waitFor();
    assert(await page.locator('.phpdocumentor-search__field').isVisible(), `${label} must show API search.`);
    assert(await page.locator('.dw-api-guide-link').isVisible(), `${label} must link back to authored guides.`);
    if (surface.name === 'API Reference' && ['desktop', 'mobile'].includes(viewportName)) {
      const search = page.locator('.phpdocumentor-search__field');
      await search.fill('Client');
      await search.press('Enter');
      const dialog = page.locator('.phpdocumentor-search-results');
      await page.waitForFunction(() => !document.querySelector('.phpdocumentor-search-results').classList.contains('phpdocumentor-search-results--hidden'));
      assert(await dialog.locator('a[href*="Client"]').count() > 0, `${label} must find generated Client symbols.`);
      await page.getByRole('button', {name: 'Close search results'}).click();
      await page.waitForFunction(() => document.querySelector('.phpdocumentor-search-results').classList.contains('phpdocumentor-search-results--hidden'));
    }
    const sidebarMenu = page.locator('.phpdocumentor-sidebar__menu-icon');
    if (await sidebarMenu.isVisible()) {
      assert.equal(await sidebarMenu.textContent(), 'Open navigation', `${label} must identify the collapsed API navigation.`);
      assert.match(await sidebarMenu.evaluate((element) => getComputedStyle(element).letterSpacing), /^(?:normal|0px)$/, `${label} must use normal menu-label spacing.`);
      await sidebarMenu.click();
      assert.equal(await sidebarMenu.textContent(), 'Close navigation', `${label} must expose a dismiss action for the API navigation.`);
      assert.equal(await page.locator('.phpdocumentor-sidebar__menu-button').getAttribute('aria-expanded'), 'true', `${label} must expose the expanded API navigation state.`);
      const title = await page.locator('.phpdocumentor-title__link').evaluate((element) => ({
        clientWidth: element.clientWidth,
        scrollWidth: element.scrollWidth,
        text: element.textContent.trim(),
      }));
      assert.equal(title.text, 'Durable Workflow PHP SDK — API Reference', `${label} must keep the complete API title while navigation is open.`);
      assert(title.scrollWidth <= title.clientWidth, `${label} clips the API title while navigation is open.`);
      await sidebarMenu.click();
    }
  } else {
    await page.locator('#main-content').waitFor();
    assert(await page.locator('.site-header .brand').isVisible(), `${label} must show the site identity.`);
    if (viewport.width <= (oneIdentity ? 920 : 800)) {
      assert(await page.locator('.primary-nav').isVisible(), `${label} mobile navigation must open.`);
    }
  }

  if (oneIdentity && (surface.name === 'home' || surface.name === 'first workflow')) {
    const copy = page.locator('.copy-button').first();
    const expected = await copy.evaluate(element => element.closest('.code-stage').querySelector('code').textContent);
    await copy.click();
    await page.waitForFunction(() => document.querySelector('.copy-button').textContent === 'Copied');
    assert.equal(await page.evaluate(() => navigator.clipboard.readText()), expected, `${label} must copy exact code text.`);
  }
  if (process.env.DOCS_SCREENSHOTS_DIRECTORY && ['home', 'first workflow', 'API Reference'].includes(surface.name)
      && ['desktop', 'mobile'].includes(viewportName)) {
    }
  if (viewport.width <= (oneIdentity ? 920 : 800)) await page.locator('.nav-toggle').click();
  if (oneIdentity) {
    fs.mkdirSync(process.env.DOCS_SCREENSHOTS_DIRECTORY, {recursive: true});
    await page.screenshot({path: path.join(process.env.DOCS_SCREENSHOTS_DIRECTORY,
      `${surface.name.replaceAll(' ', '-').toLowerCase()}-${viewportName}-${theme}.png`), fullPage: true});
  }

  const state = await layoutState(page);
  assert.equal(state.documentWidth, state.viewportWidth, `${label} has horizontal overflow.`);
  assert(state.title.length > 8, `${label} must have a descriptive title.`);
  assert(state.mainText > 100, `${label} must render useful content.`);
  assert.deepEqual(state.unreachable, [], `${label} has controls covered by unrelated content.`);
  assert.deepEqual(errors, [], `${label} must render without browser or asset errors.`);
  await context.close();
  return label;
}

const port = await availablePort();
const origin = `http://${SITE_HOSTNAME}:${port}`;
const server = spawn('python3', ['-m', 'http.server', String(port), '--bind', '127.0.0.1', '--directory', siteDirectory], {stdio: ['ignore', 'pipe', 'pipe']});
let browser;

try {
  await waitForServer(port);
  const launchOptions = {
    headless: true,
    args: [`--host-resolver-rules=MAP ${SITE_HOSTNAME} 127.0.0.1`, '--no-proxy-server'],
  };
  if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH) launchOptions.executablePath = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH;
  browser = await chromium.launch(launchOptions);

  const surfaces = [
    {name: 'home', path: '/', kind: 'portal'},
    {name: 'first workflow', path: '/getting-started/first-workflow/', kind: 'portal'},
    {name: 'Laravel', path: '/frameworks/laravel/', kind: 'portal'},
    {name: 'Symfony', path: '/frameworks/symfony/', kind: 'portal'},
    {name: 'API Reference', path: '/api/', kind: 'api'},
    {name: 'API package', path: '/api/packages/Application.html', kind: 'api'},
    {name: 'API report', path: '/api/reports/deprecated.html', kind: 'api'},
  ];
  const viewports = [
    ['desktop', {width: 1440, height: 900}],
    ['intermediate', {width: 768, height: 1024}],
    ['mobile', {width: 390, height: 844}],
    ['short height', {width: 640, height: 360}],
  ];
  // Async-generator yields keep one context at a time on shared hosts.
  async function* checks() {
    for (const surface of surfaces) {
      for (const [viewportName, viewport] of viewports) {
        for (const theme of oneIdentity ? ['light', 'dark'] : ['legacy']) {
          yield validateSurface(browser, origin, surface, viewportName, viewport, theme);
        }
      }
    }
  }
  const completed = new Set();
  for await (const label of checks()) completed.add(label);

  console.log(`Validated ${completed.size} portal/API theme/viewport cells; keyboard switcher, exact code copying and light-default controls passed.`);
} finally {
  await browser?.close();
  server.kill('SIGTERM');
  await once(server, 'exit').catch(() => {});
}
