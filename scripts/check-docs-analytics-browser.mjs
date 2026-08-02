import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import net from 'node:net';
import path from 'node:path';
import process from 'node:process';
import { chromium } from 'playwright';

const SITE_HOSTNAME = 'php.durable-workflow.com';
const CONSENT_KEY = 'durable-workflow.analytics-consent.v1';
const buildDirectory = path.resolve(process.argv[2] ?? 'build/api');

async function availablePort() {
  const server = net.createServer();
  server.unref();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  assert(address && typeof address === 'object');
  const { port } = address;
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
  throw new Error(`PHP documentation server did not start on port ${port}.`);
}

function analyticsCookies(cookies) {
  return cookies.filter(({ name }) => name === '_ga' || name.startsWith('_ga_'));
}

function observeBrowserErrors(page, label) {
  const errors = [];
  page.on('console', (message) => {
    if (message.type() === 'error') errors.push(`${label} console: ${message.text()}`);
  });
  page.on('pageerror', (error) => errors.push(`${label} page: ${error.message}`));
  page.on('requestfailed', (request) => {
    errors.push(`${label} request: ${request.url()} (${request.failure()?.errorText ?? 'unknown failure'})`);
  });
  page.on('response', (response) => {
    if (response.status() >= 400) errors.push(`${label} resource: ${response.url()} (${response.status()})`);
  });
  return errors;
}

async function waitForReferenceUi(page) {
  await page.locator('.phpdocumentor-title__link').waitFor();
  await page.locator('.phpdocumentor-header__menu-icon').waitFor({ state: 'attached' });
  await page.locator('.phpdocumentor-search--active').waitFor();
  await page.locator('.phpdocumentor-search__field:not([disabled])').waitFor();
  await page.evaluate(() => document.fonts.ready);
}

async function layoutSnapshot(page) {
  return page.evaluate(() => {
    function rectangle(selector) {
      const element = document.querySelector(selector);
      if (!element) return null;
      const { left, top, right, bottom, width, height } = element.getBoundingClientRect();
      return { left, top, right, bottom, width, height };
    }

    return {
      viewportWidth: document.documentElement.clientWidth,
      documentWidth: Math.max(document.documentElement.scrollWidth, document.body.scrollWidth),
      overflowingElements: Array.from(document.body.querySelectorAll('*'))
        .map((element) => {
          const { left, right, width } = element.getBoundingClientRect();
          return {
            element: `${element.tagName.toLowerCase()}.${Array.from(element.classList).join('.')}`,
            html: element.outerHTML.slice(0, 240),
            left,
            right,
            width,
            scrollWidth: element.scrollWidth,
            clientWidth: element.clientWidth,
          };
        })
        .filter(({ right }) => right > document.documentElement.clientWidth + 0.5)
        .slice(0, 12),
      title: rectangle('.phpdocumentor-title__link'),
      titleTextLength: document.querySelector('.phpdocumentor-title__link')?.textContent.trim().length ?? 0,
      titleIsClipped: (() => {
        const title = document.querySelector('.phpdocumentor-title__link');
        return !title || title.scrollWidth > title.clientWidth + 1 || title.scrollHeight > title.clientHeight + 1;
      })(),
      menu: rectangle('.phpdocumentor-header__menu-icon'),
      search: rectangle('.phpdocumentor-search'),
      searchField: rectangle('.phpdocumentor-search__field'),
      banner: rectangle('#durable-workflow-analytics-consent:not([hidden])'),
      deny: rectangle('#durable-workflow-analytics-consent:not([hidden]) [data-consent="denied"]'),
      allow: rectangle('#durable-workflow-analytics-consent:not([hidden]) [data-consent="granted"]'),
      preferences: rectangle('#durable-workflow-analytics-preferences:not([hidden])'),
    };
  });
}

function assertWithinViewport(rectangle, viewportWidth, label) {
  assert(rectangle, `${label} must be rendered.`);
  assert(rectangle.width > 0 && rectangle.height > 0, `${label} must have a usable hit area.`);
  assert(rectangle.left >= -0.5, `${label} extends past the viewport's left edge.`);
  assert(rectangle.right <= viewportWidth + 0.5, `${label} extends past the viewport's right edge.`);
}

function assertNoOverlap(first, second, label) {
  assert(first && second, `${label} requires both controls to be rendered.`);
  const overlapWidth = Math.min(first.right, second.right) - Math.max(first.left, second.left);
  const overlapHeight = Math.min(first.bottom, second.bottom) - Math.max(first.top, second.top);
  assert(overlapWidth <= 0 || overlapHeight <= 0, `${label} controls overlap.`);
}

async function assertReachable(page, selector, label) {
  const reachable = await page.locator(selector).evaluate((element) => {
    const rectangle = element.getBoundingClientRect();
    const hit = document.elementFromPoint(
      rectangle.left + rectangle.width / 2,
      rectangle.top + rectangle.height / 2,
    );
    return hit === element || element.contains(hit) || Boolean(hit?.contains(element));
  });
  assert(reachable, `${label} must remain reachable.`);
}

async function assertReferenceLayout(page, label, consentState) {
  await waitForReferenceUi(page);
  const snapshot = await layoutSnapshot(page);
  assert.equal(
    snapshot.documentWidth,
    snapshot.viewportWidth,
    `${label} must not have document-level horizontal overflow: ${JSON.stringify(snapshot.overflowingElements)}`,
  );
  assert(snapshot.titleTextLength > 0, `${label} must retain its SDK title.`);
  assert.equal(snapshot.titleIsClipped, false, `${label} must render the full SDK title without clipping.`);

  for (const [control, rectangle] of [
    ['title', snapshot.title],
    ['search', snapshot.search],
    ['search field', snapshot.searchField],
  ]) {
    assertWithinViewport(rectangle, snapshot.viewportWidth, `${label} ${control}`);
  }
  assertNoOverlap(snapshot.title, snapshot.search, `${label} title and search`);

  await assertReachable(page, '.phpdocumentor-title__link', `${label} title`);
  await assertReachable(page, '.phpdocumentor-search__field', `${label} search`);

  if (snapshot.viewportWidth < 1000) {
    assertWithinViewport(snapshot.menu, snapshot.viewportWidth, `${label} menu`);
    assertNoOverlap(snapshot.title, snapshot.menu, `${label} title and menu`);
    assertNoOverlap(snapshot.menu, snapshot.search, `${label} menu and search`);
    await assertReachable(page, '.phpdocumentor-header__menu-icon', `${label} menu`);
  }

  if (consentState === 'initial') {
    for (const [control, rectangle] of [
      ['consent banner', snapshot.banner],
      ['necessary-only action', snapshot.deny],
      ['analytics action', snapshot.allow],
    ]) {
      assertWithinViewport(rectangle, snapshot.viewportWidth, `${label} ${control}`);
    }
    assertNoOverlap(snapshot.deny, snapshot.allow, `${label} consent actions`);
    await assertReachable(page, '[data-consent="denied"]', `${label} necessary-only action`);
    await assertReachable(page, '[data-consent="granted"]', `${label} analytics action`);
  } else {
    assertWithinViewport(snapshot.preferences, snapshot.viewportWidth, `${label} analytics preferences`);
    for (const [control, rectangle] of [
      ['title', snapshot.title],
      ['search', snapshot.search],
    ]) {
      assertNoOverlap(rectangle, snapshot.preferences, `${label} ${control} and analytics preferences`);
    }
    if (snapshot.viewportWidth < 1000) {
      assertNoOverlap(snapshot.menu, snapshot.preferences, `${label} menu and analytics preferences`);
    }
    await assertReachable(page, '#durable-workflow-analytics-preferences', `${label} analytics preferences`);
  }
}

async function validateReferenceLayouts(browser, siteOrigin) {
  const referencePages = [
    ['root', '/index.html'],
    ['nested API', '/namespaces/durableworkflow-worker.html'],
  ];

  for (const [pageName, pagePath] of referencePages) {
    const context = await browser.newContext({ viewport: { width: 320, height: 844 } });
    const page = await context.newPage();
    const errors = observeBrowserErrors(page, `${pageName} 320px`);
    await page.goto(`${siteOrigin}${pagePath}`);
    await page.locator('#durable-workflow-analytics-consent').waitFor();
    await assertReferenceLayout(page, `${pageName} 320px initial consent`, 'initial');

    await page.locator('[data-consent="denied"]').click();
    await page.locator('#durable-workflow-analytics-preferences').waitFor();
    await assertReferenceLayout(page, `${pageName} 320px selected consent`, 'selected');

    await page.reload();
    await page.locator('#durable-workflow-analytics-preferences').waitFor();
    await assertReferenceLayout(page, `${pageName} 320px stored consent`, 'stored');
    assert.deepEqual(errors, [], `${pageName} 320px browser render must be error-free.`);
    await context.close();
  }

  for (const [viewportName, viewport] of [
    ['wider mobile', { width: 390, height: 844 }],
    ['intermediate', { width: 768, height: 1024 }],
    ['desktop', { width: 1440, height: 900 }],
  ]) {
    const context = await browser.newContext({ viewport });
    const page = await context.newPage();
    const errors = observeBrowserErrors(page, `root ${viewportName}`);
    await page.goto(`${siteOrigin}/index.html`);
    await page.locator('#durable-workflow-analytics-consent').waitFor();
    await assertReferenceLayout(page, `root ${viewportName} initial consent`, 'initial');
    assert.deepEqual(errors, [], `Root ${viewportName} browser render must be error-free.`);
    await context.close();
  }
}

const port = await availablePort();
const siteOrigin = `http://${SITE_HOSTNAME}:${port}`;
const pageUrl = `${siteOrigin}/index.html?token=must-not-leak#private-fragment`;
const server = spawn('php', ['-S', `127.0.0.1:${port}`, '-t', buildDirectory], {
  stdio: ['ignore', 'pipe', 'pipe'],
});

let browser;

try {
  await waitForServer(port);

  const launchOptions = {
    headless: true,
    args: [
      `--host-resolver-rules=MAP ${SITE_HOSTNAME} 127.0.0.1`,
      '--no-proxy-server',
    ],
  };
  if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH) {
    launchOptions.executablePath = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH;
  }
  browser = await chromium.launch(launchOptions);

  await validateReferenceLayouts(browser, siteOrigin);

  const unconsentedContext = await browser.newContext();
  const unconsentedGoogleRequests = [];
  await unconsentedContext.route('https://www.googletagmanager.com/**', async (route) => {
    unconsentedGoogleRequests.push(route.request().url());
    await route.abort();
  });
  const unconsentedPage = await unconsentedContext.newPage();
  await unconsentedPage.goto(pageUrl);
  await unconsentedPage.locator('#durable-workflow-analytics-consent').waitFor();
  assert.deepEqual(unconsentedGoogleRequests, [], 'Google must not load before the visitor grants consent.');
  assert.equal(await unconsentedPage.evaluate(() => typeof window.dataLayer), 'undefined');
  await unconsentedContext.close();

  const context = await browser.newContext();
  const googleRequests = [];
  await context.route('https://www.googletagmanager.com/**', async (route) => {
    googleRequests.push(route.request().url());
    await route.abort();
  });
  await context.addInitScript(({ consentKey, hostname }) => {
    if (window.location.hostname === hostname && window.localStorage.getItem(consentKey) === null) {
      window.localStorage.setItem(consentKey, 'granted');
    }
  }, { consentKey: CONSENT_KEY, hostname: SITE_HOSTNAME });
  await context.addCookies([
    { name: '_ga', value: 'GA1.1.111111111.222222222', url: siteOrigin },
    { name: '_ga_GHD1YHT442Y', value: 'GS1.1.333333333.1.0.333333333.0.0.0', domain: '.durable-workflow.com', path: '/' },
    { name: '_garden', value: 'unrelated-prefix-cookie', url: siteOrigin },
    { name: 'api-reference-preference', value: 'keep-me', url: siteOrigin },
  ]);
  const seededCookies = await context.cookies(siteOrigin);
  assert.equal(seededCookies.find(({ name }) => name === '_ga')?.domain, SITE_HOSTNAME);
  assert.equal(seededCookies.find(({ name }) => name === '_ga_GHD1YHT442Y')?.domain, '.durable-workflow.com');

  const page = await context.newPage();
  await page.goto(pageUrl);
  await page.locator('#durable-workflow-analytics-preferences').waitFor();
  await page.waitForFunction(() => window.dataLayer?.some((entry) => entry[0] === 'config'));

  const calls = await page.evaluate(() => window.dataLayer.map((entry) => Array.from(entry)));
  const configCalls = calls.filter(([command]) => command === 'config');
  assert.equal(configCalls.length, 1, 'A navigation must configure exactly one automatic page view.');
  assert.equal(calls.filter(([command, event]) => command === 'event' && event === 'page_view').length, 0);
  assert.equal(configCalls[0][1], 'G-HD1YHT442Y');
  assert.deepEqual(configCalls[0][2], {
    page_hostname: SITE_HOSTNAME,
    page_location: `https://${SITE_HOSTNAME}/`,
    page_path: '/',
    page_referrer: '',
    page_title: await page.title(),
    allow_ad_personalization_signals: false,
    allow_google_signals: false,
    anonymize_ip: true,
    cookie_domain: 'none',
    send_page_view: true,
  });
  assert.equal(googleRequests.length, 1, 'Granted consent must load one GA4 runtime.');

  googleRequests.length = 0;
  await page.locator('#durable-workflow-analytics-preferences').click();
  await Promise.all([
    page.waitForNavigation(),
    page.locator('[data-consent="denied"]').click(),
  ]);

  let cookies = await context.cookies(siteOrigin);
  assert.deepEqual(analyticsCookies(cookies), [], 'Withdrawal must remove host and parent-domain GA4 cookies.');
  assert.equal(cookies.find(({ name }) => name === '_garden')?.value, 'unrelated-prefix-cookie');
  assert.equal(cookies.find(({ name }) => name === 'api-reference-preference')?.value, 'keep-me');
  assert.equal(await page.evaluate((key) => window.localStorage.getItem(key), CONSENT_KEY), 'denied');
  assert.equal(await page.evaluate(() => document.getElementById('durable-workflow-ga4-loader')), null);
  assert.equal(await page.evaluate(() => typeof window.dataLayer), 'undefined');
  assert.deepEqual(googleRequests, [], 'The withdrawal reload must not load Google.');

  await page.reload();
  cookies = await context.cookies(siteOrigin);
  assert.deepEqual(analyticsCookies(cookies), [], 'GA4 cookies must remain absent after a denied-state reload.');
  assert.equal(await page.evaluate(() => typeof window.dataLayer), 'undefined');
  assert.deepEqual(googleRequests, [], 'A denied-state reload must not load Google or emit analytics.');
  await context.close();

  process.stdout.write('Validated responsive root and nested API-reference layouts plus fail-closed analytics consent in Chromium.\n');
} finally {
  await browser?.close();
  server.kill('SIGTERM');
  await Promise.race([
    once(server, 'exit'),
    new Promise((resolve) => setTimeout(resolve, 1000)),
  ]);
}
