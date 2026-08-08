import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {readFile, unlink, writeFile} from 'node:fs/promises';
import net from 'node:net';
import path from 'node:path';
import process from 'node:process';
import {chromium} from 'playwright';

const SITE_HOSTNAME = 'php.durable-workflow.com';
const TEST_TOKEN = '00000000000000000000000000000000';
const BEACON_URL = 'https://static.cloudflareinsights.com/beacon.min.js';
const RUM_URL = 'https://cloudflareinsights.com/cdn-cgi/rum';
const buildDirectory = path.resolve(process.argv[2] ?? 'build/api');
const runtimeSource = (await readFile(path.join(buildDirectory, 'analytics/analytics.js'), 'utf8'))
  .replace('__CLOUDFLARE_WEB_ANALYTICS_TOKEN__', TEST_TOKEN);
const viewports = [
  ['desktop', {width: 1440, height: 900}],
  ['intermediate', {width: 768, height: 1024}],
  ['mobile', {width: 390, height: 844}],
  ['compact-mobile', {width: 320, height: 844}],
  ['compact-height', {width: 640, height: 360}],
  ['compact-mobile-height', {width: 390, height: 360}],
];
const pages = [
  ['root', '/index.html'],
  ['nested API', '/classes/DurableWorkflow-Worker-WorkflowContext.html'],
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
      overflowing: [...document.querySelectorAll('body *')].flatMap(element => {
        const box = element.getBoundingClientRect();
        return box.right > innerWidth + 1 || box.left < -1
          ? [{element: element.outerHTML.slice(0, 180), box: box.toJSON()}]
          : [];
      }).slice(0, 10),
    };
  }, scope);
  assert.equal(
    result.documentWidth,
    result.viewportWidth,
    `${label} has horizontal overflow: ${JSON.stringify(result.overflowing)}`,
  );
  assert.deepEqual(result.unreachable, [], `${label} has unreachable controls`);
}

async function assertFloatingUtilitiesClearReadableContent(page, label, scope = '.phpdocumentor-content') {
  const collisions = await page.evaluate((scopeSelector) => {
    function isVisible(element) {
      const style = getComputedStyle(element);
      const box = element.getBoundingClientRect();
      return style.visibility !== 'hidden'
        && style.display !== 'none'
        && style.pointerEvents !== 'none'
        && Number(style.opacity) !== 0
        && box.width > 0
        && box.height > 0
        && box.right > 0
        && box.left < innerWidth
        && box.bottom > 0
        && box.top < innerHeight;
    }

    function intersection(first, second) {
      const width = Math.min(first.right, second.right) - Math.max(first.left, second.left);
      const height = Math.min(first.bottom, second.bottom) - Math.max(first.top, second.top);
      return width > 0 && height > 0 ? width * height : 0;
    }

    function visibleTextBox(textBox, element) {
      const visible = {
        top: Math.max(0, textBox.top),
        right: Math.min(innerWidth, textBox.right),
        bottom: Math.min(innerHeight, textBox.bottom),
        left: Math.max(0, textBox.left),
      };
      for (let ancestor = element; ancestor; ancestor = ancestor.parentElement) {
        const style = getComputedStyle(ancestor);
        const box = ancestor.getBoundingClientRect();
        if (['auto', 'hidden', 'scroll', 'clip'].includes(style.overflowX)) {
          visible.left = Math.max(visible.left, box.left);
          visible.right = Math.min(visible.right, box.right);
        }
        if (['auto', 'hidden', 'scroll', 'clip'].includes(style.overflowY)) {
          visible.top = Math.max(visible.top, box.top);
          visible.bottom = Math.min(visible.bottom, box.bottom);
        }
      }
      visible.width = Math.max(0, visible.right - visible.left);
      visible.height = Math.max(0, visible.bottom - visible.top);
      return visible;
    }

    const controls = 'a[href], button, input:not([type="hidden"]), select, textarea, summary, [role="button"]';
    const utilities = [...document.querySelectorAll(controls)].filter(element => {
      const position = getComputedStyle(element).position;
      return isVisible(element)
        && (position === 'fixed' || position === 'sticky' || element.matches('[data-floating-utility], .phpdocumentor-back-to-top'));
    });
    const collisions = [];
    for (const root of document.querySelectorAll(scopeSelector)) {
      const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
      for (let node = walker.nextNode(); node; node = walker.nextNode()) {
        const parent = node.parentElement;
        if (!parent || !node.textContent.trim() || parent.closest('script, style, .visually-hidden')) continue;
        if (!isVisible(parent)) continue;
        const range = document.createRange();
        range.selectNodeContents(node);
        for (const textBox of range.getClientRects()) {
          const visibleBox = visibleTextBox(textBox, parent);
          if (
            visibleBox.width < 1
            || visibleBox.height < 1
          ) continue;
          for (const utility of utilities) {
            if (utility.contains(parent) || parent.contains(utility)) continue;
            const area = intersection(utility.getBoundingClientRect(), visibleBox);
            if (area > 1) {
              collisions.push({
                utility: utility.outerHTML.slice(0, 180),
                content: node.textContent.trim().replace(/\s+/g, ' ').slice(0, 100),
                overlapArea: Math.round(area),
                utilityBox: utility.getBoundingClientRect().toJSON(),
                contentBox: visibleBox,
              });
            }
          }
        }
      }
    }
    return collisions;
  }, scope);
  assert.deepEqual(collisions, [], `${label} has a floating utility over readable primary content`);
}

async function assertFloatingUtilityGeometry(page, label) {
  const utility = page.locator('.phpdocumentor-back-to-top');
  assert.equal(await utility.count(), 1, `${label} lost its back-to-top utility`);
  assert.equal(await utility.isVisible(), true, `${label} hid its back-to-top utility`);

  const maximumScroll = await page.evaluate(() => Math.max(0, document.documentElement.scrollHeight - innerHeight));
  const scrollStep = Math.max(160, Math.floor(page.viewportSize().height / 2));
  const positions = new Set([0, maximumScroll]);
  for (let position = scrollStep; position < maximumScroll; position += scrollStep) positions.add(position);
  for (const position of [...positions].sort((first, second) => first - second)) {
    await page.evaluate(scrollPosition => new Promise(resolve => {
      scrollTo(0, scrollPosition);
      requestAnimationFrame(() => requestAnimationFrame(resolve));
    }), position);
    await assertReachableControls(page, `${label} at scroll ${position}`);
    await assertFloatingUtilitiesClearReadableContent(page, `${label} at scroll ${position}`);
  }

  if (maximumScroll > 0) {
    await page.evaluate(scrollPosition => scrollTo(0, scrollPosition), maximumScroll);
    await utility.click();
    await page.waitForFunction(() => scrollY === 0);
  }
}

async function exercisePage(browser, origin, viewportName, viewport, pageName, pagePath, edgeInjected = false) {
  const context = await browser.newContext({viewport, reducedMotion: 'reduce'});
  const page = await context.newPage();
  const errors = [];
  const rumRequests = [];
  let renderedPagePath = pagePath;
  let edgeFixturePath;
  page.on('console', message => {
    if (message.type() === 'error') errors.push(`console: ${message.text()}`);
  });
  page.on('pageerror', error => errors.push(`page: ${error.message}`));
  const label = `${pageName} ${viewportName}${edgeInjected ? ' with edge injection' : ''}`;

  try {
    await page.route('**/analytics/analytics.js', route => route.fulfill({
      body: runtimeSource,
      contentType: 'application/javascript',
    }));
    await page.route(`${BEACON_URL}*`, route => route.fulfill({
      body: `window.__cloudflareBeaconExecutions = (window.__cloudflareBeaconExecutions || 0) + 1;
        fetch('${RUM_URL}', {method: 'POST', body: '{}'});`,
      contentType: 'application/javascript',
    }));
    await page.route(`${RUM_URL}*`, route => {
      rumRequests.push(route.request().method());
      return route.fulfill({
        status: 204,
        headers: {'access-control-allow-origin': '*'},
      });
    });
    if (edgeInjected) {
      const renderedHtml = await readFile(path.join(buildDirectory, pagePath), 'utf8');
      const edgeSnippet = `<script type="module" src="${BEACON_URL}" data-cf-beacon='{"token":"${TEST_TOKEN}"}'></script>`;
      edgeFixturePath = path.join(path.dirname(path.join(buildDirectory, pagePath)), 'analytics-edge-injection.html');
      renderedPagePath = `${path.posix.dirname(pagePath)}/analytics-edge-injection.html`;
      await writeFile(edgeFixturePath, renderedHtml.replace('</body>', `${edgeSnippet}</body>`));
    }

    const response = await page.goto(`${origin}${renderedPagePath}`, {waitUntil: 'networkidle'});
    assert.equal(response?.status(), 200, `${label} did not render`);
    await page.locator('.phpdocumentor-content').waitFor();
    await page.waitForFunction(() => window.__cloudflareBeaconExecutions === 1);
    const analytics = await page.evaluate(({beaconUrl}) => ({
      retiredUi: document.querySelectorAll('.dw-analytics-consent, .dw-analytics-preferences, #durable-workflow-analytics-consent, #durable-workflow-analytics-preferences').length,
      google: [...document.scripts].filter(script => /googletagmanager|google-analytics/.test(script.src)).length,
      runtimes: [...document.scripts].filter(script => script.src.endsWith('/analytics/analytics.js')).map(script => script.type),
      beacons: [...document.scripts].filter(script => script.src.startsWith(beaconUrl)).map(script => {
        let configuration;
        try {
          configuration = JSON.parse(script.dataset.cfBeacon || 'null');
        } catch (_error) {
          configuration = null;
        }
        return {
          tokenOnly: configuration !== null
            && Object.keys(configuration).length === 1
            && /^[a-f0-9]{32}$/.test(configuration.token),
          type: script.type,
          hasAsync: script.hasAttribute('async'),
          hasDefer: script.hasAttribute('defer'),
        };
      }),
      localStorageEntries: localStorage.length,
      sessionStorageEntries: sessionStorage.length,
      loaderIdCount: document.querySelectorAll('#durable-workflow-cloudflare-web-analytics').length,
      executions: window.__cloudflareBeaconExecutions,
    }), {beaconUrl: BEACON_URL});
    assert.equal(analytics.retiredUi, 0, `${label} restored retired analytics UI`);
    assert.equal(analytics.google, 0, `${label} restored Google analytics`);
    assert.deepEqual(analytics.runtimes, ['module'], `${label} must load one module eligibility runtime`);
    assert.deepEqual(analytics.beacons, [{tokenOnly: true, type: 'module', hasAsync: false, hasDefer: false}], `${label} must use one supported Cloudflare module loader`);
    assert.equal(analytics.loaderIdCount, edgeInjected ? 0 : 1, `${label} duplicate-beacon guard failed`);
    assert.equal(analytics.executions, 1, `${label} executed the beacon more than once`);
    assert.deepEqual(rumRequests, ['POST'], `${label} did not emit one successful RUM request`);
    assert.equal(analytics.localStorageEntries, 0, `${label} wrote local storage`);
    assert.equal(analytics.sessionStorageEntries, 0, `${label} wrote session storage`);
    assert.deepEqual(await context.cookies(), [], `${label} wrote cookies`);
    assert.equal(await page.locator('.phpdocumentor-title__link').count(), 1, `${label} lost its title link`);
    assert.equal(await page.locator('.phpdocumentor-search__field').count(), 1, `${label} lost search`);
    await assertReachableControls(page, `${label} default`);
    if (edgeInjected) {
      assert.deepEqual(errors, [], `${label} emitted browser errors`);
      return;
    }
    await assertFloatingUtilityGeometry(page, `${label} default`);

    const sidebarMenu = page.locator('.phpdocumentor-sidebar__menu-icon');
    if (await sidebarMenu.isVisible()) {
      await sidebarMenu.click();
      assert.equal(await page.locator('.phpdocumentor-sidebar__menu-button').isChecked(), true, `${label} sidebar did not open`);
      await assertReachableControls(page, `${label} open sidebar`);
      await assertFloatingUtilitiesClearReadableContent(page, `${label} open sidebar`, '.phpdocumentor-sidebar');
      await sidebarMenu.click();
    }

    const search = page.locator('.phpdocumentor-search__field');
    await search.pressSequentially('Workflow');
    await page.locator('.phpdocumentor-search-results:not(.phpdocumentor-search-results--hidden)').waitFor();
    assert.ok(await page.locator('.phpdocumentor-search-results__entry').count() > 0, `${label} search has no results`);
    await assertReachableControls(page, `${label} open search`, '.phpdocumentor-search-results');
    await assertFloatingUtilitiesClearReadableContent(page, `${label} open search`, '.phpdocumentor-search-results');
    assert.deepEqual(errors, [], `${label} emitted browser errors`);
  } finally {
    await context.close();
    if (edgeFixturePath) await unlink(edgeFixturePath);
  }
}

const port = await availablePort();
const server = spawn('php', ['-S', `127.0.0.1:${port}`, '-t', buildDirectory], {stdio: 'ignore'});
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
  const origin = `http://${SITE_HOSTNAME}:${port}`;
  for (const [viewportName, viewport] of viewports) {
    for (const [pageName, pagePath] of pages) {
      await exercisePage(browser, origin, viewportName, viewport, pageName, pagePath);
    }
  }
  await exercisePage(browser, origin, 'desktop', viewports[0][1], 'nested API', pages[1][1], true);
  process.stdout.write('Validated responsive floating-control geometry and one supported cookie-free Cloudflare module loader on root and nested PHP reference pages.\n');
} finally {
  await browser?.close();
  server.kill('SIGTERM');
  await Promise.race([once(server, 'exit'), new Promise(resolve => setTimeout(resolve, 1000))]);
}
