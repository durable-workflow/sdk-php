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
  ['compact-height', {width: 1280, height: 360}],
  ['desktop', {width: 1440, height: 900}],
  ['intermediate-landscape', {width: 1024, height: 768}],
  ['intermediate-portrait', {width: 768, height: 1024}],
  ['mobile', {width: 390, height: 844}],
  ['compact-mobile', {width: 320, height: 844}],
  ['compact-narrow-height', {width: 640, height: 360}],
  ['compact-mobile-height', {width: 390, height: 360}],
];
const pages = [
  ['neighboring API', '/classes/DurableWorkflow-Worker-WorkflowContext.html'],
  ['Client API', '/classes/DurableWorkflow-Client.html'],
  ['root', '/index.html'],
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
      if (element.closest('[inert]')) continue;
      if (
        element.closest('.phpdocumentor-sidebar')
        && !document.querySelector('.phpdocumentor-sidebar__menu-button')?.checked
      ) continue;
      const style = getComputedStyle(element);
      const box = element.getBoundingClientRect();
      const visibleBox = {
        top: Math.max(0, box.top),
        right: Math.min(innerWidth, box.right),
        bottom: Math.min(innerHeight, box.bottom),
        left: Math.max(0, box.left),
      };
      let excludedByAncestor = false;
      let clippedByAncestor = false;
      for (let parent = element.parentElement; parent; parent = parent.parentElement) {
        const parentStyle = getComputedStyle(parent);
        excludedByAncestor ||= parentStyle.visibility === 'hidden'
          || parentStyle.display === 'none'
          || parentStyle.pointerEvents === 'none'
          || Number(parentStyle.opacity) === 0;
        const parentBox = parent.getBoundingClientRect();
        if (['auto', 'hidden', 'scroll', 'clip'].includes(parentStyle.overflowX)) {
          clippedByAncestor ||= parentBox.left > box.left || parentBox.right < box.right;
          visibleBox.left = Math.max(visibleBox.left, parentBox.left);
          visibleBox.right = Math.min(visibleBox.right, parentBox.right);
        }
        if (['auto', 'hidden', 'scroll', 'clip'].includes(parentStyle.overflowY)) {
          clippedByAncestor ||= parentBox.top > box.top || parentBox.bottom < box.bottom;
          visibleBox.top = Math.max(visibleBox.top, parentBox.top);
          visibleBox.bottom = Math.min(visibleBox.bottom, parentBox.bottom);
        }
      }
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
      const visibleWidth = Math.max(0, visibleBox.right - visibleBox.left);
      const visibleHeight = Math.max(0, visibleBox.bottom - visibleBox.top);
      if (visibleWidth <= 0 || visibleHeight <= 0) continue;

      const center = {x: box.left + box.width / 2, y: box.top + box.height / 2};
      if (center.x < 0 || center.x >= innerWidth || center.y < 0 || center.y >= innerHeight) continue;
      const centerHit = center.x >= visibleBox.left
        && center.x < visibleBox.right
        && center.y >= visibleBox.top
        && center.y < visibleBox.bottom
        ? document.elementFromPoint(center.x, center.y)
        : null;
      const centerReachable = Boolean(centerHit === element
        || element.contains(centerHit)
        || centerHit?.contains(element));
      if (!centerReachable) {
        unreachable.push({
          element: element.outerHTML.slice(0, 180),
          clippedByAncestor,
          box: box.toJSON(),
          visibleBox,
        });
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

async function assertTableOfContentsMetadataLegibility(page, label) {
  const result = await page.evaluate(() => {
    const readableWidth = 12 * Number.parseFloat(getComputedStyle(document.documentElement).fontSize);
    const failures = [...document.querySelectorAll(
      '.phpdocumentor-content > section:first-of-type .phpdocumentor-table-of-contents__entry',
    )].flatMap(entry => {
      const metadata = entry.querySelector(':scope > span');
      if (!metadata?.textContent.trim()) return [];

      const entryBox = entry.getBoundingClientRect();
      const metadataBox = metadata.getBoundingClientRect();
      const minimumWidth = Math.min(readableWidth, entryBox.width);
      return metadataBox.width + 1 < minimumWidth
        ? [{
          entry: entry.outerHTML.slice(0, 180),
          entryWidth: entryBox.width,
          metadataWidth: metadataBox.width,
          minimumWidth,
        }]
        : [];
    });

    return {
      metadataCount: document.querySelectorAll(
        '.phpdocumentor-content > section:first-of-type .phpdocumentor-table-of-contents__entry > span',
      ).length,
      failures,
    };
  });

  assert.ok(result.metadataCount > 0, `${label} lost its table-of-contents metadata`);
  assert.deepEqual(result.failures, [], `${label} collapsed table-of-contents metadata`);
}

async function assertOnThisPageUtilityReachability(page, label) {
  const result = await page.evaluate(async () => {
    const scrollport = document.querySelector('.phpdocumentor-on-this-page__content');
    if (!scrollport) return {present: false};

    const settle = () => new Promise(resolve => {
      requestAnimationFrame(() => requestAnimationFrame(resolve));
    });
    const originalScrollTop = scrollport.scrollTop;
    const initialScrollportBox = scrollport.getBoundingClientRect();
    const failures = [];
    const entries = [...scrollport.querySelectorAll('a[href]')];

    for (const entry of entries) {
      let scrollportBox = scrollport.getBoundingClientRect();
      let entryBox = entry.getBoundingClientRect();
      const entryCenterInContent = entryBox.top - scrollportBox.top
        + scrollport.scrollTop
        + entryBox.height / 2;
      scrollport.scrollTop = Math.max(
        0,
        Math.min(
          scrollport.scrollHeight - scrollport.clientHeight,
          entryCenterInContent - scrollport.clientHeight / 2,
        ),
      );
      await settle();

      scrollportBox = scrollport.getBoundingClientRect();
      entryBox = entry.getBoundingClientRect();
      const center = {x: entryBox.left + entryBox.width / 2, y: entryBox.top + entryBox.height / 2};
      const centerInsideScrollport = center.x >= scrollportBox.left
        && center.x < scrollportBox.right
        && center.y >= Math.max(0, scrollportBox.top)
        && center.y < Math.min(innerHeight, scrollportBox.bottom);
      const hit = centerInsideScrollport ? document.elementFromPoint(center.x, center.y) : null;
      const centerReachable = Boolean(hit === entry || entry.contains(hit) || hit?.contains(entry));
      if (!centerInsideScrollport || !centerReachable) {
        failures.push({
          entry: entry.outerHTML.slice(0, 180),
          center,
          centerInsideScrollport,
          centerReachable,
          scrollportBox: scrollportBox.toJSON(),
        });
      }
    }

    scrollport.scrollTop = originalScrollTop;
    await settle();
    return {
      present: true,
      viewportHeight: innerHeight,
      scrollportBox: initialScrollportBox.toJSON(),
      entryCount: entries.length,
      failures,
    };
  });

  if (!result.present) return;
  assert.ok(result.entryCount > 0, `${label} lost its On this page entries`);
  assert.ok(
    result.scrollportBox.top >= 0 && result.scrollportBox.bottom <= result.viewportHeight - 1,
    `${label} On this page scrollport extends outside the usable viewport: ${JSON.stringify(result.scrollportBox)}`,
  );
  assert.deepEqual(result.failures, [], `${label} has unreachable On this page entries`);
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

async function floatingUtilityCandidateScrollPositions(page) {
  return page.evaluate(() => {
    const utility = document.querySelector('.phpdocumentor-back-to-top');
    const content = document.querySelector('.phpdocumentor-content');
    if (!utility || !content) return [];

    const utilityBox = utility.getBoundingClientRect();
    const maximumScroll = Math.max(0, document.documentElement.scrollHeight - innerHeight);
    const positions = new Set([0, maximumScroll]);

    function addCandidate(box) {
      const horizontalOverlap = Math.min(box.right, utilityBox.right) - Math.max(box.left, utilityBox.left);
      if (horizontalOverlap <= 1 || box.width < 1 || box.height < 1) return;

      const documentTop = box.top + scrollY;
      const documentBottom = box.bottom + scrollY;
      const centered = documentTop - utilityBox.top - Math.max(0, (utilityBox.height - box.height) / 2);
      const position = Math.max(0, Math.min(maximumScroll, centered));
      const projectedTop = documentTop - position;
      const projectedBottom = documentBottom - position;
      const verticalOverlap = Math.min(projectedBottom, utilityBox.bottom) - Math.max(projectedTop, utilityBox.top);
      if (verticalOverlap > 1) positions.add(Math.round(position));
    }

    const walker = document.createTreeWalker(content, NodeFilter.SHOW_TEXT);
    for (let node = walker.nextNode(); node; node = walker.nextNode()) {
      const parent = node.parentElement;
      if (!parent || !node.textContent.trim() || parent.closest('script, style, .visually-hidden')) continue;
      const style = getComputedStyle(parent);
      if (
        style.visibility === 'hidden'
        || style.display === 'none'
        || style.pointerEvents === 'none'
        || Number(style.opacity) === 0
      ) continue;
      const range = document.createRange();
      range.selectNodeContents(node);
      for (const box of range.getClientRects()) addCandidate(box);
    }

    for (const element of content.querySelectorAll('canvas, img, input, button, select, table, textarea, video')) {
      const style = getComputedStyle(element);
      if (style.visibility !== 'hidden' && style.display !== 'none' && Number(style.opacity) !== 0) {
        addCandidate(element.getBoundingClientRect());
      }
    }

    return [...positions].sort((first, second) => first - second);
  });
}

async function assertFloatingUtilityGeometry(page, label, expectedVisible = true) {
  const utility = page.locator('.phpdocumentor-back-to-top');
  assert.equal(await utility.count(), 1, `${label} lost its back-to-top utility`);
  assert.equal(await utility.isVisible(), expectedVisible, `${label} back-to-top visibility drifted`);
  if (!expectedVisible) return;

  const positions = await floatingUtilityCandidateScrollPositions(page);
  for (const position of positions) {
    await page.evaluate(scrollPosition => new Promise(resolve => {
      scrollTo(0, scrollPosition);
      requestAnimationFrame(() => requestAnimationFrame(resolve));
    }), position);
    await assertReachableControls(page, `${label} at scroll ${position}`);
    await assertFloatingUtilitiesClearReadableContent(page, `${label} at scroll ${position}`);
  }

  const maximumScroll = positions.at(-1) ?? 0;
  if (maximumScroll > 0) {
    await page.evaluate(scrollPosition => scrollTo(0, scrollPosition), maximumScroll);
    await utility.click();
    await page.waitForFunction(() => scrollY === 0);
  }
}

async function exercisePage(browser, origin, viewportName, viewport, pageName, pagePath, edgeInjected = false) {
  const context = await browser.newContext({viewport, reducedMotion: 'reduce'});
  const page = await context.newPage();
  const consoleErrors = [];
  const pageErrors = [];
  const requestFailures = [];
  const httpErrors = [];
  const rumRequests = [];
  let renderedPagePath = pagePath;
  let edgeFixturePath;
  const compactQuickstart = pageName === 'root' && viewport.width <= 549;
  page.on('console', message => {
    if (message.type() === 'error') consoleErrors.push(message.text());
  });
  page.on('pageerror', error => pageErrors.push(error.message));
  page.on('requestfailed', request => {
    requestFailures.push(`${request.method()} ${request.url()}: ${request.failure()?.errorText ?? 'unknown failure'}`);
  });
  page.on('response', response => {
    if (response.status() >= 400) httpErrors.push(`${response.status()} ${response.request().method()} ${response.url()}`);
  });
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
        status: 200,
        contentType: 'text/plain',
        body: 'ok',
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
    if (pageName === 'Client API') {
      await assertTableOfContentsMetadataLegibility(page, `${label} default`);
    }
    if (viewportName === 'compact-height') {
      await assertOnThisPageUtilityReachability(page, `${label} default`);
    }
    if (edgeInjected) {
      assert.deepEqual(consoleErrors, [], `${label} emitted console errors`);
      assert.deepEqual(pageErrors, [], `${label} emitted page errors`);
      assert.deepEqual(requestFailures, [], `${label} emitted request failures`);
      assert.deepEqual(httpErrors, [], `${label} emitted HTTP errors`);
      return;
    }
    await assertFloatingUtilityGeometry(page, `${label} default`, !compactQuickstart);

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
    assert.ok(
      await page.locator('[data-api-reference-search-background][inert]').count() >= 3,
      `${label} search did not isolate its background`,
    );
    assert.equal(await search.isEditable(), true, `${label} search could not refine its query`);
    await search.press('Control+A');
    await search.pressSequentially('Client');
    await page.waitForTimeout(350);
    await page.waitForFunction(() => (
      document.querySelector('.phpdocumentor-search__field')?.value === 'Client'
      && document.querySelectorAll('.phpdocumentor-search-results__entry').length > 0
    ));
    await assertReachableControls(page, `${label} open search`);
    await assertFloatingUtilitiesClearReadableContent(page, `${label} open search`, '.phpdocumentor-search-results');
    await page.locator('.phpdocumentor-search-results__close').click();
    await page.waitForFunction(() => (
      document.querySelector('.phpdocumentor-search-results')
        ?.classList.contains('phpdocumentor-search-results--hidden')
      && !document.querySelector('[data-api-reference-search-background]')
    ));
    assert.equal(
      await page.locator('.phpdocumentor-back-to-top').isVisible(),
      !compactQuickstart,
      `${label} did not restore its back-to-top utility`,
    );
    await assertReachableControls(page, `${label} after closing search`);
    assert.deepEqual(consoleErrors, [], `${label} emitted console errors`);
    assert.deepEqual(pageErrors, [], `${label} emitted page errors`);
    assert.deepEqual(requestFailures, [], `${label} emitted request failures`);
    assert.deepEqual(httpErrors, [], `${label} emitted HTTP errors`);
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
  const desktopViewport = viewports.find(([name]) => name === 'desktop')?.[1];
  assert.ok(desktopViewport);
  await exercisePage(browser, origin, 'desktop', desktopViewport, 'nested API', pages[1][1], true);
  process.stdout.write('Validated responsive floating-control geometry and browser evidence on root, Client, and neighboring PHP reference pages.\n');
} finally {
  await browser?.close();
  server.kill('SIGTERM');
  await Promise.race([once(server, 'exit'), new Promise(resolve => setTimeout(resolve, 1000))]);
}
