import assert from 'node:assert/strict';
import process from 'node:process';
import {setTimeout as delay} from 'node:timers/promises';
import {pathToFileURL} from 'node:url';
import {chromium} from 'playwright';

const SITE_HOSTNAME = 'php.durable-workflow.com';
const PAGE_PATH = '/namespaces/durableworkflow-worker.html';
export const BEACON_URL = 'https://static.cloudflareinsights.com/beacon.min.js';
export const RUM_URL = 'https://cloudflareinsights.com/cdn-cgi/rum';
export const PROMOTION_EVENT_URL = 'https://cloud.durable-workflow.com/early-access/promotion-events';
export const PROMOTION_SOURCE = 'sdk-php-reference';
export const PROMOTION_DESTINATION = 'https://cloud.durable-workflow.com/early-access#source=sdk-php-reference';

const DEFAULT_TARGET_URL = new URL(PAGE_PATH, `https://${SITE_HOSTNAME}`);
const RUM_ENDPOINT = new URL(RUM_URL);
const PROMOTION_VIEWPORTS = [
  ['desktop', {width: 1440, height: 900}],
  ['intermediate', {width: 768, height: 1024}],
  ['mobile', {width: 390, height: 844}],
  ['short-height', {width: 640, height: 360}],
];
const PROMOTION_PAGES = [
  ['root', '/'],
  ['Client API', '/classes/DurableWorkflow-Client.html'],
];

function isRumRequest(url) {
  return url.hostname === RUM_ENDPOINT.hostname && url.pathname === RUM_ENDPOINT.pathname;
}

async function waitForCount(items, count, description) {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    if (items.length >= count) return;
    await delay(100);
  }
  assert.fail(`${description}: observed ${items.length} of ${count} requests.`);
}

async function promotionRequestContract(request, targetOrigin, source, event) {
  const headers = await request.allHeaders();
  assert.deepEqual(JSON.parse(request.postData() || 'null'), {source, event});
  assert.equal(request.method(), 'POST');
  assert.equal(headers.authorization, undefined, 'Promotion analytics sent authorization data.');
  assert.equal(headers.cookie, undefined, 'Promotion analytics sent cookies.');
  assert.equal(headers['content-type'], 'text/plain');
  assert.equal(headers.origin, targetOrigin, 'Promotion analytics did not send the documentation origin.');
  assert.equal(headers.referer, `${targetOrigin}/`, 'Promotion analytics exposed more than its origin referrer.');
  assert.equal(headers['sec-fetch-mode'], 'cors');
  assert.equal(headers['sec-fetch-site'], 'same-site');
}

export async function qualifyAnalyticsTransport(context, target = DEFAULT_TARGET_URL) {
  const targetUrl = target instanceof URL ? target : new URL(target);
  const page = await context.newPage();
  const errors = [];
  let beaconResponseStatus;
  let rumResponse;

  page.on('console', message => {
    if (message.type() === 'error') errors.push('browser console error');
  });
  page.on('pageerror', () => errors.push('uncaught browser page error'));
  page.on('requestfailed', request => {
    const url = new URL(request.url());
    if (url.href.startsWith(BEACON_URL) || isRumRequest(url)) {
      errors.push(`Cloudflare request failed: ${url.hostname}${url.pathname}`);
    }
  });
  page.on('response', response => {
    const url = new URL(response.url());
    if (url.href.startsWith(BEACON_URL)) beaconResponseStatus = response.status();
    if (isRumRequest(url)) {
      rumResponse = {method: response.request().method(), status: response.status()};
    }
  });

  const response = await page.goto(targetUrl.href, {waitUntil: 'networkidle'});
  assert.equal(response?.status(), 200, 'The deployed nested API-reference page did not render.');
  await page.locator('.phpdocumentor-content').waitFor();
  await page.waitForFunction(({beaconUrl}) => (
    [...document.scripts].some(script => script.src.startsWith(beaconUrl))
  ), {beaconUrl: BEACON_URL});

  const contract = await page.evaluate(({beaconUrl}) => {
    const beacons = [...document.scripts].filter(script => script.src.startsWith(beaconUrl));
    const beacon = beacons[0];
    let configuration;
    try {
      configuration = JSON.parse(beacon?.dataset.cfBeacon || 'null');
    } catch (_error) {
      configuration = null;
    }
    return {
      beaconCount: beacons.length,
      module: beacon?.type === 'module',
      tokenOnly: configuration !== null
        && Object.keys(configuration).length === 1
        && /^[a-f0-9]{32}$/.test(configuration.token),
      deferred: beacon?.hasAttribute('defer') === true,
      retiredUiCount: document.querySelectorAll('.dw-analytics-consent, .dw-analytics-preferences, #durable-workflow-analytics-consent, #durable-workflow-analytics-preferences').length,
      googleCount: [...document.scripts].filter(script => /googletagmanager|google-analytics/.test(script.src)).length,
      localStorageEntries: localStorage.length,
      sessionStorageEntries: sessionStorage.length,
    };
  }, {beaconUrl: BEACON_URL});

  assert.deepEqual(contract, {
    beaconCount: 1,
    module: true,
    tokenOnly: true,
    deferred: false,
    retiredUiCount: 0,
    googleCount: 0,
    localStorageEntries: 0,
    sessionStorageEntries: 0,
  }, 'The deployed page does not satisfy the supported cookie-free Cloudflare loader contract.');
  assert.deepEqual(await context.cookies(targetUrl.origin), [], 'The deployed analytics page wrote cookies.');
  assert(beaconResponseStatus && beaconResponseStatus >= 200 && beaconResponseStatus < 300, 'The deployed Cloudflare beacon module request did not succeed.');

  for (let attempt = 0; attempt < 30 && rumResponse === undefined; attempt += 1) {
    await delay(500);
  }
  if (rumResponse === undefined) {
    await page.goto('about:blank');
    for (let attempt = 0; attempt < 30 && rumResponse === undefined; attempt += 1) {
      await delay(500);
    }
  }
  assert.equal(rumResponse?.method, 'POST', 'The deployed Cloudflare RUM request did not use POST.');
  assert(rumResponse.status >= 200 && rumResponse.status < 300, 'The deployed Cloudflare RUM request did not succeed.');
  assert.deepEqual(errors, [], 'The deployed analytics page emitted browser errors.');
}

export async function qualifyPromotionTransport(context, target, contract = {}) {
  const targetUrl = target instanceof URL ? target : new URL(target);
  const eventUrl = contract.eventUrl ?? PROMOTION_EVENT_URL;
  const source = contract.source ?? PROMOTION_SOURCE;
  const destination = contract.destination ?? PROMOTION_DESTINATION;
  const destinationUrl = destination.split('#')[0];
  const page = await context.newPage();
  const errors = [];
  const promotionRequests = [];
  const promotionResponses = [];

  page.on('console', message => {
    if (message.type() === 'error') errors.push(`console: ${message.text()}`);
  });
  page.on('pageerror', error => errors.push(`page: ${error.message}`));
  context.on('request', request => {
    if (request.url() === eventUrl) promotionRequests.push(request);
  });
  context.on('requestfailed', request => {
    if (request.url() === eventUrl && request.failure()?.errorText === 'net::ERR_ABORTED') return;
    if (request.url() === eventUrl || request.url().startsWith(destinationUrl)) {
      errors.push(`request: ${request.method()} ${request.url()} ${request.failure()?.errorText ?? ''}`);
    }
  });
  context.on('response', response => {
    if (response.url() === eventUrl) promotionResponses.push(response);
  });

  const response = await page.goto(targetUrl.href, {waitUntil: 'networkidle'});
  assert.equal(response?.status(), 200, `The deployed PHP reference page did not render: ${targetUrl.href}`);
  await page.locator('.phpdocumentor-content').waitFor();
  const promotion = page.locator(`[data-promotion-source="${source}"]`);
  const action = promotion.locator('[data-promotion-action="early-access"]');
  await promotion.waitFor();
  await waitForCount(promotionResponses, 1, 'Promotion impression did not reach the deployed receiver');
  await delay(150);
  assert.equal(promotionRequests.length, 1, 'The deployed page emitted more than one promotion impression.');
  assert.equal(promotionResponses.length, 1, 'The receiver returned more than one impression response.');
  assert.equal(promotionResponses[0].status(), 204, 'The deployed receiver rejected the promotion impression.');
  await promotionRequestContract(promotionRequests[0], targetUrl.origin, source, 'impression');
  assert.equal(await action.getAttribute('href'), destination, 'The promotion lost its public early-access destination.');

  const destinationPagePromise = context.waitForEvent('page');
  await action.click({modifiers: ['Control']});
  const destinationPage = await destinationPagePromise;
  await destinationPage.waitForLoadState('load');
  await waitForCount(promotionResponses, 2, 'Promotion click did not reach the deployed receiver');
  await delay(150);
  assert.equal(promotionRequests.length, 2, 'The deployed page emitted duplicate promotion events.');
  assert.equal(promotionResponses.length, 2, 'The receiver returned duplicate promotion responses.');
  assert.equal(promotionResponses[1].status(), 204, 'The deployed receiver rejected the promotion click.');
  await promotionRequestContract(promotionRequests[1], targetUrl.origin, source, 'click');
  assert.equal(destinationPage.url(), destination, 'The promotion did not resolve to the public early-access form.');
  const destinationResponse = await destinationPage.reload({waitUntil: 'load'});
  assert.equal(destinationResponse.status(), 200, 'The public early-access form did not return HTTP 200.');
  assert.deepEqual(errors, [], 'The deployed promotion contract emitted browser errors.');
}

export async function qualifyDeployedAnalytics() {
  const launchOptions = {headless: true};
  if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH) {
    launchOptions.executablePath = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH;
  }
  const browser = await chromium.launch(launchOptions);
  try {
    const context = await browser.newContext({
      reducedMotion: 'reduce',
      viewport: {width: 1440, height: 900},
    });
    try {
      await qualifyAnalyticsTransport(context);
    } finally {
      await context.close();
    }

    for (const [viewportName, viewport] of PROMOTION_VIEWPORTS) {
      for (const [pageName, pagePath] of PROMOTION_PAGES) {
        const promotionContext = await browser.newContext({
          reducedMotion: 'reduce',
          viewport,
        });
        try {
          await qualifyPromotionTransport(
            promotionContext,
            new URL(pagePath, `https://${SITE_HOSTNAME}`),
          );
        } catch (error) {
          error.message = `${pageName} at ${viewportName}: ${error.message}`;
          throw error;
        } finally {
          await promotionContext.close();
        }
      }
    }
  } finally {
    await browser.close();
  }

  process.stdout.write('Confirmed deployed Cloudflare transport and bounded PHP promotion impressions and clicks across required viewports.\n');
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await qualifyDeployedAnalytics();
}
