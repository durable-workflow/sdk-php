import assert from 'node:assert/strict';
import process from 'node:process';
import {setTimeout as delay} from 'node:timers/promises';
import {pathToFileURL} from 'node:url';
import {chromium} from 'playwright';

const SITE_HOSTNAME = 'php.durable-workflow.com';
const PAGE_PATH = '/namespaces/durableworkflow-worker.html';
export const BEACON_URL = 'https://static.cloudflareinsights.com/beacon.min.js';
export const RUM_URL = 'https://cloudflareinsights.com/cdn-cgi/rum';

const DEFAULT_TARGET_URL = new URL(PAGE_PATH, `https://${SITE_HOSTNAME}`);
const RUM_ENDPOINT = new URL(RUM_URL);

function isRumRequest(url) {
  return url.hostname === RUM_ENDPOINT.hostname && url.pathname === RUM_ENDPOINT.pathname;
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

export async function qualifyDeployedAnalytics() {
  const browser = await chromium.launch({headless: true});
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
  } finally {
    await browser.close();
  }

  process.stdout.write('Confirmed a successful deployed Cloudflare beacon module and RUM request.\n');
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await qualifyDeployedAnalytics();
}
