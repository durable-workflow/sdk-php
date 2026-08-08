import assert from 'node:assert/strict';
import process from 'node:process';
import {setTimeout as delay} from 'node:timers/promises';
import {chromium} from 'playwright';

const SITE_HOSTNAME = 'php.durable-workflow.com';
const PAGE_PATH = '/namespaces/durableworkflow-worker.html';
const BEACON_URL = 'https://static.cloudflareinsights.com/beacon.min.js';
const RUM_HOSTNAME = 'cloudflareinsights.com';
const RUM_PATHNAME = '/cdn-cgi/rum';
const GRAPHQL_URL = 'https://api.cloudflare.com/client/v4/graphql';
const ACCOUNT_ID = process.env.CLOUDFLARE_ACCOUNT_ID;
const API_TOKEN = process.env.CLOUDFLARE_ANALYTICS_API_TOKEN;

if (!/^[a-f0-9]{32}$/.test(ACCOUNT_ID || '')) {
  throw new Error('CLOUDFLARE_ACCOUNT_ID must be configured for deployed analytics qualification.');
}
if (!API_TOKEN) {
  throw new Error('CLOUDFLARE_ANALYTICS_API_TOKEN must be configured for deployed analytics qualification.');
}

const startedAt = new Date(Date.now() - 2 * 60_000).toISOString();
const targetUrl = new URL(PAGE_PATH, `https://${SITE_HOSTNAME}`);

async function observedPageViews() {
  const response = await fetch(GRAPHQL_URL, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${API_TOKEN}`,
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      query: `query AnalyticsQualification($accountTag: string, $start: Time) {
        viewer {
          accounts(filter: {accountTag: $accountTag}) {
            rows: rumPageloadEventsAdaptiveGroups(
              filter: {
                datetime_geq: $start
                requestHost: "${SITE_HOSTNAME}"
                requestPath: "${PAGE_PATH}"
              }
              limit: 1
            ) {
              count
            }
          }
        }
      }`,
      variables: {accountTag: ACCOUNT_ID, start: startedAt},
    }),
  });
  if (!response.ok) {
    throw new Error(`Cloudflare analytics query failed with HTTP ${response.status}.`);
  }

  const payload = await response.json();
  if (payload.errors?.length) {
    throw new Error(`Cloudflare analytics query failed: ${payload.errors.map(error => error.message).join('; ')}`);
  }
  const accounts = payload.data?.viewer?.accounts;
  if (!Array.isArray(accounts) || accounts.length !== 1) {
    throw new Error('Cloudflare analytics query did not return the configured account.');
  }
  const rows = accounts[0].rows;
  if (!Array.isArray(rows)) {
    throw new Error('Cloudflare analytics query did not return page-load rows.');
  }
  return rows.reduce((total, row) => total + Number(row.count || 0), 0);
}

const baseline = await observedPageViews();
const browser = await chromium.launch({headless: true});
try {
  const context = await browser.newContext({
    reducedMotion: 'reduce',
    viewport: {width: 1440, height: 900},
  });
  const page = await context.newPage();
  const errors = [];
  let beaconResponseStatus;
  let rumResponseStatus;

  page.on('console', message => {
    if (message.type() === 'error') errors.push('browser console error');
  });
  page.on('pageerror', () => errors.push('uncaught browser page error'));
  page.on('requestfailed', request => {
    const url = new URL(request.url());
    if (url.href.startsWith(BEACON_URL) || (url.hostname === RUM_HOSTNAME && url.pathname === RUM_PATHNAME)) {
      errors.push(`Cloudflare request failed: ${url.hostname}${url.pathname}`);
    }
  });
  page.on('response', response => {
    const url = new URL(response.url());
    if (url.href.startsWith(BEACON_URL)) beaconResponseStatus = response.status();
    if (url.hostname === RUM_HOSTNAME && url.pathname === RUM_PATHNAME) rumResponseStatus = response.status();
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

  for (let attempt = 0; attempt < 30 && rumResponseStatus === undefined; attempt += 1) {
    await delay(500);
  }
  if (rumResponseStatus === undefined) {
    await page.goto('about:blank');
    for (let attempt = 0; attempt < 30 && rumResponseStatus === undefined; attempt += 1) {
      await delay(500);
    }
  }
  assert(rumResponseStatus && rumResponseStatus >= 200 && rumResponseStatus < 300, 'The deployed Cloudflare RUM request did not succeed.');
  assert.deepEqual(errors, [], 'The deployed analytics page emitted browser errors.');
  await context.close();
} finally {
  await browser.close();
}

for (let attempt = 0; attempt < 18; attempt += 1) {
  if (await observedPageViews() > baseline) {
    process.stdout.write('Confirmed a successful deployed Cloudflare RUM request and its authenticated analytics row.\n');
    process.exit(0);
  }
  if (attempt < 17) await delay(20_000);
}

throw new Error('The successful deployed page view did not appear in Cloudflare analytics within six minutes.');
