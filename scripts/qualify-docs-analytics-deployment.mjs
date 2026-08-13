import assert from 'node:assert/strict';
import process from 'node:process';
import {setTimeout as delay} from 'node:timers/promises';
import {pathToFileURL} from 'node:url';
import {chromium} from 'playwright';

const SITE_HOSTNAME = 'php.durable-workflow.com';
const PAGE_PATH = '/namespaces/durableworkflow-worker.html';
export const BEACON_URL = 'https://static.cloudflareinsights.com/beacon.min.js';
export const RUM_URL = 'https://cloudflareinsights.com/cdn-cgi/rum';
export const DEPLOYMENT_AUDIT_URL = `https://${SITE_HOSTNAME}/deployment-audit.json`;
export const DEPLOYMENT_AUDIT_SCHEMA = 'durable-workflow.sdk-php.docs-deployment/v1';
export const PROMOTION_EVENT_URL = 'https://cloud.durable-workflow.com/early-access/promotion-events';
export const PROMOTION_SOURCE = 'sdk-php-reference';
export const PROMOTION_DESTINATION = 'https://cloud.durable-workflow.com/early-access#source=sdk-php-reference';
export const QUALIFICATION_EVENT = 'qualification';

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
const SOURCE_REVISION_PATTERN = /^[a-f0-9]{40}$/;

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

function promotionQualificationRewriteScript(eventUrl, source) {
  return `
    (() => {
      const eventUrl = ${JSON.stringify(eventUrl)};
      const source = ${JSON.stringify(source)};
      const nativeFetch = window.fetch.bind(window);

      window.fetch = function (input, init) {
        const requestUrl = typeof input === 'string' ? input : input.url;
        if (requestUrl !== eventUrl) return nativeFetch(input, init);

        const options = init || {};
        let initiatedPayload = null;
        try {
          initiatedPayload = JSON.parse(options.body);
        } catch (_error) {
          // The qualification fails on the recorded initiation shape below.
        }
        window.recordPromotionQualificationInitiation(initiatedPayload);

        return nativeFetch(input, {
          ...options,
          body: JSON.stringify({source, event: ${JSON.stringify(QUALIFICATION_EVENT)}}),
        });
      };
    })();
  `;
}

export async function verifyDeployedRevision(sourceRevision, contract = {}) {
  assert.match(
    sourceRevision ?? '',
    SOURCE_REVISION_PATTERN,
    'The deployed PHP documentation source revision must be an exact commit SHA.',
  );
  const auditUrl = contract.auditUrl ?? DEPLOYMENT_AUDIT_URL;
  const fetchImpl = contract.fetchImpl ?? globalThis.fetch;
  const attempts = contract.attempts ?? 12;
  const retryDelayMs = contract.retryDelayMs ?? 10_000;
  assert(Number.isInteger(attempts) && attempts > 0, 'Deployment audit attempts must be a positive integer.');
  assert(Number.isFinite(retryDelayMs) && retryDelayMs >= 0, 'Deployment audit retry delay must be non-negative.');

  let lastObservation = 'the deployment audit was unavailable';
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const response = await fetchImpl(auditUrl, {
        cache: 'no-store',
        credentials: 'omit',
        headers: {accept: 'application/json'},
        redirect: 'error',
        referrerPolicy: 'no-referrer',
      });
      const contents = await response.text();
      let audit = null;
      try {
        audit = JSON.parse(contents);
      } catch (_error) {
        lastObservation = `the deployment audit returned invalid JSON with HTTP ${response.status}`;
      }
      if (
        response.status === 200
        && audit !== null
        && typeof audit === 'object'
        && !Array.isArray(audit)
        && Object.keys(audit).sort().join(',') === 'schema,source_revision'
        && audit.schema === DEPLOYMENT_AUDIT_SCHEMA
        && audit.source_revision === sourceRevision
      ) {
        return audit;
      }
      if (audit !== null) {
        lastObservation = `the deployment audit did not identify source revision ${sourceRevision}`;
      }
    } catch (error) {
      lastObservation = `the deployment audit request failed: ${error.message}`;
    }

    if (attempt < attempts) await delay(retryDelayMs);
  }

  assert.fail(`The exact deployed PHP documentation candidate was not confirmed: ${lastObservation}.`);
}

function normalizedHeaders(headers) {
  return Object.freeze(Object.fromEntries(
    Object.entries(headers).map(([name, value]) => [name.toLowerCase(), value]),
  ));
}

function promotionRequestContract(request, targetOrigin, source, event, eventUrl) {
  assert.deepEqual(JSON.parse(request.body || 'null'), {source, event});
  assert.equal(request.method, 'POST');
  assert.equal(request.url, eventUrl);
  const {headers} = request;
  assert.equal(headers.authorization, undefined, 'Promotion analytics sent authorization data.');
  assert.equal(headers.cookie, undefined, 'Promotion analytics sent cookies.');
  assert.equal(headers['content-type'], 'text/plain');
  assert.equal(headers.origin, targetOrigin, 'Promotion analytics did not send the documentation origin.');
  assert.equal(headers.referer, `${targetOrigin}/`, 'Promotion analytics exposed more than its origin referrer.');
  assert.equal(headers['sec-fetch-mode'], 'cors');
  assert.equal(headers['sec-fetch-site'], 'same-site');
}

async function promotionResponseContract(response, targetOrigin) {
  const headers = await response.allHeaders();
  assert.equal(response.status(), 204, 'The deployed receiver rejected promotion qualification.');
  assert.equal(
    headers['access-control-allow-origin'],
    targetOrigin,
    'The promotion receiver did not allow the documentation origin.',
  );
  assert.match(headers['cache-control'] ?? '', /(?:^|,)\s*no-store\s*(?:,|$)/i, 'The promotion response was cacheable.');
  assert(
    (headers.vary ?? '').split(',').map(value => value.trim().toLowerCase()).includes('origin'),
    'The promotion response did not vary by Origin.',
  );
  assert.equal(headers['set-cookie'], undefined, 'The promotion receiver wrote a cookie.');
}

export async function qualifyPromotionReceiverValidation(contract = {}) {
  const eventUrl = contract.eventUrl ?? PROMOTION_EVENT_URL;
  const targetOrigin = contract.targetOrigin ?? `https://${SITE_HOSTNAME}`;
  const fetchImpl = contract.fetchImpl ?? globalThis.fetch;
  const invalidPayloads = [
    {source: `${PROMOTION_SOURCE}-invalid`, event: QUALIFICATION_EVENT},
    {source: PROMOTION_SOURCE, event: `${QUALIFICATION_EVENT}-invalid`},
  ];

  for (const payload of invalidPayloads) {
    const response = await fetchImpl(eventUrl, {
      body: JSON.stringify(payload),
      cache: 'no-store',
      credentials: 'omit',
      headers: {
        'content-type': 'text/plain',
        origin: targetOrigin,
        referer: `${targetOrigin}/`,
      },
      method: 'POST',
      redirect: 'error',
    });
    assert.equal(
      response.status,
      422,
      `The promotion receiver accepted an invalid ${payload.source === PROMOTION_SOURCE ? 'event' : 'source'}.`,
    );
    assert.equal(response.headers.get('set-cookie'), null, 'Promotion validation wrote a cookie.');
  }
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
  const initiatedPromotionEvents = [];
  const ignoredNetworkRequests = new Set();
  const pendingPromotionRequests = new Map();

  function capturePageErrors(browserPage) {
    browserPage.on('console', message => {
      if (message.type() === 'error') errors.push(`console: ${message.text()}`);
    });
    browserPage.on('pageerror', error => errors.push(`page: ${error.message}`));
  }

  capturePageErrors(page);
  // Playwright can lose Chromium-generated fetch headers after this page is replaced by same-tab navigation.
  // Pair the raw request events now so the qualification asserts an immutable pre-navigation snapshot.
  const networkSession = await context.newCDPSession(page);
  await networkSession.send('Network.enable');
  function capturePromotionRequest(requestId) {
    const observation = pendingPromotionRequests.get(requestId);
    if (!observation?.request) return;
    if (observation.request.url !== eventUrl) {
      pendingPromotionRequests.delete(requestId);
      return;
    }
    if (!observation.headers) return;

    promotionRequests.push(Object.freeze({
      body: observation.request.postData ?? null,
      headers: normalizedHeaders({...observation.request.headers, ...observation.headers}),
      method: observation.request.method,
      url: observation.request.url,
    }));
    pendingPromotionRequests.delete(requestId);
  }
  networkSession.on('Network.requestWillBeSent', event => {
    if (event.request.url !== eventUrl) {
      if (pendingPromotionRequests.delete(event.requestId)) return;
      ignoredNetworkRequests.add(event.requestId);
      return;
    }
    const observation = pendingPromotionRequests.get(event.requestId) ?? {};
    observation.request = event.request;
    pendingPromotionRequests.set(event.requestId, observation);
    capturePromotionRequest(event.requestId);
  });
  networkSession.on('Network.requestWillBeSentExtraInfo', event => {
    if (ignoredNetworkRequests.delete(event.requestId)) return;
    const observation = pendingPromotionRequests.get(event.requestId) ?? {};
    observation.headers = event.headers;
    pendingPromotionRequests.set(event.requestId, observation);
    capturePromotionRequest(event.requestId);
  });
  await page.exposeFunction('recordPromotionQualificationInitiation', payload => {
    initiatedPromotionEvents.push(payload);
  });
  await page.addInitScript({content: promotionQualificationRewriteScript(eventUrl, source)});
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
  await waitForCount(promotionResponses, 1, 'Promotion qualification did not reach the deployed receiver');
  await waitForCount(promotionRequests, 1, 'Promotion qualification request metadata was not observable');
  await delay(150);
  assert.equal(promotionRequests.length, 1, 'The deployed page emitted more than one initial qualification.');
  assert.equal(promotionResponses.length, 1, 'The receiver returned more than one initial qualification response.');
  promotionRequestContract(promotionRequests[0], targetUrl.origin, source, QUALIFICATION_EVENT, eventUrl);
  await promotionResponseContract(promotionResponses[0], targetUrl.origin);
  assert.equal(await action.getAttribute('href'), destination, 'The promotion lost its public early-access destination.');

  await context.route(destinationUrl, async route => {
    await waitForCount(promotionResponses, 2, 'Promotion click qualification did not reach the deployed receiver');
    await route.continue();
  }, {times: 1});
  await Promise.all([
    page.waitForURL(url => {
      const resolvedUrl = new URL(url);
      resolvedUrl.hash = '';
      return resolvedUrl.href === destinationUrl;
    }, {waitUntil: 'load'}),
    action.click(),
  ]);
  const destinationPage = page;
  await waitForCount(promotionResponses, 2, 'Promotion click qualification did not reach the deployed receiver');
  await waitForCount(promotionRequests, 2, 'Promotion click request metadata was not observable');
  await delay(150);
  assert.equal(promotionRequests.length, 2, 'The deployed page emitted duplicate promotion events.');
  assert.equal(promotionResponses.length, 2, 'The receiver returned duplicate promotion responses.');
  promotionRequestContract(promotionRequests[1], targetUrl.origin, source, QUALIFICATION_EVENT, eventUrl);
  await promotionResponseContract(promotionResponses[1], targetUrl.origin);
  assert.deepEqual(
    initiatedPromotionEvents,
    [
      {source, event: 'impression'},
      {source, event: 'click'},
    ],
    'The deployed promotion did not initiate exactly one impression and one click.',
  );
  assert.equal(destinationPage.url(), destinationUrl, 'The public early-access form did not consume the source attribution fragment.');
  const destinationStatus = await destinationPage.evaluate(() => (
    performance.getEntriesByType('navigation')[0]?.responseStatus
  ));
  assert.equal(destinationStatus, 200, 'The public early-access form did not return HTTP 200.');
  const promotionSourceField = destinationPage.locator('input[type="hidden"][name="promotion_source"]');
  const destinationForm = destinationPage.locator('form').filter({has: promotionSourceField});
  await destinationForm.waitFor();
  assert.equal(
    await destinationForm.evaluate(form => form.action),
    destinationUrl,
    'The public early-access form lost its submission destination.',
  );
  assert.equal(
    await promotionSourceField.inputValue(),
    source,
    'The public early-access form did not retain the bounded promotion source.',
  );
  assert.equal(
    await destinationForm.locator('input[type="radio"][name="intent"]:checked').inputValue(),
    'cohort',
    'The public early-access form did not select the intended launch cohort.',
  );
  assert.deepEqual(errors, [], 'The deployed promotion contract emitted browser errors.');
}

export async function qualifyDeployedAnalytics({
  sourceRevision,
  revisionContract,
  browserType = chromium,
  analyticsQualifier = qualifyAnalyticsTransport,
  promotionQualifier = qualifyPromotionTransport,
  receiverBoundaryQualifier = qualifyPromotionReceiverValidation,
} = {}) {
  await verifyDeployedRevision(sourceRevision, revisionContract);
  await receiverBoundaryQualifier();

  const launchOptions = {headless: true};
  if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH) {
    launchOptions.executablePath = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH;
  }
  const browser = await browserType.launch(launchOptions);
  try {
    const context = await browser.newContext({
      reducedMotion: 'reduce',
      viewport: {width: 1440, height: 900},
    });
    try {
      await analyticsQualifier(context);
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
          await promotionQualifier(
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

  process.stdout.write(
    `Confirmed deployed revision ${sourceRevision}, Cloudflare transport, and all eight root/Client `
    + 'desktop, intermediate, mobile, and short-height checks through only the non-aggregating promotion '
    + 'qualification path while preserving bounded impression/click initiation and attributed destination behavior.\n',
  );
}

export function sourceRevisionArgument(args) {
  assert.deepEqual(
    args.slice(0, 1),
    ['--source-revision'],
    'Usage: qualify-docs-analytics-deployment.mjs --source-revision <40-character commit SHA>',
  );
  assert.equal(args.length, 2, 'The deployment qualifier accepts exactly one source revision.');
  assert.match(args[1], SOURCE_REVISION_PATTERN, 'The source revision must be a 40-character lowercase commit SHA.');
  return args[1];
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await qualifyDeployedAnalytics({sourceRevision: sourceRevisionArgument(process.argv.slice(2))});
}
