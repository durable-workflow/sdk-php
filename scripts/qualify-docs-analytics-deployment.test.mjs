import assert from 'node:assert/strict';
import {once} from 'node:events';
import {readFile} from 'node:fs/promises';
import http from 'node:http';
import {after, before, test} from 'node:test';
import process from 'node:process';
import {chromium} from 'playwright';
import {
  BEACON_URL,
  DEPLOYMENT_AUDIT_SCHEMA,
  qualifyDeployedAnalytics,
  qualifyAnalyticsTransport,
  qualifyPromotionReceiverValidation,
  qualifyPromotionTransport,
  PROMOTION_SOURCE,
  QUALIFICATION_EVENT,
  RUM_URL,
  sourceRevisionArgument,
  verifyDeployedRevision,
} from './qualify-docs-analytics-deployment.mjs';

const VALID_TOKEN = '00000000000000000000000000000000';
const SOURCE_REVISION = 'a'.repeat(40);
let browser;
let origin;
let promotionOrigin;
let promotionServer;
const promotionEvents = [];
let server;

function analyticsPage(token = VALID_TOKEN) {
  return `<!doctype html>
    <html>
      <body>
        <main class="phpdocumentor-content">API reference</main>
        <script type="module" src="${BEACON_URL}" data-cf-beacon='{"token":"${token}"}'></script>
      </body>
    </html>`;
}

function promotionPage(eventUrl, destination, {target = null} = {}) {
  const targetAttribute = target === null ? '' : ` target="${target}"`;
  return `<!doctype html>
    <html>
      <body>
        <main class="phpdocumentor-content">API reference</main>
        <aside data-promotion-source="${PROMOTION_SOURCE}">
          <a data-promotion-action="early-access" href="${destination}"${targetAttribute}>Request early access</a>
        </aside>
        <script>
          const sendPromotionEvent = event => fetch('${eventUrl}', {
            method: 'POST',
            mode: 'cors',
            credentials: 'omit',
            keepalive: true,
            referrerPolicy: 'origin',
            headers: {'Content-Type': 'text/plain'},
            body: JSON.stringify({source: '${PROMOTION_SOURCE}', event}),
          });
          document.querySelector('[data-promotion-action="early-access"]')
            .addEventListener('click', () => sendPromotionEvent('click'));
          sendPromotionEvent('impression');
        </script>
      </body>
    </html>`;
}

function earlyAccessPage({
  formPath = '/early-access',
  retainSource = true,
  selectedIntent = 'cohort',
} = {}) {
  return `<!doctype html>
    <html>
      <body>
        <main>
          <h1>Request Cloud early access</h1>
          <form action="${formPath}">
            <input type="hidden" name="promotion_source" value="">
            <label><input type="radio" name="intent" value="updates"> Product and pricing updates</label>
            <label><input type="radio" name="intent" value="evaluate"> Evaluate Cloud</label>
            <label><input type="radio" name="intent" value="cohort"> Join the launch cohort</label>
          </form>
        </main>
        <script>
          const source = new URLSearchParams(location.hash.slice(1)).get('source');
          if (source === '${PROMOTION_SOURCE}') {
            ${retainSource ? 'document.querySelector(\'[name="promotion_source"]\').value = source;' : ''}
            document.querySelector('[name="intent"][value="${selectedIntent}"]').checked = true;
          }
          history.replaceState(null, '', location.pathname + location.search);
        </script>
      </body>
    </html>`;
}

before(async () => {
  promotionServer = http.createServer(async (request, response) => {
    if (request.method === 'GET' && request.url === '/favicon.ico') {
      response.writeHead(204);
      response.end();
      return;
    }
    if (request.method === 'GET' && request.url === '/early-access') {
      response.writeHead(200, {'content-type': 'text/html'});
      response.end(earlyAccessPage());
      return;
    }
    if (request.method === 'GET' && request.url === '/unattributed-early-access') {
      response.writeHead(200, {'content-type': 'text/html'});
      response.end(earlyAccessPage({
        formPath: '/unattributed-early-access',
        retainSource: false,
      }));
      return;
    }
    if (request.method === 'GET' && request.url === '/wrong-intent-early-access') {
      response.writeHead(200, {'content-type': 'text/html'});
      response.end(earlyAccessPage({
        formPath: '/wrong-intent-early-access',
        selectedIntent: 'updates',
      }));
      return;
    }
    if (request.method !== 'POST') {
      response.writeHead(404);
      response.end();
      return;
    }

    let body = '';
    for await (const chunk of request) body += chunk;
    const parsedBody = JSON.parse(body);
    promotionEvents.push({
      body: parsedBody,
      headers: request.headers,
      method: request.method,
      path: request.url,
    });
    const boundedQualification = Object.keys(parsedBody).sort().join(',') === 'event,source'
      && parsedBody.source === PROMOTION_SOURCE
      && parsedBody.event === 'qualification';
    const responseStatus = request.url === '/failed-promotion-events'
      ? 403
      : boundedQualification ? 204 : 422;
    response.writeHead(responseStatus, {
      'access-control-allow-origin': origin,
      'cache-control': 'no-store',
      vary: 'Origin',
    });
    response.end();
  });
  promotionServer.listen(0, '127.0.0.1');
  await once(promotionServer, 'listening');
  const promotionAddress = promotionServer.address();
  assert(promotionAddress && typeof promotionAddress === 'object');
  promotionOrigin = `http://127.0.0.1:${promotionAddress.port}`;

  server = http.createServer((request, response) => {
    if (
      request.url === '/promotion'
      || request.url === '/failed-promotion'
      || request.url === '/unattributed-promotion'
      || request.url === '/wrong-intent-promotion'
    ) {
      const failed = request.url === '/failed-promotion';
      const destinationPath = {
        '/unattributed-promotion': '/unattributed-early-access',
        '/wrong-intent-promotion': '/wrong-intent-early-access',
      }[request.url] ?? '/early-access';
      response.writeHead(200, {'content-type': 'text/html'});
      response.end(promotionPage(
        `${promotionOrigin}/${failed ? 'failed-promotion-events' : 'promotion-events'}`,
        `${promotionOrigin}${destinationPath}#source=${PROMOTION_SOURCE}`,
        {target: request.url === '/wrong-intent-promotion' ? '_blank' : null},
      ));
      return;
    }
    response.writeHead(request.url === '/failed-page' ? 500 : 200, {'content-type': 'text/html'});
    response.end(analyticsPage(request.url === '/malformed-loader' ? 'missing' : VALID_TOKEN));
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  assert(address && typeof address === 'object');
  origin = `http://127.0.0.1:${address.port}`;
  const launchOptions = {headless: true};
  if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH) {
    launchOptions.executablePath = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH;
  }
  browser = await chromium.launch(launchOptions);
});

after(async () => {
  await browser?.close();
  if (server?.listening) {
    server.close();
    await once(server, 'close');
  }
  if (promotionServer?.listening) {
    promotionServer.close();
    await once(promotionServer, 'close');
  }
});

async function transportContext({beaconStatus = 200, rumMethod = 'POST', rumStatus = 200} = {}) {
  const context = await browser.newContext({
    reducedMotion: 'reduce',
    viewport: {width: 1440, height: 900},
  });
  const requests = {aggregation: [], rum: []};

  await context.route('https://api.cloudflare.com/**', route => {
    requests.aggregation.push(route.request().url());
    return route.abort();
  });
  await context.route(`${BEACON_URL}*`, route => route.fulfill({
    status: beaconStatus,
    contentType: 'application/javascript',
    headers: {'access-control-allow-origin': '*'},
    body: `fetch('${RUM_URL}', {method: '${rumMethod}', body: ${rumMethod === 'POST' ? "'{}'" : 'undefined'}});`,
  }));
  await context.route(`${RUM_URL}*`, route => {
    requests.rum.push(route.request().method());
    return route.fulfill({
      status: rumStatus,
      contentType: 'text/plain',
      headers: {'access-control-allow-origin': '*'},
      body: 'ok',
    });
  });

  return {context, requests};
}

test('successful browser transport does not depend on analytics aggregation', async () => {
  const {context, requests} = await transportContext();
  try {
    await qualifyAnalyticsTransport(context, `${origin}/valid`);
    assert.deepEqual(requests.rum, ['POST']);
    assert.deepEqual(requests.aggregation, []);
  } finally {
    await context.close();
  }
});

test('malformed loader credentials fail deployed transport qualification', async () => {
  const {context} = await transportContext();
  try {
    await assert.rejects(
      qualifyAnalyticsTransport(context, `${origin}/malformed-loader`),
      /supported cookie-free Cloudflare loader contract/,
    );
  } finally {
    await context.close();
  }
});

test('an unsuccessful deployed page fails transport qualification', async () => {
  const {context} = await transportContext();
  try {
    await assert.rejects(
      qualifyAnalyticsTransport(context, `${origin}/failed-page`),
      /nested API-reference page did not render/,
    );
  } finally {
    await context.close();
  }
});

test('failed beacon module requests fail deployed transport qualification', async () => {
  const {context} = await transportContext({beaconStatus: 503});
  try {
    await assert.rejects(
      qualifyAnalyticsTransport(context, `${origin}/failed-beacon`),
      /Cloudflare beacon module request did not succeed/,
    );
  } finally {
    await context.close();
  }
});

test('failed RUM posts fail deployed transport qualification', async () => {
  const {context} = await transportContext({rumStatus: 503});
  try {
    await assert.rejects(
      qualifyAnalyticsTransport(context, `${origin}/failed-rum`),
      /Cloudflare RUM request did not succeed/,
    );
  } finally {
    await context.close();
  }
});

test('a successful non-POST RUM request fails deployed transport qualification', async () => {
  const {context} = await transportContext({rumMethod: 'GET'});
  try {
    await assert.rejects(
      qualifyAnalyticsTransport(context, `${origin}/wrong-rum-method`),
      /Cloudflare RUM request did not use POST/,
    );
  } finally {
    await context.close();
  }
});

test('live promotion qualification uses only the non-aggregating receiver path', async () => {
  assert.equal(QUALIFICATION_EVENT, 'qualification');
  promotionEvents.length = 0;
  const context = await browser.newContext({
    reducedMotion: 'reduce',
    viewport: {width: 390, height: 844},
  });
  try {
    await context.addCookies([{
      name: 'private-session',
      value: 'must-not-leave-the-browser',
      url: promotionOrigin,
    }]);
    await qualifyPromotionTransport(context, `${origin}/promotion`, {
      destination: `${promotionOrigin}/early-access#source=${PROMOTION_SOURCE}`,
      eventUrl: `${promotionOrigin}/promotion-events`,
    });
    assert.equal(context.pages().length, 1, 'promotion qualification must follow the customer same-page navigation');
    assert.deepEqual(
      promotionEvents.map(event => ({
        body: event.body,
        headers: {
          origin: event.headers.origin,
          secFetchMode: event.headers['sec-fetch-mode'],
          secFetchSite: event.headers['sec-fetch-site'],
        },
        method: event.method,
        path: event.path,
      })),
      [
        {
          body: {source: PROMOTION_SOURCE, event: 'qualification'},
          headers: {origin, secFetchMode: 'cors', secFetchSite: 'same-site'},
          method: 'POST',
          path: '/promotion-events',
        },
        {
          body: {source: PROMOTION_SOURCE, event: 'qualification'},
          headers: {origin, secFetchMode: 'cors', secFetchSite: 'same-site'},
          method: 'POST',
          path: '/promotion-events',
        },
      ],
    );
  } finally {
    await context.close();
  }
});

test('live receiver validation fails closed for invalid sources and events', async () => {
  promotionEvents.length = 0;

  await qualifyPromotionReceiverValidation({
    eventUrl: `${promotionOrigin}/promotion-events`,
    targetOrigin: origin,
  });

  assert.deepEqual(
    promotionEvents.map(event => event.body),
    [
      {source: `${PROMOTION_SOURCE}-invalid`, event: QUALIFICATION_EVENT},
      {source: PROMOTION_SOURCE, event: `${QUALIFICATION_EVENT}-invalid`},
    ],
  );
  assert(promotionEvents.every(event => event.headers.cookie === undefined));
  assert(promotionEvents.every(event => event.headers.authorization === undefined));
  assert(promotionEvents.every(event => event.headers.origin === origin));
  assert(promotionEvents.every(event => event.headers.referer === `${origin}/`));
});

test('promotion qualification rejects an unsuccessful receiver response', async () => {
  const context = await browser.newContext({
    reducedMotion: 'reduce',
    viewport: {width: 1440, height: 900},
  });
  try {
    await assert.rejects(
      qualifyPromotionTransport(context, `${origin}/failed-promotion`, {
        destination: `${promotionOrigin}/early-access#source=${PROMOTION_SOURCE}`,
        eventUrl: `${promotionOrigin}/failed-promotion-events`,
      }),
      /receiver rejected promotion qualification/,
    );
  } finally {
    await context.close();
  }
});

test('promotion qualification rejects a destination that drops source attribution', async () => {
  const context = await browser.newContext({
    reducedMotion: 'reduce',
    viewport: {width: 1440, height: 900},
  });
  try {
    await assert.rejects(
      qualifyPromotionTransport(context, `${origin}/unattributed-promotion`, {
        destination: `${promotionOrigin}/unattributed-early-access#source=${PROMOTION_SOURCE}`,
        eventUrl: `${promotionOrigin}/promotion-events`,
      }),
      /did not retain the bounded promotion source/,
    );
  } finally {
    await context.close();
  }
});

test('promotion qualification rejects a destination that selects the wrong intent', async () => {
  const context = await browser.newContext({
    reducedMotion: 'reduce',
    viewport: {width: 1440, height: 900},
  });
  try {
    await assert.rejects(
      qualifyPromotionTransport(context, `${origin}/wrong-intent-promotion`, {
        destination: `${promotionOrigin}/wrong-intent-early-access#source=${PROMOTION_SOURCE}`,
        eventUrl: `${promotionOrigin}/promotion-events`,
        navigationTimeoutMs: 2_000,
      }),
      /did not select the intended launch cohort/,
    );
  } finally {
    await context.close();
  }
});

test('the deployment workflow keeps credential validation but has no aggregation dependency', async () => {
  const workflow = await readFile(new URL('../.github/workflows/docs.yml', import.meta.url), 'utf8');
  const qualification = await readFile(new URL('./qualify-docs-analytics-deployment.mjs', import.meta.url), 'utf8');

  assert.match(workflow, /CLOUDFLARE_WEB_ANALYTICS_TOKEN: \$\{\{ vars\.CLOUDFLARE_WEB_ANALYTICS_TOKEN \}\}/);
  assert.match(workflow, /build\/site\/deployment-audit\.json/);
  assert.match(workflow, /durable-workflow\.sdk-php\.docs-deployment\/v1/);
  assert.match(
    workflow,
    /npm run qualify:docs-analytics-deployment --\s+--source-revision "\$\{\{ github\.sha \}\}"/,
  );
  assert.doesNotMatch(workflow, /CLOUDFLARE_ACCOUNT_ID|CLOUDFLARE_ANALYTICS_API_TOKEN/);
  assert.doesNotMatch(qualification, /api\.cloudflare\.com|rumPageloadEventsAdaptiveGroups|observedPageViews/);
});

test('the deployment audit must identify the exact source revision', async () => {
  const observations = [
    {schema: DEPLOYMENT_AUDIT_SCHEMA, source_revision: 'b'.repeat(40)},
    {schema: DEPLOYMENT_AUDIT_SCHEMA, source_revision: SOURCE_REVISION},
  ];
  const requests = [];

  await verifyDeployedRevision(SOURCE_REVISION, {
    attempts: 2,
    retryDelayMs: 0,
    fetchImpl: async (url, options) => {
      requests.push({url, options});
      return {
        status: 200,
        text: async () => JSON.stringify(observations.shift()),
      };
    },
  });

  assert.equal(requests.length, 2);
  assert(requests.every(request => request.options.credentials === 'omit'));
  assert(requests.every(request => request.options.redirect === 'error'));
});

test('live browser verification cannot begin before the deployed revision matches', async () => {
  let browserLaunched = false;

  await assert.rejects(
    qualifyDeployedAnalytics({
      sourceRevision: SOURCE_REVISION,
      revisionContract: {
        attempts: 1,
        retryDelayMs: 0,
        fetchImpl: async () => ({
          status: 200,
          text: async () => JSON.stringify({
            schema: DEPLOYMENT_AUDIT_SCHEMA,
            source_revision: 'b'.repeat(40),
          }),
        }),
      },
      browserType: {
        launch: async () => {
          browserLaunched = true;
          throw new Error('browser launch must not be reached');
        },
      },
      receiverBoundaryQualifier: async () => {
        throw new Error('receiver validation must not be reached');
      },
    }),
    /exact deployed PHP documentation candidate was not confirmed/,
  );

  assert.equal(browserLaunched, false);
});

test('deployment qualification retains the eight-page viewport matrix', async () => {
  const calls = [];
  const browser = {
    close: async () => calls.push('browser-close'),
    newContext: async options => ({
      close: async () => calls.push(['context-close', options.viewport]),
      options,
    }),
  };

  await qualifyDeployedAnalytics({
    sourceRevision: SOURCE_REVISION,
    revisionContract: {
      attempts: 1,
      fetchImpl: async () => ({
        status: 200,
        text: async () => JSON.stringify({
          schema: DEPLOYMENT_AUDIT_SCHEMA,
          source_revision: SOURCE_REVISION,
        }),
      }),
    },
    browserType: {
      launch: async () => {
        calls.push('browser-launch');
        return browser;
      },
    },
    analyticsQualifier: async context => calls.push(['analytics', context.options.viewport]),
    promotionQualifier: async (context, target) => calls.push([
      'promotion',
      context.options.viewport,
      target.pathname,
    ]),
    receiverBoundaryQualifier: async () => calls.push('receiver-boundary'),
  });

  assert.deepEqual(calls.slice(0, 2), ['receiver-boundary', 'browser-launch']);
  assert.deepEqual(
    calls.filter(call => Array.isArray(call) && call[0] === 'promotion'),
    [
      ['promotion', {width: 1440, height: 900}, '/'],
      ['promotion', {width: 1440, height: 900}, '/api/classes/DurableWorkflow-Client.html'],
      ['promotion', {width: 768, height: 1024}, '/'],
      ['promotion', {width: 768, height: 1024}, '/api/classes/DurableWorkflow-Client.html'],
      ['promotion', {width: 390, height: 844}, '/'],
      ['promotion', {width: 390, height: 844}, '/api/classes/DurableWorkflow-Client.html'],
      ['promotion', {width: 640, height: 360}, '/'],
      ['promotion', {width: 640, height: 360}, '/api/classes/DurableWorkflow-Client.html'],
    ],
  );
});

test('the deployment entrypoint requires one exact source revision', () => {
  assert.equal(sourceRevisionArgument(['--source-revision', SOURCE_REVISION]), SOURCE_REVISION);
  assert.throws(() => sourceRevisionArgument([]), /Usage:/);
  assert.throws(
    () => sourceRevisionArgument(['--source-revision', 'main']),
    /40-character lowercase commit SHA/,
  );
});
