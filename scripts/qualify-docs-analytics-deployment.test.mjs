import assert from 'node:assert/strict';
import {once} from 'node:events';
import {readFile} from 'node:fs/promises';
import http from 'node:http';
import {after, before, test} from 'node:test';
import process from 'node:process';
import {chromium} from 'playwright';
import {
  BEACON_URL,
  qualifyAnalyticsTransport,
  qualifyPromotionTransport,
  PROMOTION_SOURCE,
  RUM_URL,
} from './qualify-docs-analytics-deployment.mjs';

const VALID_TOKEN = '00000000000000000000000000000000';
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

function promotionPage(eventUrl, destination) {
  return `<!doctype html>
    <html>
      <body>
        <main class="phpdocumentor-content">API reference</main>
        <aside data-promotion-source="${PROMOTION_SOURCE}">
          <a data-promotion-action="early-access" href="${destination}">Request early access</a>
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

before(async () => {
  promotionServer = http.createServer(async (request, response) => {
    if (request.method === 'GET' && request.url === '/early-access') {
      response.writeHead(200, {'content-type': 'text/html'});
      response.end('<!doctype html><title>Early access</title><main>Early access form</main>');
      return;
    }
    if (request.method !== 'POST') {
      response.writeHead(404);
      response.end();
      return;
    }

    let body = '';
    for await (const chunk of request) body += chunk;
    promotionEvents.push({
      body: JSON.parse(body),
      headers: request.headers,
      method: request.method,
      path: request.url,
    });
    response.writeHead(request.url === '/failed-promotion-events' ? 403 : 204, {
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
    if (request.url === '/promotion' || request.url === '/failed-promotion') {
      const failed = request.url === '/failed-promotion';
      response.writeHead(200, {'content-type': 'text/html'});
      response.end(promotionPage(
        `${promotionOrigin}/${failed ? 'failed-promotion-events' : 'promotion-events'}`,
        `${promotionOrigin}/early-access#source=${PROMOTION_SOURCE}`,
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

test('promotion qualification exercises browser headers, receiver status, and destination', async () => {
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
    assert.deepEqual(
      promotionEvents.map(event => ({body: event.body, method: event.method, path: event.path})),
      [
        {
          body: {source: PROMOTION_SOURCE, event: 'impression'},
          method: 'POST',
          path: '/promotion-events',
        },
        {
          body: {source: PROMOTION_SOURCE, event: 'click'},
          method: 'POST',
          path: '/promotion-events',
        },
      ],
    );
  } finally {
    await context.close();
  }
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
      /receiver rejected the promotion impression/,
    );
  } finally {
    await context.close();
  }
});

test('the deployment workflow keeps credential validation but has no aggregation dependency', async () => {
  const workflow = await readFile(new URL('../.github/workflows/docs.yml', import.meta.url), 'utf8');
  const qualification = await readFile(new URL('./qualify-docs-analytics-deployment.mjs', import.meta.url), 'utf8');

  assert.match(workflow, /CLOUDFLARE_WEB_ANALYTICS_TOKEN: \$\{\{ vars\.CLOUDFLARE_WEB_ANALYTICS_TOKEN \}\}/);
  assert.match(workflow, /npm run qualify:docs-analytics-deployment/);
  assert.doesNotMatch(workflow, /CLOUDFLARE_ACCOUNT_ID|CLOUDFLARE_ANALYTICS_API_TOKEN/);
  assert.doesNotMatch(qualification, /api\.cloudflare\.com|rumPageloadEventsAdaptiveGroups|observedPageViews/);
});
