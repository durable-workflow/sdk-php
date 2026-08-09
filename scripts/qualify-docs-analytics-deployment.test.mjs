import assert from 'node:assert/strict';
import {once} from 'node:events';
import {readFile} from 'node:fs/promises';
import http from 'node:http';
import {after, before, test} from 'node:test';
import {chromium} from 'playwright';
import {
  BEACON_URL,
  qualifyAnalyticsTransport,
  RUM_URL,
} from './qualify-docs-analytics-deployment.mjs';

const VALID_TOKEN = '00000000000000000000000000000000';
let browser;
let origin;
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

before(async () => {
  server = http.createServer((request, response) => {
    response.writeHead(request.url === '/failed-page' ? 500 : 200, {'content-type': 'text/html'});
    response.end(analyticsPage(request.url === '/malformed-loader' ? 'missing' : VALID_TOKEN));
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  assert(address && typeof address === 'object');
  origin = `http://127.0.0.1:${address.port}`;
  browser = await chromium.launch({headless: true});
});

after(async () => {
  await browser?.close();
  if (server?.listening) {
    server.close();
    await once(server, 'close');
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

test('the deployment workflow keeps credential validation but has no aggregation dependency', async () => {
  const workflow = await readFile(new URL('../.github/workflows/docs.yml', import.meta.url), 'utf8');
  const qualification = await readFile(new URL('./qualify-docs-analytics-deployment.mjs', import.meta.url), 'utf8');

  assert.match(workflow, /CLOUDFLARE_WEB_ANALYTICS_TOKEN: \$\{\{ vars\.CLOUDFLARE_WEB_ANALYTICS_TOKEN \}\}/);
  assert.match(workflow, /npm run qualify:docs-analytics-deployment/);
  assert.doesNotMatch(workflow, /CLOUDFLARE_ACCOUNT_ID|CLOUDFLARE_ANALYTICS_API_TOKEN/);
  assert.doesNotMatch(qualification, /api\.cloudflare\.com|rumPageloadEventsAdaptiveGroups|observedPageViews/);
});
