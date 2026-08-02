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

  process.stdout.write('Validated fail-closed analytics consent and cross-scope GA4 cookie withdrawal in Chromium.\n');
} finally {
  await browser?.close();
  server.kill('SIGTERM');
  await Promise.race([
    once(server, 'exit'),
    new Promise((resolve) => setTimeout(resolve, 1000)),
  ]);
}
