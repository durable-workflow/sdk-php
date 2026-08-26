import assert from 'node:assert/strict';
import {once} from 'node:events';
import {readFile} from 'node:fs/promises';
import http from 'node:http';
import process from 'node:process';
import test from 'node:test';
import {chromium} from 'playwright';
import {assertNoBrowserFailures, formatHttpFailure} from './check-docs-browser-failures.mjs';
import {assertPrimaryGuideNavigation} from './check-docs-portal-navigation.mjs';

test('portal qualification rejects duplicate primary guide destinations', async () => {
  const fixture = JSON.parse(
    await readFile(
      new URL('./check-docs-portal-fixtures/duplicate-primary-navigation.json', import.meta.url),
      'utf8',
    ),
  );

  assert.throws(() => assertPrimaryGuideNavigation(fixture), {name: 'AssertionError'});
});

test('the layout check isolates only the exact Cloudflare RUM endpoint', async () => {
  const source = await readFile(new URL('./check-docs-browser.mjs', import.meta.url), 'utf8');
  const rumRoutes = [...source.matchAll(/page\.route\((['"])([^'"]*cloudflareinsights[^'"]*)\1/g)]
    .map(match => match[2]);

  assert.deepEqual(rumRoutes, ['https://cloudflareinsights.com/cdn-cgi/rum']);
  assert.match(source, /'access-control-allow-origin': origin/);
  assert.match(source, /'cache-control': 'no-store'/);
  assert.match(source, /if \(response\.status\(\) >= 400\)/);
  assert.match(source, /assert\.deepEqual\(errors, \[\],/);
});

test('a missing font reports its exact HTTP evidence before its console error', async () => {
  const server = http.createServer((request, response) => {
    if (request.url === '/') {
      response.writeHead(200, {'Content-Type': 'text/html'});
      response.end(`<!doctype html>
        <style>
          @font-face { font-family: MissingReferenceFont; src: url('/missing-reference-font.woff2'); }
          body { font-family: MissingReferenceFont, sans-serif; }
        </style>
        <p>The missing reference font must remain a blocking resource error.</p>`);
      return;
    }

    response.writeHead(404, {'Content-Type': 'text/plain'});
    response.end('missing');
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');

  const address = server.address();
  assert.ok(address && typeof address === 'object');
  const origin = `http://127.0.0.1:${address.port}`;
  const launchOptions = {headless: true};
  if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH) {
    launchOptions.executablePath = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH;
  }

  let browser;
  try {
    browser = await chromium.launch(launchOptions);
    const page = await browser.newPage();
    const evidence = {
      consoleErrors: [],
      httpErrors: [],
      pageErrors: [],
      requestFailures: [],
    };
    page.on('console', message => {
      if (message.type() === 'error') evidence.consoleErrors.push(message.text());
    });
    page.on('response', response => {
      if (response.status() >= 400) evidence.httpErrors.push(formatHttpFailure(response));
    });

    await page.goto(origin, {waitUntil: 'networkidle'});
    await page.evaluate(() => document.fonts.ready);

    const expected = `404 GET ${origin}/missing-reference-font.woff2`;
    assert.deepEqual(evidence.httpErrors, [expected]);
    assert.ok(evidence.consoleErrors.length > 0, 'Chromium did not emit the companion resource console error');
    assert.throws(
      () => assertNoBrowserFailures('missing-font fixture', evidence),
      error => {
        assert.match(error.message, /emitted HTTP errors \(status, method, URL\)/);
        assert.ok(error.message.includes(expected), 'HTTP assertion omitted the exact missing resource');
        assert.doesNotMatch(error.message, /emitted console errors/);
        return true;
      },
    );
  } finally {
    await browser?.close();
    server.close();
    await once(server, 'close');
  }
});
