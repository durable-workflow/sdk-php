import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { foreignCssAssets, foreignHtmlAssets, siteProblems, vendorManifestProblems } from './check-docs-third-party.mjs';

const page = 'https://php.durable-workflow.com/api/classes/Example.html';
const repository = path.resolve(import.meta.dirname, '..');

function site(files) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'third-party-'));
  for (const [name, content] of Object.entries(files)) {
    fs.mkdirSync(path.dirname(path.join(root, name)), {recursive: true});
    fs.writeFileSync(path.join(root, name), content);
  }
  return root;
}

function manifest(files) {
  return JSON.stringify({
    schema: 'durable-workflow.sdk-php.vendored-browser-assets/v1',
    packages: [{
      name: 'lib', version: '1.0.0', tarball: 'https://registry.npmjs.org/lib/-/lib-1.0.0.tgz', tarball_integrity: 'sha512-x',
      files: Object.entries(files).map(([name, content]) => ({
        path: `lib-1.0.0/${name}`, sha256: crypto.createHash('sha256').update(content).digest('hex'), bytes: Buffer.byteLength(content),
      })),
    }],
  });
}

test('same-origin, relative and data references are allowed', () => {
  const html = '<base href="/api/"><script src="/assets/vendor/lib-1.0.0/lib.js"></script><script src="js/search.js"></script>'
    + '<link rel="stylesheet" href="css/template.css"><link rel="stylesheet" href="https://php.durable-workflow.com/assets/x.css">'
    + '<link rel="icon" href="https://example.com/icon.png"><a href="https://github.com/durable-workflow/sdk-php">source</a>'
    + '<script type="module">import "./x.js";</script><style>.a{background:url(data:image/png;base64,AA==)}</style>';
  assert.deepEqual(foreignHtmlAssets(html, page), []);
});

test('third-party scripts, stylesheets, preloads and inline imports are reported', () => {
  const html = '<script src="https://cdn.jsdelivr.net/npm/fuse.js@3.4.6"></script>'
    + "<script defer src='//unpkg.com/x.js'></script>"
    + '<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/prismjs@1.23.0/themes/prism-okaidia.css">'
    + '<link rel="preload" as="font" href="https://fonts.gstatic.com/a.woff2">'
    + '<style>@import url("https://fonts.googleapis.com/css2?family=Inter");</style>';
  assert.deepEqual(foreignHtmlAssets(html, page), [
    'https://cdn.jsdelivr.net/npm/fuse.js@3.4.6',
    'https://unpkg.com/x.js',
    'https://cdn.jsdelivr.net/npm/prismjs@1.23.0/themes/prism-okaidia.css',
    'https://fonts.gstatic.com/a.woff2',
    'https://fonts.googleapis.com/css2?family=Inter',
  ]);
});

test('stylesheets may not import or reference third-party hosts', () => {
  const css = '@import "base.css"; .a{src:url(/assets/fonts/a.woff2)} .b{background:url("https://cdn.example/b.png")}'
    + "@import url('https://fonts.googleapis.com/css');";
  assert.deepEqual(foreignCssAssets(css, 'https://php.durable-workflow.com/assets/portal.css'),
    ['https://fonts.googleapis.com/css', 'https://cdn.example/b.png']);
});

test('vendored files must match the manifest exactly', () => {
  const ok = site({'manifest.json': manifest({'lib.js': 'ok'}), 'lib-1.0.0/lib.js': 'ok'});
  assert.deepEqual(vendorManifestProblems(ok), []);
  const changed = site({'manifest.json': manifest({'lib.js': 'ok'}), 'lib-1.0.0/lib.js': 'changed'});
  assert.match(vendorManifestProblems(changed).join('\n'), /bytes differ/);
  const extra = site({'manifest.json': manifest({'lib.js': 'ok'}), 'lib-1.0.0/lib.js': 'ok', 'other.js': 'x'});
  assert.match(vendorManifestProblems(extra).join('\n'), /other\.js: vendored but not in the manifest/);
  const missing = site({'manifest.json': manifest({'lib.js': 'ok'})});
  assert.match(vendorManifestProblems(missing).join('\n'), /listed in the manifest but missing/);
  assert.deepEqual(vendorManifestProblems(site({})), ['the vendored asset manifest is missing']);
});

test('a whole site fails on one third-party reference', () => {
  const vendor = {'assets/vendor/manifest.json': manifest({'lib.js': 'ok'}), 'assets/vendor/lib-1.0.0/lib.js': 'ok'};
  assert.deepEqual(siteProblems(site({...vendor, 'index.html': '<script src="/assets/vendor/lib-1.0.0/lib.js"></script>'})), []);
  assert.deepEqual(siteProblems(site({...vendor, 'api/index.html': '<script src="https://cdn.jsdelivr.net/npm/x"></script>'})),
    ['api/index.html loads a third-party script or style: https://cdn.jsdelivr.net/npm/x']);
});

test('the committed template vendors exactly the files its manifest records', () => {
  assert.deepEqual(vendorManifestProblems(path.join(repository, '.phpdoc/template/assets/vendor')), []);
  const layout = fs.readFileSync(path.join(repository, '.phpdoc/template/layout.html.twig'), 'utf8');
  assert.deepEqual(foreignHtmlAssets(layout, page), []);
  for (const [, source] of layout.matchAll(/(?:src|href)="(\/assets\/vendor\/[^"]+)"/g)) {
    assert.ok(fs.existsSync(path.join(repository, '.phpdoc/template', source.replace(/^\//, ''))), source);
  }
});
