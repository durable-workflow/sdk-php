// The built portal and API reference load scripts and styles only from their
// own origin (sdk-php #483). Third-party browser libraries are vendored under
// /assets/vendor/, and every vendored file must match the SHA-256 recorded in
// /assets/vendor/manifest.json.
//
//   node scripts/check-docs-third-party.mjs build/site
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

export const SITE_ORIGIN = 'https://php.durable-workflow.com';
const MANIFEST_SCHEMA = 'durable-workflow.sdk-php.vendored-browser-assets/v1';

function filesUnder(directory) {
  return fs.readdirSync(directory, {withFileTypes: true}).flatMap((entry) => {
    const target = path.join(directory, entry.name);
    if (entry.isSymbolicLink()) throw new Error(`Documentation output must not contain symbolic links: ${target}`);
    if (entry.isDirectory()) return filesUnder(target);
    return entry.isFile() ? [target] : [];
  });
}

function attribute(tag, name) {
  const match = tag.match(new RegExp(`\\s${name}\\s*=\\s*(?:"([^"]*)"|'([^']*)'|([^\\s>]+))`, 'i'));
  return match ? (match[1] ?? match[2] ?? match[3]) : null;
}

function foreign(reference, pageUrl) {
  let url;
  try {
    url = new URL(reference.trim(), pageUrl);
  } catch {
    return reference;
  }
  if (url.protocol === 'data:') return null;
  return url.origin === SITE_ORIGIN ? null : url.href;
}

// Script and style references in one HTML page that leave the site origin.
export function foreignHtmlAssets(html, pageUrl) {
  const found = [];
  for (const [tag] of html.matchAll(/<script\b[^>]*>/gi)) {
    const source = attribute(tag, 'src');
    if (source !== null) found.push(foreign(source, pageUrl));
  }
  for (const [tag] of html.matchAll(/<link\b[^>]*>/gi)) {
    const relations = (attribute(tag, 'rel') ?? '').toLowerCase().split(/\s+/);
    const href = attribute(tag, 'href');
    if (href !== null && relations.some((relation) => ['stylesheet', 'preload', 'modulepreload'].includes(relation))) {
      found.push(foreign(href, pageUrl));
    }
  }
  for (const [, body] of html.matchAll(/<style\b[^>]*>([\s\S]*?)<\/style>/gi)) {
    found.push(...foreignCssAssets(body, pageUrl));
  }
  return found.filter(Boolean);
}

// @import and url() references in a stylesheet that leave the site origin.
export function foreignCssAssets(css, stylesheetUrl) {
  const references = [
    ...[...css.matchAll(/@import\s+(?:url\(\s*)?["']?([^"')\s;]+)/gi)].map((match) => match[1]),
    ...[...css.matchAll(/url\(\s*["']?([^"')]+)["']?\s*\)/gi)].map((match) => match[1]),
  ];
  return [...new Set(references.map((reference) => foreign(reference, stylesheetUrl)).filter(Boolean))];
}

// Every vendored file is listed once with its exact bytes, and nothing else is vendored.
export function vendorManifestProblems(vendorDirectory) {
  const manifestPath = path.join(vendorDirectory, 'manifest.json');
  if (!fs.existsSync(manifestPath)) return ['the vendored asset manifest is missing'];
  const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
  const problems = [];
  if (manifest.schema !== MANIFEST_SCHEMA) problems.push('the vendored asset manifest has an unexpected schema');
  const listed = new Set();
  for (const pkg of manifest.packages ?? []) {
    if (!/^https:\/\/registry\.npmjs\.org\//.test(pkg.tarball ?? '') || !/^sha512-/.test(pkg.tarball_integrity ?? '')) {
      problems.push(`${pkg.name}: the source tarball and its integrity must be recorded`);
    }
    for (const file of pkg.files ?? []) {
      const prefix = `${pkg.name}-${pkg.version}/`;
      if (!file.path?.startsWith(prefix) || file.path.split('/').includes('..')) {
        problems.push(`${file.path}: vendored files must live under ${prefix}`);
        continue;
      }
      listed.add(file.path);
      const target = path.join(vendorDirectory, file.path);
      if (!fs.existsSync(target)) {
        problems.push(`${file.path}: listed in the manifest but missing`);
        continue;
      }
      const bytes = fs.readFileSync(target);
      const digest = crypto.createHash('sha256').update(bytes).digest('hex');
      if (digest !== file.sha256 || bytes.length !== file.bytes) problems.push(`${file.path}: bytes differ from the manifest`);
    }
  }
  for (const file of filesUnder(vendorDirectory)) {
    const relative = path.relative(vendorDirectory, file).split(path.sep).join('/');
    if (relative !== 'manifest.json' && !listed.has(relative)) problems.push(`${relative}: vendored but not in the manifest`);
  }
  return problems;
}

export function siteProblems(siteDirectory) {
  const problems = [];
  let pages = 0;
  for (const file of filesUnder(siteDirectory)) {
    const relative = path.relative(siteDirectory, file).split(path.sep).join('/');
    const url = `${SITE_ORIGIN}/${relative}`;
    let references = [];
    if (file.endsWith('.html')) {
      pages += 1;
      references = foreignHtmlAssets(fs.readFileSync(file, 'utf8'), url);
    } else if (file.endsWith('.css')) {
      references = foreignCssAssets(fs.readFileSync(file, 'utf8'), url);
    }
    for (const reference of new Set(references)) problems.push(`${relative} loads a third-party script or style: ${reference}`);
  }
  if (pages === 0) problems.push('the documentation output has no HTML pages');
  problems.push(...vendorManifestProblems(path.join(siteDirectory, 'assets/vendor')));
  return problems;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  if (process.argv[2] !== 'build/site') throw new Error('The third-party asset check accepts only the build/site output.');
  const problems = siteProblems(path.resolve('build/site'));
  for (const problem of problems.slice(0, 50)) console.error(problem);
  assert.equal(problems.length, 0, `${problems.length} third-party asset problem(s) in build/site.`);
  console.log('build/site loads scripts and styles only from its own origin; vendored files match their manifest.');
}
