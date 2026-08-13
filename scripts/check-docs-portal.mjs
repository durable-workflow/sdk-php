import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';

const siteDirectory = path.resolve(process.argv[2] ?? 'build/site');
const projectDirectory = path.resolve(import.meta.dirname, '..');
const composer = JSON.parse(fs.readFileSync(path.join(projectDirectory, 'composer.json'), 'utf8'));
const navigation = JSON.parse(fs.readFileSync(path.join(projectDirectory, 'docs/portal/_data/navigation.json'), 'utf8'));
const quickstartContract = JSON.parse(fs.readFileSync(path.join(projectDirectory, 'docs/quickstart-contract.json'), 'utf8'));
const quickstartSchema = JSON.parse(fs.readFileSync(path.join(projectDirectory, 'docs/quickstart-contract.schema.v2.json'), 'utf8'));
const release = composer.extra['durable-workflow'];
const sdkVersion = release['product-train'];
const composerCommand = `composer require ${quickstartContract.package.name}:${quickstartContract.package.onboarding_requirement}`;
const sdkSeries = sdkVersion.split('.').slice(0, 2).join('.');

function filesUnder(directory, extension) {
  return fs.readdirSync(directory, {withFileTypes: true}).flatMap((entry) => {
    const target = path.join(directory, entry.name);
    if (entry.isDirectory()) return filesUnder(target, extension);
    return entry.isFile() && target.endsWith(extension) ? [target] : [];
  });
}

function outputPath(url) {
  const pathname = new URL(url, 'https://php.durable-workflow.com').pathname;
  if (pathname.endsWith('/')) return path.join(siteDirectory, pathname, 'index.html');
  return path.join(siteDirectory, pathname);
}

const navigationItems = navigation.flatMap(({items}) => items);
for (const item of navigationItems) {
  assert(fs.existsSync(outputPath(item.url)), `Navigation target does not exist: ${item.url}`);
}
assert(fs.existsSync(path.join(siteDirectory, 'api/index.html')), 'Generated API Reference must live under /api/.');
assert(
  filesUnder(path.join(siteDirectory, 'api'), '.html').length > 2,
  'Generated API Reference must contain useful nested symbol pages.',
);

const htmlFiles = filesUnder(siteDirectory, '.html');
for (const file of htmlFiles) {
  const html = fs.readFileSync(file, 'utf8');
  const relative = path.relative(siteDirectory, file).replaceAll(path.sep, '/');
  if (!relative.startsWith('api/')) {
    assert(
      !/\b\d+\.\d+\.\d+-(?:alpha|beta|rc)\.\d+\b/.test(html),
      `${relative} exposes an exact prerelease instead of channel-level onboarding guidance.`,
    );
  }
  for (const required of [
    '<meta name="description"',
    '<link rel="canonical"',
    '<link rel="icon" href="/assets/favicon.svg"',
    '<meta property="og:title"',
    '<meta property="og:description"',
    '<meta property="og:image"',
    '<meta name="twitter:card"',
  ]) {
    assert(html.includes(required), `${relative} is missing metadata: ${required}`);
  }
  const canonical = html.match(/<link rel="canonical" href="([^"]+)"/)?.[1];
  assert(canonical?.startsWith('https://php.durable-workflow.com/'), `${relative} has an invalid canonical URL.`);
  assert(!canonical.includes('/index.html'), `${relative} exposes index.html in its canonical URL.`);

  if (relative.startsWith('api/')) {
    assert.equal(
      html.match(/<base href="\/api\/">/g)?.length,
      1,
      `${relative} must resolve generated references from the deployed /api/ mount.`,
    );

    for (const match of html.matchAll(/(?:href|src)="([^"]*)"/g)) {
      const reference = match[1];
      assert(
        reference === '' || /^(?:[a-z][a-z0-9+.-]*:|\/|#|\?)/i.test(reference),
        `${relative} contains an unrooted local API reference: ${reference}`,
      );
    }
  }

  for (const href of html.matchAll(/href="(\/[^"]*)"/g)) {
    const url = href[1];
    if (url.startsWith('//')) continue;
    const target = outputPath(url.split('#')[0].split('?')[0]);
    assert(fs.existsSync(target), `${relative} links to a missing local target: ${url}`);
  }
}

const graphDirectory = path.join(siteDirectory, 'api/graphs');
const graphPages = fs.existsSync(graphDirectory) ? filesUnder(graphDirectory, '.html') : [];
for (const file of graphPages) {
  const html = fs.readFileSync(file, 'utf8');
  const relative = path.relative(siteDirectory, file).replaceAll(path.sep, '/');
  const graphAssets = [...html.matchAll(/src="(\/api\/graphs\/[^"?#]+\.svg)(?:[?#][^"]*)?"/g)]
    .map((match) => match[1]);
  assert(graphAssets.length > 0, `${relative} does not reference a generated SVG graph.`);
  for (const asset of graphAssets) {
    assert(fs.existsSync(outputPath(asset)), `${relative} references a missing generated graph: ${asset}`);
  }
}

for (const relative of ['api/packages/Application.html', 'api/reports/deprecated.html']) {
  const file = path.join(siteDirectory, relative);
  assert(fs.existsSync(file), `Nested API regression page does not exist: ${relative}`);
  const html = fs.readFileSync(file, 'utf8');
  const localTargets = [...html.matchAll(/(?:href|src)="([^"]*)"/g)].flatMap((match) => {
    const url = new URL(match[1], `https://php.durable-workflow.com/${relative}`);
    return url.origin === 'https://php.durable-workflow.com' ? [url] : [];
  });
  assert(localTargets.length > 0, `${relative} has no local API references to validate.`);
  for (const url of localTargets) {
    assert(
      fs.existsSync(outputPath(url)),
      `${relative} resolves a missing deployed reference: ${url.pathname}${url.hash}`,
    );
  }
}

const home = fs.readFileSync(path.join(siteDirectory, 'index.html'), 'utf8');
const quickstart = fs.readFileSync(path.join(siteDirectory, 'getting-started/first-workflow/index.html'), 'utf8');
const deployment = fs.readFileSync(path.join(siteDirectory, 'operate/deployment/index.html'), 'utf8');
const deployedQuickstartContract = JSON.parse(fs.readFileSync(path.join(siteDirectory, 'quickstart-contract.json'), 'utf8'));
const deployedQuickstartSchema = JSON.parse(fs.readFileSync(path.join(siteDirectory, 'quickstart-contract.schema.v2.json'), 'utf8'));
assert.deepEqual(deployedQuickstartContract, quickstartContract, 'Deployed quickstart contract must match its release-owned source.');
assert.deepEqual(deployedQuickstartSchema, quickstartSchema, 'Deployed quickstart schema must match its release-owned source.');
assert(home.includes(`PHP SDK ${sdkSeries} prerelease`), 'Home release badge must identify the SDK prerelease channel.');
assert.equal(quickstartContract.schema_version, 2, 'Quickstart contract must use consumer-resolvable schema version 2.');
assert.equal(quickstartContract.$schema, quickstartSchema.$id, 'Quickstart contract must identify its deployed schema.');
assert.equal(quickstartContract.package.name, composer.name, 'Quickstart package must match Composer metadata.');
assert.equal(quickstartContract.package.published_version, sdkVersion, 'Quickstart version must match Composer metadata.');
assert.equal(quickstartContract.package.onboarding_requirement, `^${sdkSeries}@RC`);
assert.equal(
  quickstartContract.runtime_targets.server.image,
  `durableworkflow/server:${release['supported-server-versions']}`,
  'Quickstart Server image must match Composer compatibility metadata.',
);
assert.equal(quickstartContract.reference_resolution.version, 1, 'Quickstart references must use declared resolution semantics.');
const provenance = quickstartContract.qualification_provenance;
const qualificationBase = quickstartContract.reference_resolution.bases[provenance.subject_base];
assert.deepEqual(
  qualificationBase,
  {kind: 'composer_package', package_pointer: '/package'},
  'Quickstart qualification must resolve the immutable Composer package.',
);
assert.equal(provenance.evidence.kind, 'github_actions_workflow');
assert.match(provenance.evidence.api_url, /^https:\/\/api\.github\.com\/repos\/durable-workflow\/sdk-php\/actions\/workflows\//);
assert.match(provenance.evidence.web_url, /^https:\/\/github\.com\/durable-workflow\/sdk-php\/actions\/workflows\//);
assert.deepEqual(
  provenance.evidence.version_input,
  {name: 'sdk_version', value_pointer: '/package/published_version'},
  'Quickstart qualification must bind its workflow input to the published SDK version.',
);
assert(quickstart.includes(composerCommand), 'First-workflow install command must be rendered from the quickstart contract.');
assert(quickstart.includes('/quickstart-contract.json'), 'First workflow must resolve its qualified Server image from the deployed contract.');
for (const [name, html] of [['first workflow', quickstart], ['deployment', deployment]]) {
  assert(
    !/&amp;(?:quot|#39|#92);/.test(html),
    `${name} must not double-escape its machine-derived Server image command.`,
  );
}

for (const reference of Object.values(quickstartContract.sources)) {
  assert.equal(reference.kind, 'composer_package_path', 'Quickstart source must be a Composer package path.');
  assert.equal(reference.base, provenance.subject_base, 'Quickstart source must use the qualified package base.');
  const sourcePath = reference.path;
  const source = path.join(projectDirectory, sourcePath);
  assert(fs.existsSync(source), `Quickstart source does not exist: ${sourcePath}`);
  assert(
    quickstart.includes(`data-quickstart-source="${sourcePath}"`),
    `First workflow does not render package-owned source: ${sourcePath}`,
  );
  execFileSync('php', ['-l', source], {stdio: 'pipe'});
}

const socialCard = fs.readFileSync(path.join(siteDirectory, 'assets/social-card.png'));
assert(socialCard.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10])), 'Social share image must be a valid PNG.');
assert.equal(socialCard.readUInt32BE(16), 1200, 'Social share image must be 1200 pixels wide.');
assert.equal(socialCard.readUInt32BE(20), 630, 'Social share image must be 630 pixels high.');
assert(fs.statSync(path.join(siteDirectory, 'assets/favicon.svg')).size > 100, 'Favicon must be a non-empty image.');

for (const source of filesUnder(path.join(projectDirectory, 'docs/portal'), '.md')) {
  const markdown = fs.readFileSync(source, 'utf8');
  assert(
    !/\b2\.0\.0-(?:alpha|beta|rc)\.\d+\b/.test(markdown),
    `${path.relative(projectDirectory, source)} hard-codes a release candidate instead of using release metadata.`,
  );
}

console.log(`Validated ${htmlFiles.length} pages, ${navigationItems.length} guide destinations, contract-derived commands and examples, metadata, and links.`);
