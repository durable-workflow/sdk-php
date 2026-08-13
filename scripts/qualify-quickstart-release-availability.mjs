import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {appendFile, readFile} from 'node:fs/promises';
import process from 'node:process';
import {pathToFileURL} from 'node:url';

const PACKAGE_NAME = 'durable-workflow/sdk';
const RELEASE_PATTERN = /^[0-9]+\.[0-9]+\.[0-9]+(?:-(?:alpha|beta|rc)\.[0-9]+)?$/;
const DIAGNOSTIC_LIMIT = 1000;

function inspectPublishedPackage(packageName, version) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.env.COMPOSER_BINARY || 'composer', [
      'show',
      packageName,
      version,
      '--all',
      '--format=json',
    ], {stdio: ['ignore', 'pipe', 'pipe']});
    let stdout = '';
    let stderr = '';
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on('data', chunk => { stdout += chunk; });
    child.stderr.on('data', chunk => { stderr += chunk; });
    child.once('error', reject);
    child.once('exit', (code, signal) => {
      if (code !== 0) {
        const detail = stderr.trim().replace(/\s+/g, ' ').slice(0, DIAGNOSTIC_LIMIT);
        console.warn(
          `Composer cannot resolve ${packageName}:${version} yet (${signal ? `signal ${signal}` : `status ${code}`})${detail ? `: ${detail}` : ''}`,
        );
        resolve(null);
        return;
      }

      try {
        resolve(JSON.parse(stdout));
      } catch (error) {
        reject(new Error(`Composer returned invalid JSON for ${packageName}:${version}: ${error.message}`));
      }
    });
  });
}

export async function resolvePublishedRelease(manifest, {inspector = inspectPublishedPackage} = {}) {
  const release = manifest?.extra?.['durable-workflow']?.['product-train'];
  assert(
    typeof release === 'string' && RELEASE_PATTERN.test(release),
    'Composer metadata must declare one exact PHP SDK release identity',
  );

  const metadata = await inspector(PACKAGE_NAME, release);
  if (metadata === null) return {release, published: false};

  assert.equal(metadata?.name, PACKAGE_NAME, 'Composer resolved the wrong package identity');
  assert.deepEqual(
    metadata?.versions,
    [release],
    'Composer did not resolve only the source-declared release identity',
  );
  return {release, published: true};
}

async function main() {
  const manifest = JSON.parse(
    await readFile(new URL('../composer.json', import.meta.url), 'utf8'),
  );
  const state = await resolvePublishedRelease(manifest);
  if (process.env.GITHUB_OUTPUT) {
    await appendFile(
      process.env.GITHUB_OUTPUT,
      `release_published=${state.published}\n`,
    );
  }
  console.log(
    state.published
      ? `${PACKAGE_NAME}:${state.release} is available for portal deployment.`
      : `${PACKAGE_NAME}:${state.release} is not published; preserving the current portal deployment.`,
  );
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await main();
}
