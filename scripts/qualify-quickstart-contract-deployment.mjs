import {spawn} from 'node:child_process';
import {mkdtemp, readFile, rm, stat, writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';
import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';

const DEFAULT_CONTRACT_URL = 'https://php.durable-workflow.com/quickstart-contract.json';
const GITHUB_API_ORIGIN = 'https://api.github.com';
const MAX_GITHUB_ERROR_BODY_BYTES = 4096;
const MAX_GITHUB_ERROR_MESSAGE_CHARACTERS = 512;
const MAX_REDIRECTS = 5;
const SOURCE_NAMES = ['bootstrap', 'client', 'worker'];

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function publicUrl(value, context) {
  const url = new URL(value);
  const loopback = url.hostname === '127.0.0.1' || url.hostname === 'localhost';
  assert(url.protocol === 'https:' || (url.protocol === 'http:' && loopback), `${context} must use HTTPS`);
  assert(url.username === '' && url.password === '', `${context} must not contain credentials`);
  return url;
}

function jsonPointer(document, pointer, context) {
  assert(
    typeof pointer === 'string' && pointer.startsWith('/'),
    `${context} must be an RFC 6901 JSON Pointer`,
  );

  return pointer.slice(1).split('/').reduce((value, rawSegment) => {
    const segment = rawSegment.replace(/~1/g, '/').replace(/~0/g, '~');
    assert(value !== null && typeof value === 'object' && segment in value, `${context} does not resolve`);
    return value[segment];
  }, document);
}

function boundedRateLimitHeader(response, name, maximumDigits = 13) {
  const value = response.headers.get(name);
  return value && new RegExp(`^\\d{1,${maximumDigits}}$`).test(value) ? value : null;
}

async function boundedGitHubErrorMessage(response) {
  if (!response.body) return null;

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let body = '';
  let bytes = 0;

  try {
    while (true) {
      const {done, value} = await reader.read();
      if (done) break;
      bytes += value.byteLength;
      if (bytes > MAX_GITHUB_ERROR_BODY_BYTES) {
        await reader.cancel();
        return null;
      }
      body += decoder.decode(value, {stream: true});
    }
    body += decoder.decode();
  } finally {
    reader.releaseLock();
  }

  try {
    const document = JSON.parse(body);
    if (
      document === null
      || typeof document !== 'object'
      || Array.isArray(document)
      || typeof document.message !== 'string'
      || document.message.length > MAX_GITHUB_ERROR_MESSAGE_CHARACTERS
    ) {
      return null;
    }
    return document.message;
  } catch {
    return null;
  }
}

function rateLimitDiagnostics(status, remaining, retryAfter, reset) {
  const diagnostics = [`HTTP ${status}`, `remaining=${remaining ?? 'unknown'}`];
  if (retryAfter !== null) diagnostics.push(`retry-after=${retryAfter}`);
  if (reset !== null) diagnostics.push(`reset=${reset}`);
  return diagnostics.join('; ');
}

async function workflowEvidenceHttpError(response, url, githubToken) {
  const status = response.status;
  const remaining = boundedRateLimitHeader(response, 'x-ratelimit-remaining');
  const reset = boundedRateLimitHeader(response, 'x-ratelimit-reset');
  const retryAfter = boundedRateLimitHeader(response, 'retry-after', 6);

  if (status === 401) {
    return new Error(
      'qualification evidence API authentication failed (HTTP 401); '
      + 'check that GITHUB_TOKEN is valid and has actions: read permission',
    );
  }
  if ((status === 403 || status === 429) && remaining === '0') {
    return new Error(
      `qualification evidence API GitHub primary rate limit exhausted (${rateLimitDiagnostics(status, remaining, retryAfter, reset)}); `
      + `${githubToken ? 'check the token rate limit' : 'set GITHUB_TOKEN for local qualification'} and retry`,
    );
  }
  if (status === 403 || status === 429) {
    const message = await boundedGitHubErrorMessage(response);
    const hasSecondaryMessage = message !== null && /\bsecondary rate limit(?:s|ing)?\b/i.test(message);
    if (retryAfter !== null || hasSecondaryMessage) {
      const retryGuidance = retryAfter === null
        ? 'wait before retrying and reduce request concurrency'
        : `retry after ${retryAfter} seconds and reduce request concurrency`;
      return new Error(
        `qualification evidence API GitHub secondary rate limit exceeded (${rateLimitDiagnostics(status, remaining, retryAfter, reset)}); ${retryGuidance}`,
      );
    }
  }
  if (status === 429) {
    return new Error(
      `qualification evidence API GitHub rate limit response (${rateLimitDiagnostics(status, remaining, retryAfter, reset)}); wait before retrying`,
    );
  }
  if (status === 403) {
    return new Error(
      'qualification evidence API access was forbidden (HTTP 403); '
      + `${githubToken ? 'check that GITHUB_TOKEN has actions: read permission' : 'set GITHUB_TOKEN for local qualification'}`,
    );
  }
  if (status === 404) {
    return new Error(
      'qualification evidence workflow is missing or inaccessible (HTTP 404); '
      + 'verify the public workflow API URL and actions: read access',
    );
  }

  return new Error(`qualification evidence API returned HTTP ${status} from ${url.origin}`);
}

async function fetchResponse(url, context, {
  authenticateGitHubWorkflow = false,
  fetchImpl = fetch,
  githubToken,
  workflowEvidence = false,
} = {}) {
  let requestUrl = publicUrl(url, context);
  const signal = AbortSignal.timeout(30_000);

  for (let redirects = 0; redirects <= MAX_REDIRECTS; redirects += 1) {
    const headers = {
      accept: 'application/json, text/html;q=0.9, */*;q=0.1',
      'user-agent': 'durable-workflow-quickstart-contract-qualifier',
    };
    if (
      redirects === 0
      && authenticateGitHubWorkflow
      && githubToken
      && requestUrl.origin === GITHUB_API_ORIGIN
    ) {
      headers.authorization = `Bearer ${githubToken}`;
      headers['x-github-api-version'] = '2022-11-28';
    }

    const response = await fetchImpl(requestUrl, {
      headers,
      redirect: 'manual',
      signal,
    });
    if ([301, 302, 303, 307, 308].includes(response.status)) {
      assert(redirects < MAX_REDIRECTS, `${context} exceeded ${MAX_REDIRECTS} redirects`);
      const location = response.headers.get('location');
      assert(location, `${context} returned a redirect without a location`);
      await response.body?.cancel();
      requestUrl = publicUrl(new URL(location, requestUrl), `${context} redirect`);
      continue;
    }
    if (!response.ok) {
      if (workflowEvidence && requestUrl.origin === GITHUB_API_ORIGIN) {
        throw await workflowEvidenceHttpError(response, requestUrl, githubToken);
      }
      throw new Error(`${context} returned HTTP ${response.status} from ${requestUrl.origin}`);
    }
    return response;
  }

  throw new Error(`${context} exceeded ${MAX_REDIRECTS} redirects`);
}

async function fetchJson(url, context, options) {
  const response = await fetchResponse(url, context, options);
  try {
    return await response.json();
  } catch (error) {
    throw new Error(`${context} did not return JSON: ${error.message}`);
  }
}

function validateContract(contract, schema) {
  assert(contract?.schema_version === 2, 'deployed quickstart contract must use schema version 2');
  assert(contract.$schema === schema?.$id, 'deployed quickstart contract and schema identities must match');
  assert(schema?.properties?.schema_version?.const === 2, 'deployed schema must describe contract version 2');
  assert(
    schema?.$defs?.reference_resolution && schema?.$defs?.composer_package_path,
    'deployed schema must define package reference resolution semantics',
  );
  assert(
    contract.reference_resolution?.version === 1,
    'deployed contract must use reference resolution semantics version 1',
  );
  assert(contract.package?.name === 'durable-workflow/sdk', 'deployed contract names the wrong Composer package');
  assert(
    /^[0-9]+\.[0-9]+\.[0-9]+(?:-(?:alpha|beta|rc)\.[0-9]+)?$/.test(
      contract.package?.published_version || '',
    ),
    'deployed contract must select an exact published package version',
  );
  assert(
    contract.package?.composer_requirement === `${contract.package.published_version}@RC`,
    'deployed contract must derive its Composer requirement from the published version',
  );
  assert(!('published_smoke' in contract), 'deployed contract exposes a repository-local smoke path');

  const sourceNames = Object.keys(contract.sources || {}).sort();
  assert(
    JSON.stringify(sourceNames) === JSON.stringify(SOURCE_NAMES),
    'deployed contract must expose exactly the bootstrap, client, and worker sources',
  );

  const sourcePaths = new Map();
  for (const name of SOURCE_NAMES) {
    const reference = contract.sources[name];
    assert(reference?.kind === 'composer_package_path', `source ${name} must be a Composer package path`);
    const base = contract.reference_resolution?.bases?.[reference.base];
    assert(base?.kind === 'composer_package', `source ${name} must select a Composer package base`);
    assert(
      jsonPointer(contract, base.package_pointer, `source ${name} package pointer`) === contract.package,
      `source ${name} must resolve against the declared package coordinate`,
    );
    assert(
      typeof reference.path === 'string'
        && reference.path.length > 0
        && !reference.path.startsWith('/')
        && !reference.path.includes('\\')
        && !reference.path.split('/').some((segment) => segment === '' || segment === '..'),
      `source ${name} must use a safe package-relative path`,
    );
    sourcePaths.set(name, reference.path);
  }

  const provenance = contract.qualification_provenance;
  assert(
    typeof provenance?.subject_base === 'string'
      && provenance.subject_base in contract.reference_resolution.bases,
    'qualification provenance must select a declared package base',
  );
  const evidence = provenance?.evidence;
  assert(
    evidence?.kind === 'github_actions_workflow',
    'qualification evidence must identify a public GitHub Actions workflow',
  );
  publicUrl(evidence.api_url, 'qualification evidence API URL');
  publicUrl(evidence.web_url, 'qualification evidence web URL');
  assert(
    jsonPointer(contract, evidence.version_input?.value_pointer, 'qualification version input pointer')
      === contract.package.published_version,
    'qualification evidence must bind its input to the published package version',
  );
  assert(
    typeof evidence.version_input?.name === 'string' && evidence.version_input.name.length > 0,
    'qualification evidence must name its exact-version input',
  );

  const schemaValidator = new Ajv2020({allErrors: true});
  addFormats(schemaValidator);
  const validateQuickstart = schemaValidator.compile(schema);
  assert(
    validateQuickstart(contract),
    `deployed quickstart contract does not satisfy its schema: ${schemaValidator.errorsText(validateQuickstart.errors)}`,
  );

  return {evidence, sourcePaths};
}

function run(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {stdio: 'inherit', ...options});
    child.once('error', reject);
    child.once('exit', (code, signal) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`${command} exited with ${signal ? `signal ${signal}` : `status ${code}`}`));
    });
  });
}

async function installPublishedPackage(contract) {
  const consumer = await mkdtemp(join(tmpdir(), 'durable-workflow-quickstart-contract-'));
  const composerEnvironment = {...process.env};
  delete composerEnvironment.GITHUB_TOKEN;
  await writeFile(
    join(consumer, 'composer.json'),
    `${JSON.stringify({name: 'durable-workflow/quickstart-contract-qualifier'}, null, 2)}\n`,
  );
  await run(process.env.COMPOSER_BINARY || 'composer', [
    'require',
    `${contract.package.name}:${contract.package.published_version}`,
    '--working-dir',
    consumer,
    '--no-interaction',
    '--prefer-dist',
    '--no-plugins',
    '--no-scripts',
    '--no-audit',
  ], {env: composerEnvironment});

  return {
    root: join(consumer, 'vendor', ...contract.package.name.split('/')),
    cleanup: async () => rm(consumer, {recursive: true, force: true}),
  };
}

async function verifyInstalledPackage(contract, sourcePaths, packageRoot) {
  const packageManifest = JSON.parse(await readFile(join(packageRoot, 'composer.json'), 'utf8'));
  assert(packageManifest.name === contract.package.name, 'installed package name does not match the contract');
  assert(
    packageManifest.extra?.['durable-workflow']?.['product-train'] === contract.package.published_version,
    'installed package release identity does not match the contract',
  );

  for (const [name, relativePath] of sourcePaths) {
    let source;
    try {
      source = await stat(join(packageRoot, relativePath));
    } catch {
      throw new Error(`published package source ${name} is not consumable`);
    }
    assert(source.isFile() && source.size > 0, `published package source ${name} is not consumable`);
  }
}

async function verifyEvidence(evidence, {fetchImpl, githubToken}) {
  const workflow = await fetchJson(evidence.api_url, 'qualification evidence API', {
    authenticateGitHubWorkflow: true,
    fetchImpl,
    githubToken,
    workflowEvidence: true,
  });
  assert(
    typeof workflow?.state === 'string' && workflow.state.length > 0,
    'qualification evidence API did not report workflow state',
  );
  assert(
    workflow.state === 'active',
    `qualification evidence workflow is inactive (state=${JSON.stringify(workflow.state.slice(0, 80))})`,
  );
  assert(
    Number.isInteger(workflow.id) && workflow.id > 0 && typeof workflow.name === 'string' && workflow.name,
    'qualification evidence API did not resolve a workflow identity',
  );
  await fetchResponse(evidence.web_url, 'qualification evidence web page', {fetchImpl, githubToken});
}

export async function qualifyDeployment({
  contractUrl = DEFAULT_CONTRACT_URL,
  fetchImpl = fetch,
  githubToken = process.env.GITHUB_TOKEN,
  packageRoot,
} = {}) {
  const contract = await fetchJson(contractUrl, 'deployed quickstart contract', {fetchImpl, githubToken});
  const schema = await fetchJson(contract?.$schema, 'deployed quickstart contract schema', {
    fetchImpl,
    githubToken,
  });
  const {evidence, sourcePaths} = validateContract(contract, schema);

  const installation = packageRoot
    ? {root: packageRoot, cleanup: async () => {}}
    : await installPublishedPackage(contract);
  try {
    await verifyInstalledPackage(contract, sourcePaths, installation.root);
    await verifyEvidence(evidence, {fetchImpl, githubToken});
  } finally {
    await installation.cleanup();
  }

  return {
    package: `${contract.package.name}:${contract.package.published_version}`,
    sources: Object.fromEntries(sourcePaths),
    evidence: evidence.web_url,
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const result = await qualifyDeployment({contractUrl: process.env.QUICKSTART_CONTRACT_URL});
  console.log(
    `Qualified ${result.package}: ${Object.values(result.sources).join(', ')}; evidence ${result.evidence}`,
  );
}
