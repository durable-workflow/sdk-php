import {readFile} from 'node:fs/promises';
import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';

const repoRoot = new URL('../', import.meta.url);
const contract = JSON.parse(
  await readFile(new URL('scripts/check-docs-examples-contract.json', repoRoot), 'utf8'),
);
const quickstart = JSON.parse(
  await readFile(new URL(contract.quickstartContract, repoRoot), 'utf8'),
);
const quickstartSchema = JSON.parse(
  await readFile(new URL('docs/quickstart-contract.schema.v2.json', repoRoot), 'utf8'),
);

const schemaValidator = new Ajv2020({allErrors: true});
addFormats(schemaValidator);
const validateQuickstart = schemaValidator.compile(quickstartSchema);
if (!validateQuickstart(quickstart)) {
  throw new Error(
    `quickstart contract does not satisfy its published schema: ${schemaValidator.errorsText(validateQuickstart.errors)}`,
  );
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function examplePattern(id) {
  return new RegExp(
    `<!--\\s*docs-example\\s+id=["']${escapeRegExp(id)}["']\\s*-->\\s*` +
      '\\n\\s*```([A-Za-z0-9_-]+)?\\n([\\s\\S]*?)\\n\\s*```',
    'm',
  );
}

function publicMethodPattern(method) {
  return new RegExp(`\\bpublic\\s+function\\s+${escapeRegExp(method)}\\s*\\(`);
}

function methodCallPattern(method) {
  return new RegExp(`->\\s*${escapeRegExp(method)}\\s*\\(`);
}

function patternOccurrences(block, source, context) {
  try {
    return [...block.matchAll(new RegExp(source, 'g'))].length;
  } catch (error) {
    throw new Error(`${context} has invalid workflow identity pattern: ${error.message}`);
  }
}

function checkWorkflowIdentity(block, identity, context) {
  if (identity.intent !== 'runnable_first_start') {
    throw new Error(`${context} has unsupported workflow identity intent ${identity.intent}`);
  }
  if (identity.generator !== 'php_random_bytes_hex' || !identity.variable) {
    throw new Error(`${context} must declare a PHP runtime-random workflow ID variable`);
  }

  const variable = escapeRegExp(identity.variable);
  const assignment = new RegExp(
    `^\\s*\\$${variable}\\s*=\\s*[^;]*bin2hex\\(random_bytes\\((\\d+)\\)\\)[^;]*;\\s*$`,
    'm',
  );
  const assignmentMatch = block.match(assignment);
  if (!assignmentMatch || Number(assignmentMatch[1]) < 16) {
    throw new Error(`${context} must derive $${identity.variable} from at least 16 random bytes`);
  }

  if (!(identity.references || []).length) {
    throw new Error(`${context} must declare generated-ID reference requirements`);
  }
  for (const reference of identity.references) {
    if (!reference.pattern || !Number.isInteger(reference.minimum) || reference.minimum < 1) {
      throw new Error(`${context} must declare valid generated-ID reference requirements`);
    }
    if (patternOccurrences(block, reference.pattern, context) < reference.minimum) {
      throw new Error(`${context} does not reuse its generated workflow ID`);
    }
  }
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
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

function packageSourcePath(reference, context) {
  assert(reference?.kind === 'composer_package_path', `${context} must be a Composer package path`);
  const base = quickstart.reference_resolution?.bases?.[reference.base];
  assert(base?.kind === 'composer_package', `${context} must select a Composer package base`);
  assert(
    jsonPointer(quickstart, base.package_pointer, `${context} package pointer`) === quickstart.package,
    `${context} must resolve against the contract package coordinate`,
  );
  assert(
    typeof reference.path === 'string'
      && reference.path.length > 0
      && !reference.path.startsWith('/')
      && !reference.path.includes('\\')
      && !reference.path.split('/').some((segment) => segment === '' || segment === '..'),
    `${context} must use a safe package-relative path`,
  );
  return reference.path;
}

const composer = JSON.parse(await readFile(new URL('composer.json', repoRoot), 'utf8'));
assert(quickstart.schema_version === 2, 'quickstart contract must use schema version 2');
assert(quickstart.$schema === quickstartSchema.$id, 'quickstart contract must identify its deployed schema');
assert(quickstartSchema.properties?.schema_version?.const === 2, 'quickstart schema must describe version 2');
assert(
  quickstart.reference_resolution?.version === 1,
  'quickstart contract must use reference resolution semantics version 1',
);
assert(quickstart.package?.name === composer.name, 'quickstart package name must match composer.json');
assert(
  quickstart.package?.published_version === composer.extra?.['durable-workflow']?.['product-train'],
  'quickstart published version must match the Composer product train',
);
assert(
  quickstart.package?.composer_requirement === quickstart.package.published_version,
  'quickstart Composer requirement must select the published stable release',
);
assert(
  /^\^\d+\.\d+$/.test(quickstart.package?.onboarding_requirement || ''),
  'quickstart onboarding requirement must select a stable release series',
);
assert(
  quickstart.workflow_authoring?.execution_model === 'fiber'
    && quickstart.workflow_authoring?.syntax === 'straight_line'
    && quickstart.workflow_authoring?.operation_results === 'direct_return'
    && quickstart.workflow_authoring?.generator_compatibility === false,
  'quickstart workflow authoring must declare the Fiber-backed straight-line contract',
);

const server = new URL(quickstart.runtime_targets?.server?.client_input);
const serverRequest = new URL(quickstart.runtime_targets?.server?.example_request);
assert(server.pathname === '/', 'Server client input must be a bare origin');
assert(
  quickstart.runtime_targets?.server?.namespace_input === 'default',
  'Server quickstart must use the default local namespace',
);
assert(serverRequest.pathname === '/api/workflows', 'Server request example must contain one SDK API segment');
assert(
  quickstart.runtime_targets?.server?.image
    === `durableworkflow/server:${composer.extra?.['durable-workflow']?.['supported-server-versions']}`,
  'Server quickstart image must match the Composer compatibility metadata',
);

const cloud = new URL(quickstart.runtime_targets?.cloud?.client_input.replace('<runtime-id>', 'runtime-id'));
const cloudRequest = new URL(
  quickstart.runtime_targets?.cloud?.example_request.replace('<runtime-id>', 'runtime-id'),
);
assert(
  cloud.pathname === '/api/runtime/v1/namespaces/runtime-id',
  'Cloud client input must preserve the complete namespace runtime path',
);
assert(
  quickstart.runtime_targets?.cloud?.namespace_input === '<provisioned-namespace>',
  'Cloud quickstart must require the separately provisioned namespace',
);
assert(
  cloudRequest.pathname === `${cloud.pathname}/api/workflows`,
  'Cloud request example must append the SDK API segment after the namespace runtime path',
);

const sourcePaths = Object.fromEntries(
  Object.entries(quickstart.sources || {}).map(([name, reference]) => [
    name,
    packageSourcePath(reference, `quickstart source ${name}`),
  ]),
);
assert(
  JSON.stringify(Object.keys(sourcePaths).sort()) === JSON.stringify(['bootstrap', 'client', 'worker']),
  'quickstart contract must expose exactly the bootstrap, client, and worker sources',
);

const provenance = quickstart.qualification_provenance;
assert(
  typeof provenance?.subject_base === 'string'
    && provenance.subject_base in quickstart.reference_resolution.bases,
  'qualification provenance must select a declared reference base',
);
assert(
  provenance?.evidence?.kind === 'github_actions_workflow',
  'qualification provenance must use a public GitHub Actions workflow identity',
);
for (const field of ['api_url', 'web_url']) {
  const url = new URL(provenance.evidence[field]);
  assert(url.protocol === 'https:', `qualification provenance ${field} must use HTTPS`);
}
assert(
  jsonPointer(
    quickstart,
    provenance.evidence.version_input?.value_pointer,
    'qualification version input pointer',
  ) === quickstart.package.published_version,
  'qualification provenance must bind its input to the published package version',
);
assert(!('published_smoke' in quickstart), 'quickstart contract must not expose a repository-local smoke path');

for (const example of contract.examples || []) {
  const context = `${example.path}#${example.id}`;
  const source = await readFile(new URL(example.path, repoRoot), 'utf8');
  const match = source.match(examplePattern(example.id));
  if (!match) {
    throw new Error(`${context} is missing its marked fenced block`);
  }
  if ((match[1] || '') !== example.language) {
    throw new Error(`${context} must use a ${example.language} fenced block`);
  }
  const executableSource = await readFile(new URL(example.source, repoRoot), 'utf8');
  if (`${match[2]}\n` !== executableSource) {
    throw new Error(`${context} must render the shipped executable ${example.source} without drift`);
  }
  for (const apiMethod of example.publicApiMethods || []) {
    const declaration = await readFile(new URL(apiMethod.declaration, repoRoot), 'utf8');
    assert(
      publicMethodPattern(apiMethod.method).test(declaration),
      `${context} references missing public API ${apiMethod.declaration}::${apiMethod.method}()`,
    );
    assert(
      methodCallPattern(apiMethod.method).test(executableSource),
      `${context} does not exercise declared public API method ${apiMethod.method}()`,
    );
  }
  if (example.workflowIdentity) {
    checkWorkflowIdentity(match[2], example.workflowIdentity, context);
  }
}

const workerSource = await readFile(new URL(sourcePaths.worker, repoRoot), 'utf8');
const clientSource = await readFile(new URL(sourcePaths.client, repoRoot), 'utf8');
const bootstrapSource = await readFile(new URL(sourcePaths.bootstrap, repoRoot), 'utf8');
for (const autoloadCandidate of [
  "__DIR__.'/vendor/autoload.php'",
  "dirname(__DIR__).'/vendor/autoload.php'",
  "dirname(__DIR__, 2).'/vendor/autoload.php'",
  "dirname(__DIR__, 3).'/autoload.php'",
  "getcwd().'/vendor/autoload.php'",
]) {
  assert(
    bootstrapSource.includes(autoloadCandidate),
    `${sourcePaths.bootstrap} must support the ${autoloadCandidate} Composer layout`,
  );
}
for (const attribute of ['#[Workflow(', '#[Activity(']) {
  assert(workerSource.includes(attribute), `${sourcePaths.worker} must expose ${attribute}`);
}
assert(
  !workerSource.includes('yield ') && !workerSource.includes('Generator'),
  'quickstart worker must use straight-line workflow calls without generator syntax',
);
assert(
  workerSource.includes('->register(GreeterWorkflow::class, GreetingActivities::class)'),
  'quickstart worker must register both attributed handler classes',
);
assert(
  workerSource.includes("workerToken: quickstartEnvironment('DURABLE_WORKFLOW_WORKER_TOKEN')"),
  'quickstart worker must consume only its scoped credential',
);
assert(
  clientSource.includes("controlToken: quickstartEnvironment('DURABLE_WORKFLOW_CLIENT_TOKEN')"),
  'quickstart client must consume only its scoped credential',
);
assert(
  !workerSource.includes('DURABLE_WORKFLOW_CLIENT_TOKEN')
    && !clientSource.includes('DURABLE_WORKFLOW_WORKER_TOKEN'),
  'quickstart sources must keep role credentials separate',
);

console.log(`Documentation example checks passed for ${contract.examples.length} examples`);
