import {readFile} from 'node:fs/promises';

const repoRoot = new URL('../', import.meta.url);
const contract = JSON.parse(
  await readFile(new URL('scripts/check-docs-examples-contract.json', repoRoot), 'utf8'),
);
const quickstart = JSON.parse(
  await readFile(new URL(contract.quickstartContract, repoRoot), 'utf8'),
);

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

const composer = JSON.parse(await readFile(new URL('composer.json', repoRoot), 'utf8'));
assert(quickstart.schema_version === 1, 'quickstart contract must use schema version 1');
assert(quickstart.package?.name === composer.name, 'quickstart package name must match composer.json');
assert(
  quickstart.package?.published_version === composer.extra?.['durable-workflow']?.['product-train'],
  'quickstart published version must match the Composer product train',
);
assert(
  quickstart.package?.composer_requirement === `${quickstart.package.published_version}@RC`,
  'quickstart Composer requirement must select the published release candidate',
);

const server = new URL(quickstart.runtime_targets?.server?.client_input);
const serverRequest = new URL(quickstart.runtime_targets?.server?.example_request);
assert(server.pathname === '/', 'Server client input must be a bare origin');
assert(
  quickstart.runtime_targets?.server?.namespace_input === 'default',
  'Server quickstart must use the default local namespace',
);
assert(serverRequest.pathname === '/api/workflows', 'Server request example must contain one SDK API segment');

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
  if (example.workflowIdentity) {
    checkWorkflowIdentity(match[2], example.workflowIdentity, context);
  }
}

const workerSource = await readFile(new URL(quickstart.sources.worker, repoRoot), 'utf8');
const clientSource = await readFile(new URL(quickstart.sources.client, repoRoot), 'utf8');
const bootstrapSource = await readFile(new URL(quickstart.sources.bootstrap, repoRoot), 'utf8');
for (const autoloadCandidate of [
  "__DIR__.'/vendor/autoload.php'",
  "dirname(__DIR__).'/vendor/autoload.php'",
  "dirname(__DIR__, 2).'/vendor/autoload.php'",
  "dirname(__DIR__, 3).'/autoload.php'",
  "getcwd().'/vendor/autoload.php'",
]) {
  assert(
    bootstrapSource.includes(autoloadCandidate),
    `${quickstart.sources.bootstrap} must support the ${autoloadCandidate} Composer layout`,
  );
}
for (const attribute of ['#[Workflow(', '#[Activity(']) {
  assert(workerSource.includes(attribute), `${quickstart.sources.worker} must expose ${attribute}`);
}
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
