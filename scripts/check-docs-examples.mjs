import {readFile} from 'node:fs/promises';

const repoRoot = new URL('../', import.meta.url);
const contract = JSON.parse(
  await readFile(new URL('scripts/check-docs-examples-contract.json', repoRoot), 'utf8'),
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
  checkWorkflowIdentity(match[2], example.workflowIdentity, context);
}

console.log(`Documentation example checks passed for ${contract.examples.length} examples`);
