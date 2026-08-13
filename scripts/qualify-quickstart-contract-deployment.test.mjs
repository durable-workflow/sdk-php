import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createServer} from 'node:http';
import test from 'node:test';
import {fileURLToPath} from 'node:url';

import {qualifyDeployment} from './qualify-quickstart-contract-deployment.mjs';

const repoRoot = new URL('../', import.meta.url);
const packageRoot = fileURLToPath(repoRoot);
const sourceContract = JSON.parse(
  await readFile(new URL('docs/quickstart-contract.json', repoRoot), 'utf8'),
);
const sourceSchema = JSON.parse(
  await readFile(new URL('docs/quickstart-contract.schema.v2.json', repoRoot), 'utf8'),
);

async function fixture(mutator = () => {}) {
  const contract = structuredClone(sourceContract);
  const schema = structuredClone(sourceSchema);
  mutator(contract, schema);

  const server = createServer((request, response) => {
    const origin = `http://127.0.0.1:${server.address().port}`;
    const documents = {
      '/quickstart-contract.json': contract,
      '/quickstart-contract.schema.v2.json': schema,
      '/workflow': {id: 1, name: 'Published service-mode smoke', state: 'active'},
    };

    if (request.url === '/workflow/runs') {
      response.writeHead(200, {'content-type': 'text/html'});
      response.end('<!doctype html><title>Public qualification</title>');
      return;
    }
    if (!(request.url in documents)) {
      response.writeHead(404);
      response.end();
      return;
    }
    response.writeHead(200, {'content-type': 'application/json'});
    response.end(JSON.stringify(documents[request.url]));
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));

  const origin = `http://127.0.0.1:${server.address().port}`;
  contract.$schema = `${origin}/quickstart-contract.schema.v2.json`;
  schema.$id = contract.$schema;
  schema.properties.$schema.const = contract.$schema;
  contract.qualification_provenance.evidence.api_url = `${origin}/workflow`;
  contract.qualification_provenance.evidence.web_url = `${origin}/workflow/runs`;
  delete schema.$defs.github_actions_workflow.properties.api_url.pattern;
  delete schema.$defs.github_actions_workflow.properties.web_url.pattern;

  return {
    contractUrl: `${origin}/quickstart-contract.json`,
    close: () => new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve())),
  };
}

test('deployed references resolve through the exact published package and public evidence', async () => {
  const deployed = await fixture();
  try {
    const result = await qualifyDeployment({contractUrl: deployed.contractUrl, packageRoot});
    assert.deepEqual(Object.keys(result.sources).sort(), ['bootstrap', 'client', 'worker']);
    assert.equal(
      result.package,
      `${sourceContract.package.name}:${sourceContract.package.published_version}`,
    );
  } finally {
    await deployed.close();
  }
});

test('the deployed quickstart instance is rejected when its schema omits workflow authoring', async () => {
  const deployed = await fixture((_contract, schema) => {
    schema.required = schema.required.filter((property) => property !== 'workflow_authoring');
    delete schema.properties.workflow_authoring;
    delete schema.$defs.workflow_authoring;
  });
  try {
    await assert.rejects(
      qualifyDeployment({contractUrl: deployed.contractUrl, packageRoot}),
      /deployed quickstart contract does not satisfy its schema/,
    );
  } finally {
    await deployed.close();
  }
});

test('legacy repository-relative source strings are rejected', async () => {
  const deployed = await fixture((contract) => {
    contract.sources.bootstrap = 'examples/bootstrap.php';
  });
  try {
    await assert.rejects(
      qualifyDeployment({contractUrl: deployed.contractUrl, packageRoot}),
      /source bootstrap must be a Composer package path/,
    );
  } finally {
    await deployed.close();
  }
});

test('missing published package files are rejected', async () => {
  const deployed = await fixture((contract) => {
    contract.sources.client.path = 'examples/missing-client.php';
  });
  try {
    await assert.rejects(
      qualifyDeployment({contractUrl: deployed.contractUrl, packageRoot}),
      /published package source client is not consumable/,
    );
  } finally {
    await deployed.close();
  }
});

test('unresolvable qualification identities are rejected', async () => {
  const deployed = await fixture((contract) => {
    contract.qualification_provenance.evidence.version_input.value_pointer = '/package/missing_version';
  });
  try {
    await assert.rejects(
      qualifyDeployment({contractUrl: deployed.contractUrl, packageRoot}),
      /qualification version input pointer does not resolve/,
    );
  } finally {
    await deployed.close();
  }
});

test('the deployment workflow runs public quickstart reference qualification', async () => {
  const workflow = await readFile(new URL('../.github/workflows/docs.yml', import.meta.url), 'utf8');
  assert.match(workflow, /npm run qualify:quickstart-contract-deployment/);
});
