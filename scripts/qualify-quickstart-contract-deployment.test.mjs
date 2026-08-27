import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createServer} from 'node:http';
import test from 'node:test';
import {fileURLToPath} from 'node:url';

import {qualifyDeployment} from './qualify-quickstart-contract-deployment.mjs';
import {resolvePublishedRelease} from './qualify-quickstart-release-availability.mjs';

const repoRoot = new URL('../', import.meta.url);
const packageRoot = fileURLToPath(repoRoot);
const sourceContract = JSON.parse(
  await readFile(new URL('docs/quickstart-contract.json', repoRoot), 'utf8'),
);
const sourceManifest = JSON.parse(
  await readFile(new URL('composer.json', repoRoot), 'utf8'),
);
const sourceSchema = JSON.parse(
  await readFile(new URL('docs/quickstart-contract.schema.v2.json', repoRoot), 'utf8'),
);

async function fixture(mutator = () => {}) {
  const contract = structuredClone(sourceContract);
  const schema = structuredClone(sourceSchema);
  const requests = [];
  mutator(contract, schema);

  const server = createServer((request, response) => {
    requests.push({authorization: request.headers.authorization, url: request.url});
    const origin = `http://127.0.0.1:${server.address().port}`;
    const documents = {
      '/quickstart-contract.json': contract,
      '/quickstart-contract.schema.v2.json': schema,
      '/workflow': {id: 1, name: 'Published service-mode smoke', state: 'active'},
      '/workflow-inactive': {id: 1, name: 'Published service-mode smoke', state: 'disabled_manually'},
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
    contract,
    origin,
    requests,
    close: () => new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve())),
  };
}

test('deployed references resolve through the exact published package and public evidence', async () => {
  const deployed = await fixture();
  try {
    const result = await qualifyDeployment({
      contractUrl: deployed.contractUrl,
      githubToken: 'fixture-token',
      packageRoot,
    });
    assert.deepEqual(Object.keys(result.sources).sort(), ['bootstrap', 'client', 'worker']);
    assert.equal(
      result.package,
      `${sourceContract.package.name}:${sourceContract.package.published_version}`,
    );
    assert(deployed.requests.length >= 4);
    assert(deployed.requests.every((request) => request.authorization === undefined));
  } finally {
    await deployed.close();
  }
});

test('workflow authentication is removed from redirects and non-GitHub origins', async () => {
  const deployed = await fixture();
  const requests = [];
  const apiUrl =
    'https://api.github.com/repos/durable-workflow/sdk-php/actions/workflows/service-mode-published-smoke.yml';
  const sameOriginRedirect =
    'https://api.github.com/repos/durable-workflow/sdk-php/actions/workflows/redirected.yml';
  const redirectedUrl = 'https://api.github.com.attacker.invalid/workflow';
  deployed.contract.qualification_provenance.evidence.api_url = apiUrl;

  const fetchImpl = async (input, options) => {
    const url = new URL(input);
    const authorization = new Headers(options.headers).get('authorization');
    requests.push({authorization, url: url.href});
    if (url.href === apiUrl) {
      return new Response(null, {status: 302, headers: {location: sameOriginRedirect}});
    }
    if (url.href === sameOriginRedirect) {
      return new Response(null, {status: 302, headers: {location: redirectedUrl}});
    }
    if (url.href === redirectedUrl) {
      return Response.json({id: 1, name: 'Published service-mode smoke', state: 'active'});
    }
    return fetch(input, options);
  };

  try {
    await qualifyDeployment({
      contractUrl: deployed.contractUrl,
      fetchImpl,
      githubToken: 'fixture-token',
      packageRoot,
    });
    assert.equal(
      requests.find((request) => request.url === apiUrl)?.authorization,
      'Bearer fixture-token',
    );
    assert.equal(
      requests.find((request) => request.url === sameOriginRedirect)?.authorization,
      null,
    );
    assert.equal(
      requests.find((request) => request.url === redirectedUrl)?.authorization,
      null,
    );
    assert(deployed.requests.every((request) => request.authorization === undefined));
  } finally {
    await deployed.close();
  }
});

test('portal and schema requests cannot opt into GitHub workflow authentication', async () => {
  const deployed = await fixture();
  const githubContractUrl = 'https://api.github.com/portal/quickstart-contract.json';
  const githubSchemaUrl = 'https://api.github.com/portal/quickstart-contract.schema.v2.json';
  deployed.contract.$schema = githubSchemaUrl;
  const schema = structuredClone(sourceSchema);
  schema.$id = githubSchemaUrl;
  schema.properties.$schema.const = githubSchemaUrl;
  deployed.contract.qualification_provenance.evidence = structuredClone(
    sourceContract.qualification_provenance.evidence,
  );
  const requests = [];

  const fetchImpl = async (input, options) => {
    const url = new URL(input);
    requests.push({
      authorization: new Headers(options.headers).get('authorization'),
      url: url.href,
    });
    if (url.href === githubContractUrl) return Response.json(deployed.contract);
    if (url.href === githubSchemaUrl) return Response.json(schema);
    if (url.href === deployed.contract.qualification_provenance.evidence.api_url) {
      return Response.json({id: 1, name: 'Published service-mode smoke', state: 'active'});
    }
    if (url.href === deployed.contract.qualification_provenance.evidence.web_url) {
      return new Response('<!doctype html><title>Public qualification</title>', {
        headers: {'content-type': 'text/html'},
      });
    }
    return fetch(input, options);
  };

  try {
    await qualifyDeployment({
      contractUrl: githubContractUrl,
      fetchImpl,
      githubToken: 'fixture-token',
      packageRoot,
    });
    assert.equal(requests.find((request) => request.url === githubContractUrl)?.authorization, null);
    assert.equal(requests.find((request) => request.url === githubSchemaUrl)?.authorization, null);
  } finally {
    await deployed.close();
  }
});

test('GitHub workflow failures have bounded, actionable classifications', async (t) => {
  const cases = [
    {
      name: 'authentication failure',
      response: () => new Response('sensitive upstream body', {status: 401}),
      expected: /authentication failed \(HTTP 401\).*actions: read/,
    },
    {
      name: 'secondary limit with nonzero primary quota',
      response: () => new Response('sensitive upstream body', {
        status: 403,
        headers: {
          'retry-after': '60',
          'x-ratelimit-remaining': '4999',
          'x-ratelimit-reset': '1790000000',
        },
      }),
      expected: /secondary rate limit exceeded \(HTTP 403; remaining=4999; retry-after=60; reset=1790000000\); retry after 60 seconds/,
    },
    {
      name: 'secondary limit with absent primary quota',
      response: () => Response.json(
        {message: 'You have exceeded a secondary rate limit. Sensitive detail is omitted.'},
        {status: 403},
      ),
      expected: /secondary rate limit exceeded \(HTTP 403; remaining=unknown\); wait before retrying/,
    },
    {
      name: 'primary rate limit exhaustion',
      response: () => new Response('sensitive upstream body', {
        status: 403,
        headers: {'x-ratelimit-remaining': '0', 'x-ratelimit-reset': '1790000000'},
      }),
      expected: /primary rate limit exhausted \(HTTP 403; remaining=0; reset=1790000000\).*retry/,
    },
    {
      name: 'permission denial',
      response: () => Response.json({message: 'Resource not accessible by integration'}, {status: 403}),
      expected: /access was forbidden \(HTTP 403\).*actions: read/,
    },
    {
      name: 'HTTP 429 without primary quota evidence',
      response: () => new Response('sensitive upstream body', {status: 429}),
      expected: /GitHub rate limit response \(HTTP 429; remaining=unknown\); wait before retrying/,
    },
    {
      name: 'oversized structured message is not trusted',
      response: () => Response.json(
        {message: `secondary rate limit ${'sensitive'.repeat(600)}`},
        {status: 403, headers: {'retry-after': 'invalid', 'x-ratelimit-reset': 'invalid'}},
      ),
      expected: /access was forbidden \(HTTP 403\).*actions: read/,
    },
    {
      name: 'missing workflow',
      response: () => new Response('sensitive upstream body', {status: 404}),
      expected: /workflow is missing or inaccessible \(HTTP 404\).*public workflow API URL/,
    },
  ];

  for (const failure of cases) {
    await t.test(failure.name, async () => {
      const deployed = await fixture();
      deployed.contract.qualification_provenance.evidence.api_url =
        'https://api.github.com/repos/durable-workflow/sdk-php/actions/workflows/missing.yml';
      const fetchImpl = async (input, options) => {
        if (new URL(input).origin === 'https://api.github.com') return failure.response();
        return fetch(input, options);
      };
      try {
        await assert.rejects(
          qualifyDeployment({
            contractUrl: deployed.contractUrl,
            fetchImpl,
            githubToken: 'fixture-secret-token',
            packageRoot,
          }),
          (error) => {
            assert.match(error.message, failure.expected);
            assert(!error.message.includes('sensitive upstream body'));
            assert(!error.message.includes('Sensitive detail'));
            assert(!error.message.includes('fixture-secret-token'));
            assert(error.message.length < 300);
            return true;
          },
        );
      } finally {
        await deployed.close();
      }
    });
  }
});

test('inactive workflow evidence is distinct from transport failures', async () => {
  const deployed = await fixture();
  deployed.contract.qualification_provenance.evidence.api_url = `${deployed.origin}/workflow-inactive`;
  try {
    await assert.rejects(
      qualifyDeployment({contractUrl: deployed.contractUrl, githubToken: '', packageRoot}),
      /workflow is inactive \(state="disabled_manually"\)/,
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
  assert.match(workflow, /- 'scripts\/qualify-quickstart-\*'/);
  assert.match(workflow, /needs\.build\.outputs\.release_published == 'true'/);
  assert.match(workflow, /schedule:\n\s+- cron:/);
  assert.match(
    workflow,
    /qualify-deployment:[\s\S]*?permissions:\n\s+actions: read\n\s+contents: read[\s\S]*?GITHUB_TOKEN: \$\{\{ github\.token \}\}/,
  );
});

test('portal deployment waits for the exact source-declared release', async () => {
  const requests = [];
  const unavailable = await resolvePublishedRelease(sourceManifest, {
    inspector: async (packageName, version) => {
      requests.push([packageName, version]);
      return null;
    },
  });

  assert.deepEqual(requests, [[sourceContract.package.name, sourceContract.package.published_version]]);
  assert.equal(
    sourceManifest.extra['durable-workflow']['product-train'],
    sourceContract.package.published_version,
  );
  assert.deepEqual(unavailable, {
    release: sourceContract.package.published_version,
    published: false,
  });
});

test('portal deployment accepts only exact Composer release metadata', async () => {
  const manifest = {
    extra: {'durable-workflow': {'product-train': sourceContract.package.published_version}},
  };
  const published = await resolvePublishedRelease(manifest, {
    inspector: async () => ({
      name: sourceContract.package.name,
      versions: [sourceContract.package.published_version],
    }),
  });
  assert.equal(published.published, true);

  await assert.rejects(
    resolvePublishedRelease(manifest, {
      inspector: async () => ({
        name: sourceContract.package.name,
        versions: ['2.0.0-rc.14'],
      }),
    }),
    /did not resolve only the source-declared release identity/,
  );
});
