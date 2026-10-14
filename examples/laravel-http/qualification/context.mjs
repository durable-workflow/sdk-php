import assert from 'node:assert/strict';

const origin = process.env.EXAMPLE_ORIGIN ?? 'http://nginx:8080';
const namespace = process.env.DURABLE_WORKFLOW_NAMESPACE ?? 'service-http-example';
const pids = new Set();

for (let index = 0; index < 40; index++) {
    const user = index % 2 === 0 ? 'alpha' : 'beta';
    const headers = {
        Authorization: `Bearer example-${user}`,
        Accept: 'application/json',
        'X-Request-Id': `context-${index}`,
        'X-Namespace': namespace,
    };
    if (index % 3 === 0) headers['X-Optional-Context'] = `optional-${index}`;
    const response = await fetch(`${origin}/api/context`, {headers, signal: AbortSignal.timeout(10_000)});
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), {user, namespace});
    assert.equal(response.headers.get('X-Request-Id'), `context-${index}`);
    assert.equal(response.headers.get('X-Example-User'), user);
    assert.equal(response.headers.get('X-Example-Namespace'), namespace);
    assert.equal(response.headers.get('X-Optional-Context'), headers['X-Optional-Context'] ?? '');
    assert.match(response.headers.get('X-Worker-Pid'), /^\d+$/);
    pids.add(response.headers.get('X-Worker-Pid'));
}

for (const [headers, expectedStatus] of [
    [{}, 401],
    [{Authorization: 'Bearer example-alpha', 'X-Request-Id': 'foreign', 'X-Namespace': `${namespace}-foreign`}, 403],
    [{Authorization: 'Bearer example-beta', 'X-Request-Id': 'invalid!'}, 422],
]) {
    const response = await fetch(`${origin}/api/context`, {
        headers: {...headers, Accept: 'application/json'}, signal: AbortSignal.timeout(10_000),
    });
    assert.equal(response.status, expectedStatus);
    await response.arrayBuffer();
}

console.log(JSON.stringify({requests: 40, refusals: 3, http_worker_pids: [...pids]}));
