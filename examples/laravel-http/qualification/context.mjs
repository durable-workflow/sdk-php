import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';

const origin = process.env.EXAMPLE_ORIGIN ?? 'http://nginx:8080';
const namespace = process.env.DURABLE_WORKFLOW_NAMESPACE ?? 'service-http-example';
const pids = new Set();
const expectedEager = process.env.EXAMPLE_EXPECT_EAGER === 'true';
const prefix = process.env.EXAMPLE_REQUEST_PREFIX ?? 'context';

for (let index = 0; index < 40; index++) {
    const user = index % 2 === 0 ? 'alpha' : 'beta';
    const headers = {
        Authorization: `Bearer example-${user}`,
        Accept: 'application/json',
        'X-Request-Id': `${prefix}-${index}`,
        'X-Namespace': namespace,
    };
    if (index % 3 === 0) headers['X-Optional-Context'] = `optional-${index}`;
    const response = await fetch(`${origin}/api/context`, {headers, signal: AbortSignal.timeout(10_000)});
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), {user, namespace, client_resolved_at_boot: expectedEager});
    assert.equal(response.headers.get('X-Request-Id'), `${prefix}-${index}`);
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

let logRecords = 0;
if (process.env.EXAMPLE_LOG_PATH) {
    const contents = await readFile(process.env.EXAMPLE_LOG_PATH, 'utf8');
    const records = contents.split('\n').filter(line => line.includes('example.request {'))
        .map(line => JSON.parse(line.slice(line.indexOf('{'))));
    for (let index = 0; index < 40; index++) {
        const matches = records.filter(record => record.request_id === `${prefix}-${index}`);
        assert.equal(matches.length, 1);
        assert.equal(matches[0].user, index % 2 === 0 ? 'alpha' : 'beta');
        assert.equal(matches[0].namespace, namespace);
        assert.equal(matches[0].optional_context, index % 3 === 0 ? `optional-${index}` : null);
        assert.equal(matches[0].status, 200);
        logRecords++;
    }
}

console.log(JSON.stringify({requests: 40, refusals: 3, log_records: logRecords, eager_at_boot: expectedEager, http_worker_pids: [...pids]}));
