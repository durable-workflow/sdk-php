import assert from 'node:assert/strict';

const origin = process.env.EXAMPLE_ORIGIN ?? 'http://nginx:8080';
const prefix = process.env.EXAMPLE_RUN_PREFIX;
assert.match(prefix ?? '', /^[a-zA-Z0-9_-]{1,40}$/, 'Select a fresh run prefix before admission.');
let requestNumber = 0;
const originals = [];

async function request(user, path, method = 'GET', body, expectedStatus = 200, timeout = 30_000) {
    const response = await fetch(`${origin}/api/${path}`, {
        method, signal: AbortSignal.timeout(Math.max(1, Math.floor(timeout))),
        headers: {Authorization: `Bearer example-${user}`, Accept: 'application/json',
            'Content-Type': 'application/json', 'X-Request-Id': `${prefix}-request-${requestNumber++}`},
        body: body === undefined ? undefined : JSON.stringify(body),
    });
    const raw = await response.text();
    assert.equal(response.status, expectedStatus, raw);
    return {raw, value: raw === '' ? null : JSON.parse(raw)};
}

async function waitStatus(user, id, expected) {
    const deadline = performance.now() + 35_000;
    do {
        const result = await request(user, `workflows/${id}`, 'GET', undefined, 200, Math.min(30_000, deadline - performance.now()));
        assert.ok(performance.now() <= deadline, 'The original observation deadline expired.');
        if (result.value.status?.toLowerCase() === expected) return result;
        await new Promise(resolve => setTimeout(resolve, 250));
    } while (performance.now() < deadline);
    throw new Error(`Original ${user}/${id} did not reach ${expected} within its 35-second observer.`);
}

for (const [user, kind, suffix] of [
    ['alpha', 'short', 'short-a'], ['beta', 'short', 'short-b'],
    ['alpha', 'timer', 'timer'], ['beta', 'signal', 'signal'], ['alpha', 'cancel', 'cancel'],
]) {
    const id = `${prefix}-${suffix}`;
    const {value} = await request(user, 'workflows', 'POST', {id, kind}, 201);
    assert.equal(value.workflow_id, `${user}-${id}`);
    assert.equal(typeof value.run_id, 'string');
    originals.push({user, kind, id, workflow_id: value.workflow_id, run_id: value.run_id});
}

for (const original of originals.filter(value => ['short', 'timer'].includes(value.kind))) {
    const result = await waitStatus(original.user, original.id, 'completed');
    assert.equal(result.value.output.user, original.user);
    assert.equal(result.value.output.echo, `hello ${original.user}`);
    assert.equal(result.value.output.nullable, null);
    assert.equal(result.value.output.text, '雪');
    assert.match(result.raw, /"number":9007199254740993(?:,|})/);
}

const signal = originals.find(value => value.kind === 'signal');
await waitStatus(signal.user, signal.id, 'waiting');
assert.equal((await request(signal.user, `workflows/${signal.id}/query`, 'POST')).value.phase, 'waiting');
await request(signal.user, `workflows/${signal.id}/signal`, 'POST', undefined, 204);
await waitStatus(signal.user, signal.id, 'completed');

const cancel = originals.find(value => value.kind === 'cancel');
await waitStatus(cancel.user, cancel.id, 'waiting');
assert.equal((await request(cancel.user, `workflows/${cancel.id}/query`, 'POST')).value.phase, 'waiting');
await request(cancel.user, `workflows/${cancel.id}/cancel`, 'POST', undefined, 204);
await waitStatus(cancel.user, cancel.id, 'cancelled');
await request('beta', `workflows/${prefix}-short-a`, 'GET', undefined, 404);

console.log(JSON.stringify({originals, completed: 4, cancelled: 1, queries: 2, signal: 1, foreign_user_refusal: 404}));
