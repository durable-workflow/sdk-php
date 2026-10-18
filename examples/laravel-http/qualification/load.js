import http from 'k6/http';
import {check, sleep} from 'k6';
import execution from 'k6/execution';
import {Counter, Trend} from 'k6/metrics';

const origin = __ENV.EXAMPLE_ORIGIN || 'http://nginx:8080';
const namespace = __ENV.DURABLE_WORKFLOW_NAMESPACE;
const prefix = __ENV.EXAMPLE_RUN_PREFIX;
const seconds = Number(__ENV.EXAMPLE_DURATION_SECONDS || 120);
const mixed = __ENV.EXAMPLE_MIXED === 'true';
const requestTimeoutMs = 10000;
// Deliver the fixed context rate while every still-on-time request is in flight.
const contextUsers = 6 * (requestTimeoutMs / 1000) + 4;
if (!namespace || !/^[a-zA-Z0-9_-]{1,30}$/.test(prefix || '') || !Number.isInteger(seconds) || seconds < 5 || seconds > 300) {
    throw new Error('Select a fixed namespace, fresh prefix and a 5–300 second offered window.');
}

const offered = new Counter('workflow_offered');
const admitted = new Counter('workflow_admitted');
const completed = new Counter('workflow_completed');
const cancelled = new Counter('workflow_cancelled');
const failed = new Counter('workflow_failed');
const terminal = new Counter('workflow_terminal');
const contextCompleted = new Counter('context_completed');
const errors = new Counter('run_errors');
const boundaryIterations = new Counter('generator_boundary_iterations');
const observed = new Trend('workflow_observed_ms', true);
const timings = Object.fromEntries(['context', 'start', 'status', 'query', 'signal', 'cancel']
    .map(name => [name, new Trend(`${name}_http_ms`, true)]));
let requestNumber = 0;
const seenPids = new Set();

export const options = {
    scenarios: {
        context: {executor: 'constant-arrival-rate', rate: 6, timeUnit: '1s', duration: `${seconds}s`,
            preAllocatedVUs: contextUsers, maxVUs: contextUsers, exec: 'context', gracefulStop: '36s'},
        workflows: {executor: 'constant-arrival-rate', rate: 1, timeUnit: '1s', duration: `${seconds}s`,
            preAllocatedVUs: 16, maxVUs: 16, exec: 'workflow', gracefulStop: '36s'},
    },
    thresholds: {
        checks: ['rate==1'],
        run_errors: ['count==0'],
        http_req_failed: ['rate==0'],
        dropped_iterations: ['count==0'],
        workflow_offered: [`count==${seconds}`],
        workflow_admitted: [`count==${seconds}`],
        workflow_terminal: [`count==${seconds}`],
        context_completed: [`count==${seconds * 6}`],
    },
    summaryTrendStats: ['count', 'avg', 'min', 'med', 'max', 'p(95)', 'p(99)'],
};

function requireCheck(condition, message) {
    if (!check(condition, {[message]: value => value === true})) throw new Error(message);
}

function header(response, name) {
    const key = Object.keys(response.headers).find(key => key.toLowerCase() === name.toLowerCase());
    return key === undefined ? '' : response.headers[key];
}

function request(user, path, name, method = 'GET', body, expected = 200, remainingMs = 10000) {
    if (remainingMs <= 0) throw new Error('Original 35-second observation expired.');
    const transportLimitMs = name === 'query' ? 30000 : requestTimeoutMs;
    const requestId = `${prefix}-${__VU}-${__ITER}-${requestNumber++}`;
    const response = http.request(method, `${origin}/api/${path}`, body === undefined ? null : JSON.stringify(body), {
        headers: {Authorization: `Bearer example-${user}`, Accept: 'application/json',
            'Content-Type': 'application/json', 'X-Request-Id': requestId, 'X-Namespace': namespace},
        timeout: `${Math.max(1, Math.floor(Math.min(transportLimitMs, remainingMs)))}ms`,
        tags: {name}, redirects: 0,
    });
    timings[name].add(response.timings.duration);
    if (response.status !== expected) {
        console.error(JSON.stringify({phase: 'request_failed', name, request_id: requestId,
            status: response.status, error_code: response.error_code, timings: response.timings}));
    }
    requireCheck(response.status === expected, `${name} returned expected status`);
    requireCheck(header(response, 'X-Request-Id') === requestId, 'request context is isolated');
    requireCheck(header(response, 'X-Example-User') === user, 'user context is isolated');
    requireCheck(header(response, 'X-Example-Namespace') === namespace, 'namespace context is fixed');
    const pid = header(response, 'X-Worker-Pid');
    requireCheck(/^\d+$/.test(pid), 'HTTP worker identity is present');
    if (!seenPids.has(pid)) {
        seenPids.add(pid);
        console.log(JSON.stringify({phase: 'http_worker_observed', pid}));
    }
    return response;
}

export function context() {
    errors.add(0);
    boundaryIterations.add(0);
    const index = execution.scenario.iterationInTest;
    if (index >= seconds * 6 || Date.now() >= execution.scenario.startTime + seconds * 1000) {
        boundaryIterations.add(1);
        return;
    }
    try {
        const user = index % 2 === 0 ? 'alpha' : 'beta';
        const response = request(user, 'context', 'context');
        const value = response.json();
        requireCheck(value.user === user && value.namespace === namespace, 'context values match');
        requireCheck(value.client_resolved_at_boot === false, 'fixed lazy profile');
        requireCheck(header(response, 'X-Optional-Context') === '', 'optional context does not leak');
        contextCompleted.add(1);
    } catch (error) {
        errors.add(1);
        console.error(JSON.stringify({phase: 'context_failed', message: String(error)}));
    }
}

function waitStatus(user, id, expected, cutoff) {
    while (Date.now() < cutoff) {
        const response = request(user, `workflows/${id}`, 'status', 'GET', undefined, 200, cutoff - Date.now());
        requireCheck(Date.now() <= cutoff, 'original observer has not expired');
        const status = (response.json().status || '').toLowerCase();
        if (status === expected) return response;
        if (['completed', 'failed', 'cancelled', 'terminated', 'timed_out'].includes(status)) {
            throw new Error(`Original reached unexpected terminal status ${status}.`);
        }
        sleep(Math.min(0.25, Math.max(0, cutoff - Date.now()) / 1000));
    }
    throw new Error('Original 35-second observation expired.');
}

function waitForActivity(user, id, cutoff) {
    while (Date.now() < cutoff) {
        const phase = request(user, `workflows/${id}/query`, 'query', 'POST', undefined, 200, cutoff - Date.now()).json().phase;
        requireCheck(Date.now() <= cutoff, 'original observer has not expired');
        requireCheck(['activity', 'waiting'].includes(phase), 'query returns a known activity phase');
        if (phase === 'waiting') return;
        sleep(Math.min(0.25, Math.max(0, cutoff - Date.now()) / 1000));
    }
    throw new Error('Activity was not committed within the original 35-second observation.');
}

export function workflow() {
    errors.add(0);
    failed.add(0);
    completed.add(0);
    cancelled.add(0);
    boundaryIterations.add(0);
    const index = execution.scenario.iterationInTest;
    const started = Date.now();
    if (index >= seconds || started >= execution.scenario.startTime + seconds * 1000) {
        boundaryIterations.add(1);
        return;
    }
    offered.add(1);
    const user = index % 2 === 0 ? 'alpha' : 'beta';
    const kind = mixed ? ['short', 'short', 'timer', 'signal', 'cancel'][index % 5] : 'short';
    const id = `${prefix}-${kind}-${index}`;
    const cutoff = started + 35000;
    try {
        const acceptance = request(user, 'workflows', 'start', 'POST', {id, kind}, 201, cutoff - Date.now()).json();
        admitted.add(1);
        console.log(JSON.stringify({phase: 'admitted', user, kind, id, workflow_id: acceptance.workflow_id, run_id: acceptance.run_id, started}));
        requireCheck(acceptance.workflow_id === `${user}-${id}` && typeof acceptance.run_id === 'string', 'accepted identity matches');
        if (kind === 'signal' || kind === 'cancel') {
            waitStatus(user, id, 'waiting', cutoff);
            waitForActivity(user, id, cutoff);
            request(user, `workflows/${id}/${kind === 'signal' ? 'signal' : 'cancel'}`,
                kind === 'signal' ? 'signal' : 'cancel', 'POST', undefined, 204, cutoff - Date.now());
        }
        const response = waitStatus(user, id, kind === 'cancel' ? 'cancelled' : 'completed', cutoff);
        if (kind === 'cancel') {
            cancelled.add(1);
        } else {
            const output = response.json().output;
            requireCheck(output.user === user && output.echo === `hello ${user}` && output.nullable === null && output.text === '雪', 'typed output matches');
            requireCheck(/"number":9007199254740993(?:,|})/.test(response.body), 'int64 wire bytes are exact');
            completed.add(1);
        }
        observed.add(Date.now() - started, {kind});
        terminal.add(1);
        console.log(JSON.stringify({phase: 'terminal', user, kind, id, workflow_id: acceptance.workflow_id, run_id: acceptance.run_id, elapsed_ms: Date.now() - started}));
    } catch (error) {
        failed.add(1);
        errors.add(1);
        console.error(JSON.stringify({phase: 'workflow_failed', user, kind, id, message: String(error)}));
    }
}

export function handleSummary(data) {
    data.example = {prefix, namespace, duration_seconds: seconds, context_rate: 6, workflow_rate: 1,
        context_virtual_users: contextUsers, context_request_timeout_ms: requestTimeoutMs,
        mixed, original_observer_seconds: 35, outer_drain_seconds: 36};
    return {stdout: JSON.stringify(data), [__ENV.EXAMPLE_SUMMARY_PATH || '/proof/summary.json']: JSON.stringify(data)};
}
