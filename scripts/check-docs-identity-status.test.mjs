import assert from 'node:assert/strict';
import {test} from 'node:test';
import {publicStatus, fetchPublicStatus, watchPublicStatus, STATUS_API} from '../docs/portal/assets/identity/status.mjs';

const now = Date.parse('2026-11-05T12:00:30Z');
function snapshot() {
  return {
    schema: 'durable_workflow.status_page_snapshot.v1', product: 'cloud', environment: 'prod',
    checked_at: '2026-11-05T12:00:00Z', generated_at: '2026-11-05T12:00:05Z',
    status: 'ok', alert_level: 'ok', customer_visible: false,
    freshness: {status: 'fresh', age_seconds: 30, stale_after_seconds: 900},
    components: [], customer_impact: {status: 'ok', alert_level: 'ok', affected_families: []},
  };
}
const state = s => publicStatus(s, now).state;
test('fresh public aggregate is healthy with no customer-visible incident rows', () => {
  assert.equal(state(snapshot()), 'operational');
  assert.equal(publicStatus(snapshot(), now).validUntil, Date.parse('2026-11-05T12:15:00Z'));
});
test('public impact cannot be hidden by healthy top-level fields', () => {
  for (const [status, level, expected] of [['failed','critical','incident'],['warning','warning','degraded'],['unknown','unknown','unknown']]) {
    const s = snapshot(); s.customer_impact.status = status; s.customer_impact.alert_level = level;
    assert.equal(state(s), expected);
  }
});
test('severity tokens and maintenance-only affected family follow producer precedence', () => {
  const s = snapshot(); s.status = 'warning'; s.alert_level = 'warning';
  assert.equal(state(s), 'degraded');
  s.customer_impact.affected_families = ['maintenance'];
  assert.equal(state(s), 'maintenance');
  s.customer_impact.affected_families.push('managed_runtime');
  assert.equal(state(s), 'degraded');
  s.alert_level = 'critical'; assert.equal(state(s), 'incident');
});
test('public failure reasons retain unavailable, unverified and stale distinctions', () => {
  for (const [reason, expected] of [['cloud_unreachable','unavailable'],['monitor_stale','unverified'],['probe_unverified','unverified'],['publisher_stale','stale'],['new_reason','unknown']]) {
    const s = snapshot(); s.meta = {public_status_reason: reason}; assert.equal(state(s), expected);
  }
});
test('freshness is recomputed from checked time, including exact cutoff', () => {
  const s = snapshot();
  assert.equal(publicStatus(s, Date.parse('2026-11-05T12:14:59Z')).state, 'operational');
  assert.equal(publicStatus(s, Date.parse('2026-11-05T12:15:00Z')).state, 'stale');
  s.freshness.age_seconds = 900; assert.equal(state(s), 'stale');
  s.freshness.age_seconds = 30; s.freshness.status = 'stale'; assert.equal(state(s), 'stale');
});
test('future, malformed, invalid and out-of-order evidence cannot show healthy', () => {
  const mutations = [
    s => s.checked_at = '2026-11-05T12:01:00Z',
    s => s.generated_at = '2026-11-05T12:01:00Z',
    s => s.generated_at = '2026-11-05T11:59:00Z',
    s => s.checked_at = '2026-99-99T12:00:00Z',
    s => s.checked_at = '2026-11-05',
    s => s.freshness.age_seconds = -1,
    s => s.freshness.stale_after_seconds = 86400,
    s => s.freshness.status = 'unrecognized',
    s => s.schema = 'unknown', s => s.product = 'server', s => s.environment = 'test',
    s => s.customer_impact = undefined, s => s.status = 'healthy', s => s.alert_level = 'healthy',
  ];
  for (const mutate of mutations) {const s = snapshot(); mutate(s); assert.equal(state(s), 'unknown');}
});
test('private/internal rows are ignored and never returned', () => {
  const s = snapshot(); s.components = [{status: 'failed', message: 'private'}]; s.operator_alerts = ['private'];
  const result = publicStatus(s, now);
  assert.deepEqual(Object.keys(result).sort(), ['state','validUntil']);
  assert.equal(result.state, 'operational');
});
test('anonymous feed uses exact public GET contract', async () => {
  let request;
  const result = await fetchPublicStatus({now: () => now, fetchImpl: async (url, options) => {
    request = {url, options}; return {ok: true, text: async () => JSON.stringify(snapshot())};
  }});
  assert.equal(result.state, 'operational'); assert.equal(request.url, STATUS_API);
  for (const [k,v] of Object.entries({method:'GET',mode:'cors',credentials:'omit',cache:'no-store',redirect:'error',referrerPolicy:'no-referrer'})) assert.equal(request.options[k], v);
});
test('HTTP, oversized and malformed responses refuse', async () => {
  for (const response of [{ok:false},{ok:true,text:async()=> 'x'.repeat(65537)},{ok:true,text:async()=> '{'}]) {
    await assert.rejects(fetchPublicStatus({fetchImpl: async () => response}));
  }
});
test('network failure publishes unknown and unmount aborts pending work', async () => {
  const values = []; let signal;
  const stop = watchPublicStatus(value => values.push(value), {fetchImpl: async (_, options) => {signal = options.signal; throw new Error('offline');}});
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(values, ['unknown']); stop(); assert.equal(signal.aborted, true);
  const pending = [];
  const stopPending = watchPublicStatus(value => pending.push(value), {fetchImpl: (_, options) => new Promise((_, reject) => options.signal.addEventListener('abort', () => reject(new Error('aborted'))))});
  stopPending(); await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(pending, []);
});
test('a successful result expires while a later request is still pending', async t => {
  t.mock.timers.enable({apis: ['setTimeout', 'Date'], now});
  const values = []; let requests = 0;
  const s = snapshot(); s.freshness.stale_after_seconds = 90;
  const stop = watchPublicStatus(value => values.push(value), {fetchImpl: async (_, options) => {
    requests++;
    if (requests === 1) return {ok:true, text:async()=>JSON.stringify(s)};
    return new Promise((_, reject) => options.signal.addEventListener('abort', () => reject(new Error('timeout'))));
  }});
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(values, ['operational']);
  t.mock.timers.tick(60000);
  assert.equal(values.at(-1), 'stale');
  assert.equal(requests, 2);
  t.mock.timers.tick(5000);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(values.at(-1), 'unknown');
  stop();
});
test('calendar overflow and 24-hour normalization cannot create fresh evidence', () => {
  for (const [bad, normalized] of [['2026-02-30T12:00:00Z','2026-03-02T12:00:00Z'],['2026-11-04T24:00:00Z','2026-11-05T00:00:00Z']]) {
    const s = snapshot(); const normalizedMs = Date.parse(normalized);
    s.checked_at = bad; s.generated_at = new Date(normalizedMs + 5000).toISOString();
    assert.equal(publicStatus(s, normalizedMs + 30000).state, 'unknown');
  }
});
test('valid offsets and subsecond timestamps preserve the same checked instant', () => {
  const s = snapshot(); s.checked_at = '2026-11-05T14:00:00.000000+02:00'; s.generated_at = '2026-11-05T07:00:05-05:00';
  assert.equal(state(s), 'operational');
  assert.equal(publicStatus(s, now).validUntil, Date.parse('2026-11-05T12:15:00Z'));
});
