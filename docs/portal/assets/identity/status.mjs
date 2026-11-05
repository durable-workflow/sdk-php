// Shared public footer summary. No component rows, credentials or telemetry.
export const STATUS_API = 'https://status.durable-workflow.com/api/current.json';
const statuses = new Set(['ok', 'warning', 'failed', 'unknown']);
const levels = new Set(['ok', 'warning', 'critical', 'unknown']);
const unknown = () => ({state: 'unknown', validUntil: 0});
function timestamp(value) {
  if (typeof value !== 'string') return Number.NaN;
  const match = /^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.\d{1,9})?(Z|([+-])(\d\d):(\d\d))$/.exec(value);
  if (!match) return Number.NaN;
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) return Number.NaN;
  const offset = match[2] === 'Z' ? 0 : (Number(match[4]) * 60 + Number(match[5])) * (match[3] === '-' ? -1 : 1);
  // Date.parse normalizes impossible days and 24:00. Round-trip the supplied
  // calendar fields in their original offset before accepting the evidence.
  const suppliedCalendar = new Date(parsed + offset * 60000).toISOString().slice(0, 19);
  return suppliedCalendar === match[1] ? parsed : Number.NaN;
}

function evidenceWindow(snapshot, now) {
  if (snapshot?.schema !== 'durable_workflow.status_page_snapshot.v1'
    || snapshot.product !== 'cloud' || snapshot.environment !== 'prod' || !Number.isFinite(now)) return unknown();
  const checked = timestamp(snapshot.checked_at);
  const generated = timestamp(snapshot.generated_at);
  const freshness = snapshot.freshness;
  if (!Number.isFinite(checked) || !Number.isFinite(generated) || checked > now
    || generated > now || generated < checked || !freshness
    || !Number.isFinite(freshness.stale_after_seconds) || freshness.stale_after_seconds <= 0
    || freshness.stale_after_seconds > 900 || !Number.isFinite(freshness.age_seconds)
    || freshness.age_seconds < 0) return unknown();
  const validUntil = checked + freshness.stale_after_seconds * 1000;
  if (freshness.status === 'stale' || now >= validUntil || freshness.age_seconds >= freshness.stale_after_seconds) return {state: 'stale', validUntil: 0};
  if (freshness.status !== 'fresh') return unknown();
  return {validUntil};
}

function aggregateState(snapshot, validUntil) {
  const impact = snapshot.customer_impact;
  if (!statuses.has(snapshot.status) || !levels.has(snapshot.alert_level)
    || !impact || !statuses.has(impact.status) || !levels.has(impact.alert_level)) return unknown();
  const reason = snapshot.meta?.public_status_reason;
  if (reason === 'cloud_unreachable') return {state: 'unavailable', validUntil};
  if (reason === 'monitor_stale' || reason === 'probe_unverified') return {state: 'unverified', validUntil};
  if (reason === 'publisher_stale') return {state: 'stale', validUntil: 0};
  if (reason !== undefined && reason !== null && reason !== '') return unknown();
  const stateTokens = new Set([snapshot.status, impact.status]);
  const levelTokens = new Set([snapshot.alert_level, impact.alert_level]);
  if (stateTokens.has('failed') || levelTokens.has('critical')) return {state: 'incident', validUntil};
  if (stateTokens.has('warning') || levelTokens.has('warning')) {
    const families = impact.affected_families;
    const maintenanceOnly = Array.isArray(families) && families.length === 1 && families[0] === 'maintenance';
    return {state: maintenanceOnly ? 'maintenance' : 'degraded', validUntil};
  }
  if (stateTokens.has('unknown') || levelTokens.has('unknown')) return {state: 'unknown', validUntil};
  return {state: 'operational', validUntil};
}

export function publicStatus(snapshot, now = Date.now()) {
  const evidence = evidenceWindow(snapshot, now);
  return evidence.state ? evidence : aggregateState(snapshot, evidence.validUntil);
}

export async function fetchPublicStatus({fetchImpl = globalThis.fetch, now = Date.now, signal} = {}) {
  const response = await fetchImpl(STATUS_API, {
    method: 'GET', mode: 'cors', credentials: 'omit', cache: 'no-store',
    redirect: 'error', referrerPolicy: 'no-referrer', signal,
  });
  if (!response.ok) throw new Error('Public status unavailable');
  const body = await response.text();
  if (body.length > 65536) throw new Error('Public status response too large');
  return publicStatus(JSON.parse(body), now());
}

// At most one request at a time, five-second deadline, sixty-second cadence.
// Invalidate a previously successful result at its evidence cutoff, even if the
// next request hangs or the tab has been backgrounded. The caller owns cleanup.
export function watchPublicStatus(onChange, {fetchImpl = globalThis.fetch, now = Date.now} = {}) {
  let stopped = false;
  let controller;
  let timer;
  let expiry;
  const publish = summary => {
    clearTimeout(expiry);
    onChange(summary.state);
    if (summary.validUntil > now()) {
      expiry = setTimeout(() => onChange('stale'), summary.validUntil - now());
    }
  };
  const refresh = async () => {
    controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 5000);
    try {
      const summary = await fetchPublicStatus({fetchImpl, now, signal: controller.signal});
      if (!stopped) publish(summary);
    } catch {
      if (!stopped) publish(unknown());
    } finally {
      clearTimeout(timeout);
      if (!stopped) timer = setTimeout(refresh, 60000);
    }
  };
  void refresh();
  return () => {
    stopped = true;
    controller?.abort();
    clearTimeout(timer);
    clearTimeout(expiry);
  };
}
