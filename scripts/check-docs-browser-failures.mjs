import assert from 'node:assert/strict';

export function formatHttpFailure(response) {
  return `${response.status()} ${response.request().method()} ${response.url()}`;
}

export function formatRequestFailure(request) {
  return `${request.method()} ${request.url()}: ${request.failure()?.errorText ?? 'unknown failure'}`;
}

export function assertNoBrowserFailures(label, evidence) {
  assert.deepEqual(evidence.httpErrors, [], `${label} emitted HTTP errors (status, method, URL)`);
  assert.deepEqual(evidence.requestFailures, [], `${label} emitted request failures (method, URL, reason)`);
  assert.deepEqual(evidence.pageErrors, [], `${label} emitted page errors`);
  assert.deepEqual(evidence.consoleErrors, [], `${label} emitted console errors`);
}
