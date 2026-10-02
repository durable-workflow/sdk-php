# Runtime External Payload Restart

`runtime-external-payloads.php` is a manual integration test against an isolated
published Server and MySQL. It is not a benchmark or a production operation.
Install this checkout's Composer dependencies first.

Configure the disposable Server with `DW_AUTH_TOKEN=external-payload-fixture`,
`DW_RUNTIME_CREDENTIALS_ENABLED=true`, poll dispatch, a one-second worker poll
timeout, and a writable `/payloads` directory owned by its PHP user. Run
`server-bootstrap` and wait for `/api/ready` before starting this test. Preserve
MySQL data and `/payloads` when restarting. Do not point it at a shared runtime.

In a PHP environment with network access to that Server:

```sh
export RUNTIME_URL=http://localhost:8080
export RUNTIME_PROOF_DIRECTORY="$(mktemp -d)"
php tests/Integration/runtime-external-payloads.php prepare
```

`prepare` creates the test namespace and enables local external storage above
64 bytes. The authored value exceeds 2 MiB, so the encoded input, activity
arguments/result, workflow result and query result cannot pass through the
ordinary 2 MiB JSON request path. The SDK must upload them to the runtime.
In another terminal, with the same environment, start the worker:

```sh
php tests/Integration/runtime-external-payloads.php worker
```

Then create one completed workflow and one durably waiting workflow:

```sh
php tests/Integration/runtime-external-payloads.php start
php -d memory_limit=512M tests/Integration/runtime-external-payloads.php maximum
```

After it confirms the completed result and waiting history, stop the worker,
Server, and MySQL. Start MySQL and Server again, wait for readiness, and start a
new worker process with **unchanged source**. Keep the original `runs.json` in
`RUNTIME_PROOF_DIRECTORY`, then run:

```sh
php tests/Integration/runtime-external-payloads.php verify
php -d memory_limit=512M tests/Integration/runtime-external-payloads.php verify-maximum
php tests/Integration/runtime-external-payloads.php boundaries
```

`verify` checks the old completed result, queries committed activity history on
the waiting run, verifies that a large uploaded signal still obeys Server's
structural payload limit, then sends an above-inline-threshold digest signal
and checks the resumed result byte for byte. Transport offload must not bypass
operation-specific admission limits. `boundaries` verifies downloads with separate operator/worker credentials,
rejects those credentials in another namespace, and rejects even an
administrator's attempt to use the reference in the wrong namespace. It creates
only disposable fixture credentials, not provider credentials.

The worker uses only its fixture worker credential; the client uses only its
fixture operator credential. Run the worker with `php -d memory_limit=512M`
for the maximum-size case. This is SDK process memory, not a change to Server's
HTTP memory or request limits. `maximum` and `verify-maximum` require a published
Server with a working 64 MiB external-payload transport.

Record the exact Server image digest, SDK revision or installed package, PHP,
Avro, Guzzle and MySQL versions, outcome and any failed attempts on the owning
PR. Remove the disposable runtime, its volumes, and the proof directory when
finished. `RUNTIME_SDK_AUTOLOAD` can select a clean installed consumer's Composer
autoloader for post-release validation.
## Cooperative cancellation source qualification

The CI workflow accepts `cooperative_qualification=true` with an exact public
`server_commit` SHA. It builds that source in an isolated MySQL/Redis stack,
runs `CooperativeCancellationTest` plus the persisted memo restart case, retains
the JUnit results and raw scenario histories for 90 days and
removes the stack and images. The ordinary worker default remains protocol 1.19.

The connected cases cover waiting timers, cold worker replacement, duplicate
request identity/deadline, a discarded successful delivery reply, shielded local
cleanup, local user heartbeats and cancellation during a blocked callback.
The payload case enables local storage in a unique namespace on a task-owned
shared volume. A cooperative worker hydrates and completes a value above the
ordinary 2 MiB request limit, and the proof checks the runtime's stored result
reference, byte count, digest and decoded value. Teardown removes the volume.
The local cases use a 60-second callback, request cancellation while it is active
and require canonical cleanup in less than ten seconds. They verify that both
callback and relay processes stop and that no late return marker appears.

With an exact `native_commit` overlay, the prepared group cases require explicit
`prepared_local_activity_groups` discovery. They admit two local members before
either callback starts, cancel both blocked callbacks without application
heartbeats and run a two-member shielded cleanup group. The cold case sends
SIGKILL to the owning worker during cleanup and verifies every callback and
relay stops. A replacement must retain the original canonical delivery, recover
both unresolved attempts and finish as Cancelled before the original 30-second
deadline. Raw histories include the request, attempts, joined stops and recovery
events. This source qualification does not replace the published mixed-language
cancellation cascade gate.

The optional `python_commit` and `rust_commit` inputs must both be exact public
Source SHAs and require the Native overlay. They build an actual Python child
worker and Rust activity consumer alongside the PHP worker. The mixed case
starts the child and a prepared PHP local activity together, waits for both
callbacks to enter, then requests cancellation with a 30-second budget. It
checks root lineage, genuine child cooperation, callback stops without app
heartbeats, stop receipts and stale Rust publication. It sends SIGKILL to the
PHP worker during shielded cleanup and starts a fresh worker, which must retain
the committed group boundary and finish both runs as Cancelled before the
original deadline. Duplicate cancellation cannot change that identity or budget.
The case also checks Server's shared cascade during cleanup and after recovery,
from both selected runs. The view must retain the original budget, both stop
receipts, the same delivery boundary, and the original-to-replacement cleanup
grant with its recorded unknown callback state.

The optional `cli_commit` input requires both language candidates and the Native
overlay. It runs the exact CLI against the final real cascade in JSON and human
formats. JSON must preserve Server's view, and the human output must show the
root, both runs, stop receipts, cleanup outcome and worker recovery.

The Rust consumer's published dependency is replaced by the exact Source
checkout in this qualification. Provenance retains each selected source commit, the actual
consumer lockfile, Python package versions, histories and worker observations.
Source results support the model's development. The final published-artifact
cascade and the Waterline inspection view remain separate required gates.

Explicit cooperative workers require Unix CLI, `pcntl`, `posix` and a transport
that supports bounded requests, downloads and uploads. The default transport
requires Guzzle with cURL for this opt-in mode. A worker control request shares
one five-second monotonic budget across discovery, payload uploads, the API
reply and downloads. Polls allow their requested wait plus five seconds, then
give task hydration one five-second budget shared by all references. Complete
transfers use finite temporary sinks, so a trickling body cannot restart a
per-read timeout or grow without a byte bound. The owning
worker alone renews the real task lease, observes its original request and
deadline, records user heartbeats and permits result encoding. Activity callbacks
run in a child process. Open handler-owned database or network connections in
that process. Changes to captured memory do not update the owning worker.
Parent connections and inherited shutdown hooks must not be used for callback
cleanup. Normal workers keep their existing execution model.

Stopping a callback does not roll back an external effect. Activities remain
subject to at-least-once execution and must make retries safe. Remote activity
lifetime is observed through the candidate1.20 activity status endpoint, with
no automatic user progress or activity lease renewal. The existing five-minute
lease requires authored heartbeats for longer attempts. Failed observations
abandon the attempt without writing an application failure. Connected remote
cases use separate workflow and activity workers, so workflow capacity remains
available to record canonical cancellation while a callback blocks. They cover
user/no-user heartbeats, cold workflow replacement, SIGTERM/SIGKILL and late
publication fencing. Active remote-attempt reclaim and exact published-tuple
qualification remain required before release.
