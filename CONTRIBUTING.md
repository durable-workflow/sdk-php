# Contributing

Run Composer validation, focused PHPUnit coverage, PHPStan, and the public
boundary check for changed code. Run `composer check-http-only-platform` for
HTTP-client dependency changes; it resolves PHP8.1/Guzzle7/PSR-message1 with
cURL and OpenSSL disabled and exercises a real HTTP call without TLS functions.

The production graph remains independent of Laravel, Illuminate, framework
bundles/containers, Core and Server. The owned native Guzzle handler delegates
HTTP serialization, framing and total socket deadlines to HTTP_Request2. Its
OpenSSL dependency is optional, preserving HTTP-only installations. Thin adapters
share one deadline across CONNECT/TLS and reject incomplete-transfer warnings.
The reviewed stable floors and neutral Symfony contracts/polyfills are listed in
`resources/dependency-boundary.json`. The boundary check covers declared and
transitive production packages. Change that closed support list or dependency
floor with an owning product decision and compatibility/security tests.

Before publishing a tag, run `RELEASE_TAG=<exact-version> composer check-release-identity`.
The installed Composer version, `extra.durable-workflow.product-train`, quickstart
package versions and worker registration must identify that same PHP SDK release.
Protocol versions and the supported Server baseline are separate compatibility
declarations. Align release metadata before tagging a stable patch, even when
development previously reserved a future prerelease.

Replay and payload-codec fixes also follow the organization
[regression-corpus contract](https://github.com/durable-workflow/.github/tree/main/regression-corpus).
A replay fix adds one minimal history or command-sequence JSON fixture under
`tests/fixtures/replay-regressions/`. A codec fix adds the shared wire fixture
under `tests/fixtures/codec-regressions/` and to every other applicable
official binding.

Fixtures preserve the value and type, framing, and stable failure policy.
Existing evidence is append-only; protocol evolution adds a new fixture with a
`supersedes` identity. Every new replay fixture added with a replay fix is
executed through the official PHP replayer at both revisions: it must fail on
the target revision and pass with the candidate fix. Install Composer
dependencies, then run:

```bash
python scripts/ci/validate-regression-corpus.py --base-ref <target>
```

## Managed worker presence against published artifacts

Start an isolated native Server from the published image digest with
`DW_WORKER_HEARTBEAT_INTERVAL_SECONDS=2` and `DW_WORKER_STALE_AFTER_SECONDS=9`.
Use a fresh database and a disposable token. Set `DURABLE_WORKFLOW_SERVER_URL`
and `DURABLE_WORKFLOW_AUTH_TOKEN` for that Server, and set `SERVER_VERSION`
to its published version.

Install the selected stable SDK with Composer in a fresh consumer directory.
Set `SDK_VERSION` and `SDK_REFERENCE` to the exact installed version and
source commit from the lock. Run with a PHP CLI that has `pcntl` and
`proc_open`. The PHP process needs an unused loopback TCP port 1 for one
deliberately refused connection.

```sh
php tests/smoke/managed-worker-heartbeat.php /path/to/consumer/vendor/autoload.php
```

The runner checks real acknowledgements from the managed `Worker::run()`
loop. It verifies retry on the same registration, graceful deregistration,
staleness after process loss and completion by a replacement worker. A pass
prints a JSON receipt with the exact SDK reference and Server version.
The runner reaps its local workers and temporary logs. Retire the disposable
Server and database when finished.
