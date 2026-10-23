# Contributing

Run Composer validation, focused PHPUnit coverage, PHPStan, and the public
boundary check for changed code.

The production graph remains independent of Laravel, Illuminate, framework
bundles/containers, Core and Server. The reviewed exception in
`resources/dependency-boundary.json` permits the standalone HTTP-client component
and its contract/polyfill support for native control-request deadlines. It does
not load a Symfony application or container. The check covers declared and
transitive production packages and retains the reviewed stable HTTP-client
security floors. Expand that closed exception only with an owning product
decision and dependency/compatibility tests.

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
