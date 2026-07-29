# Contributing

Run Composer validation, focused PHPUnit coverage, PHPStan, and the public
boundary check for changed code.

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
