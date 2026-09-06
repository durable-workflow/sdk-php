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
1,024 bytes. In another terminal, with the same environment, start the worker:

```sh
php tests/Integration/runtime-external-payloads.php worker
```

Then create one completed workflow and one durably waiting workflow:

```sh
php tests/Integration/runtime-external-payloads.php start
```

After it confirms the completed result and waiting history, stop the worker,
Server, and MySQL. Start MySQL and Server again, wait for readiness, and start a
new worker process with **unchanged source**. Keep the original `runs.json` in
`RUNTIME_PROOF_DIRECTORY`, then run:

```sh
php tests/Integration/runtime-external-payloads.php verify
php tests/Integration/runtime-external-payloads.php boundaries
```

`verify` checks the old completed result, queries committed activity history on
the waiting run, sends a large signal, and checks the resumed result byte for
byte. `boundaries` verifies downloads with separate operator/worker credentials,
rejects those credentials in another namespace, and rejects even an
administrator's attempt to use the reference in the wrong namespace. It creates
only disposable fixture credentials, not provider credentials.

Record the exact Server image digest, SDK revision or installed package, PHP,
Avro, Guzzle and MySQL versions, outcome and any failed attempts on the owning
PR. Remove the disposable runtime, its volumes, and the proof directory when
finished. `RUNTIME_SDK_AUTOLOAD` can select a clean installed consumer's Composer
autoloader for post-release validation.
