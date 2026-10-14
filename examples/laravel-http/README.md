# Laravel service HTTP example

This candidate application compares nginx/PHP-FPM with optional Laravel Octane
and RoadRunner. Qualification is in progress on [PHP SDK #112](https://github.com/durable-workflow/sdk-php/issues/112).
No performance or production hosting recommendation is established yet.

The application uses the normally published SDK. Durable workflow and activity
polling runs in the separate `durable-workflow:worker` CLI container. HTTP
workers only submit and inspect runtime operations. Server or Cloud owns durable
history, timers, task leases and recovery.

## Installation

The exact PHP SDK, Laravel and Octane versions live in `composer.json` and the
resolved dependencies in `composer.lock`. The image uses PHP 8.3.35. Preparation
downloads RoadRunner 2025.1.15 through its standard installer, then runs
`octane:install` and `config:cache`. Octane remains an application dependency.

Use a disposable self-hosted Server namespace with separate application-client
and worker role credentials. Put the following Compose values in a private file
outside this application directory. Do not put runtime credentials in Laravel
configuration or commit them.

```text
APP_KEY=<base64 application key>
DURABLE_WORKFLOW_RUNTIME_URL=<Server origin or complete Cloud runtime URL>
DURABLE_WORKFLOW_NAMESPACE=<one fixed namespace>
DURABLE_WORKFLOW_CLIENT_TOKEN=<application client-role credential>
DURABLE_WORKFLOW_WORKER_TOKEN=<worker-role credential>
EXAMPLE_STATE_DIRECTORY=<absolute writable local state directory>
```

Create the state directory as your local UID, then run:

```bash
docker compose --env-file /private/example.env up --build -d
```

The HTTP container receives only the client token. The CLI container receives
only the worker token. The preparation container receives neither. Change the
runtime address, namespace or credential by replacing the relevant process.
Never rewrite a shared client using request headers or an authenticated user's
token. Multiple runtime authorities need independently configured clients or
separate application processes.

For the FPM baseline:

```bash
docker compose --env-file /private/example.env -f compose.yaml -f compose.fpm.yaml up --build -d
```

Both modes use two HTTP workers, one durable CLI worker and nginx with the same
OPcache and cached configuration. The application allocation totals two CPUs
and 1 GiB with no swap: HTTP 0.9 CPU/640 MiB, CLI 1 CPU/320 MiB, nginx
0.1 CPU/64 MiB. Provision and report the separate Server/database allocation.
These limits are qualification settings, not a general capacity promise.

## Exercise the application

The two `example-alpha` and `example-beta` bearer values are synthetic application
identities. Replace this middleware with your application's authorization before
exposing the example. It binds workflow IDs to the current user and refuses a
different `X-Namespace` before calling the SDK.

```bash
curl -f http://localhost:8088/api/workflows \
  -H 'Authorization: Bearer example-alpha' \
  -H 'X-Request-Id: start-1' -H 'Content-Type: application/json' \
  -d '{"id":"first","kind":"short"}'
curl -f http://localhost:8088/api/workflows/first \
  -H 'Authorization: Bearer example-alpha' -H 'X-Request-Id: read-1'
```

Kinds `short`, `timer`, `signal` and `cancel` share one echo activity. A timer
waits one second. A signal workflow waits for `POST /api/workflows/{id}/signal`.
Query it with `POST /api/workflows/{id}/query`. Cancel a waiting original with
`POST /api/workflows/{id}/cancel`. Status reads return canonical status and the
typed result, including an integer above JavaScript's exact-number range, null
and Unicode. Decode that integer with an appropriate client.

Every API request requires a fresh `X-Request-Id`. Alternating requests omit or
set `X-Optional-Context` and inspect response headers plus application logs.
Request context is removed in the middleware's `finally` block. Set
`EXAMPLE_EAGER=true` before starting a fresh application to resolve the SDK at
boot; the default resolves it lazily. Both processes keep a fixed authority.

`qualification/context.mjs` checks 40 alternating requests and three auth,
namespace and invalid-header refusals through `/api/context`. That diagnostic
route resolves the normal SDK client without sending a runtime request. It
checks HTTP request isolation only; it cannot prove workflow execution or
runtime namespace authorization.

With the real runtime and CLI worker ready, choose a fresh original prefix and
run `qualification/lifecycle.mjs` with `EXAMPLE_ORIGIN` and `EXAMPLE_RUN_PREFIX`.
It admits five originals once, observes four completions and one cancellation,
and checks signal/query and a foreign-user refusal. Status observation budgets
are 35 seconds; individual HTTP calls are capped at the remaining budget.
`qualification/verify-results.php` reads those same original results through the
installed PHP SDK and checks the exact int64/null/Unicode types. Run the drivers
in a separate tool container so their resources are excluded from the fixed
application allocation.

The durable worker explicitly uses Docker's SIGTERM stop signal. The FPM image's
default SIGQUIT belongs to the HTTP process; use SIGTERM for the CLI worker so
its normal SDK shutdown can deregister.

The separate `recovery-timer` kind waits ten seconds after its echo activity.
It provides a checkpoint before testing CLI replacement. Use the returned
original workflow/run IDs with `qualification/recovery-checkpoint.php before`,
writing a temporary file under the private state directory. The check requires
one completed activity and one unfired persisted timer. An interruption control
must preserve the same original IDs and accepted 600/900-second deadlines, use
the declared observation budget, and never seed a replacement workflow or
manually repair history. After the replacement completes, `after` requires the
whole original prefix, unchanged deadline fields and exact typed result. Delete
the temporary checkpoint when the control ends. This helper's presence is not
proof that SIGKILL or connection-loss recovery has been qualified.

History-cache defaults remain 100 entries/300 seconds. The existing SDK validates
capacity and TTL, with a 3600-second TTL ceiling. `EXAMPLE_HISTORY_CAPACITY` and
`EXAMPLE_HISTORY_TTL` select this process's settings before startup. A cache hit
retains history and still replays instructions. HTTP reuse does not retain a
workflow Fiber or avoid execution replay.

## Qualification before recommendation

Freeze the normal dependency lock, RoadRunner binary hash, runtime image digest,
role configuration, offered schedule, original deadlines and resource limits
before admitting workflows. Qualify eager/lazy resolution, alternating users and
namespace refusals, headers/log context, signal/query/cancel, cache expiry and
eviction, HTTP reload/recycling, CLI SIGKILL/cold replacement and runtime
connection loss first. Inspect canonical histories for duplicate effects and
unchanged identities/deadlines.

Then run three independent FPM/Octane pairs and recheck FPM. Keep CLI execution
settings identical. Report offered/completed/failed/unfinished operations,
submission/status latency separately from workflow latency, measured drain,
timer due-to-resume latency, per-completion CPU, aggregate steady/peak memory,
history bytes/requests and existing cache diagnostics. Reject OOM, swapping or
growing backlog as a successful measurement. Follow with a bounded mixed-request
soak and explicit recycle/reload observations. Gains and tradeoffs belong in the
owning issue, including a result with no consistent benefit.

Stop the Compose project and delete only this example's state when finished.
This application does not provision or retire your runtime namespace or its
credentials. Retire those through your Server or Cloud's normal admin path.
