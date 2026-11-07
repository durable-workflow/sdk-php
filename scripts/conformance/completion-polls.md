# Installed completion-poll consumer

This is caller-owned source preparation for SDK #328–#332. It has not run
against a published package or Native image. Source controls do not qualify
Server admission, an installed upgrade, artifact publication or performance.

The caller separately selects and verifies the immutable published Server
index/container, normally installed SDK archive and sealed dependency graph,
and maintained runner files. Native 3.2.0 preparation is recorded in SDK
#293; no SDK version, image publication or invocation follows from this runner.
The template has unselected identities and refuses before consumer autoload.

The program does not install packages, fetch tools, start Server, upgrade a
database or change accounts/credentials. It requires an already-running
caller-owned disposable runtime at an explicit `http://127.0.0.1:port` address.
It creates uniquely named synthetic workflows and one SDK Worker, or six
Workers in an explicitly selected load cell, under the supplied namespace.
The public-API opposite-slot and workflow-refence rows use an owned registration
instead of a managed Worker. The caller must own that runtime and namespace and
include this effect in its separately selected execution packet. A loopback
address alone does not establish operating permission.

Invoke from a fresh process after the caller has bound the complete packet:

```sh
php /consumer/scripts/conformance/qualify-completion-polls.php \
  /consumer/vendor/autoload.php /packet/subject.json "$SUBJECT_SHA256"
```

## Held consumer root

The sealed runner files and the normally installed `vendor/` tree must share
one consumer root. The runner executes only from a container-private tmpfs
copy of it: a tmpfs mount at exactly the runner's root, with nothing mounted
beneath it, which no host file writer can reach
(`completion_admission_boundary`). The execution packet copies the read-only
`/consumer` bind into `/held` and runs `/held/scripts/...` as a uid that has no
host account. The autoloader argument must
be that root's `vendor/autoload.php`, so it is the same file that
`tests/Support/completion-worker-http.php` loads by its fixed relative path.
Before any consumer code loads, the runner admits the root and refuses:

- a root reached through a symlink, or a `vendor/` outside the runner's root,
  such as a source checkout (`completion_admission_root`);
- a missing, substituted or symlinked file among the 22 sealed runner files in
  `CompletionPollSubject::RUNNER_FILES` (`completion_admission_runner`);
- an installed graph (`vendor/composer/installed.json`) that differs from the
  lock in package names, versions or distributions, a development install,
  development packages in the lock, any path distribution, or an SDK installed
  outside `vendor/durable-workflow/sdk` (`completion_admission_graph`);
- an installed SDK package whose file inventory differs from the selected one
  (`completion_admission_package`), or any other vendor change, including a
  symlinked package (`completion_admission_vendor`);
- an expired authority, or one that extends more than
  `CompletionPollSubject::AUTHORITY_SECONDS` (two hours) into the future
  (`completion_admission_expired`);
- a writable root, `vendor/`, `vendor/composer` or SDK package directory
  (`completion_admission_writable`).

The runner then holds the device and inode of those four directories. It
re-checks them, their read-only state, the authority window, and the bytes of
the 22 runner files, the lock and every vendor file:
- immediately before the autoloader is required;
- when installed identity is read;
- before every router launch;
- before a result is reported.

The codes are `completion_admission_replaced`, `completion_admission_expired`
and `completion_admission_changed`. A launch may use only the three sealed routers
in `CompletionPollSubject::ROUTERS`, by their exact unaliased paths in the held
root, with the sealed concurrent server helper (`completion_admission_router`).

The four admission fields in the subject are:

| Field | Value |
| --- | --- |
| `runner_manifest_sha256` | SHA-256 of the canonical runner manifest: the 22 files as `{"bytes","path","sha256"}` objects sorted by path, compact JSON without escaped slashes, plus one newline. |
| `vendor_inventory_sha256` | The same canonical form over every regular file under `vendor/`, with paths relative to `vendor/`. |
| `sdk_inventory_sha256` | The same form over `vendor/durable-workflow/sdk/`, with package-relative paths. The caller compares it with the inventory of the selected registry ZIP. |
| `admission_expires_at` | Unix time at which this admission stops. |

These in-process checks bind the result to the admitted bytes. They cannot
prove the runner's own integrity, which rests on the private execution root
and on the caller verifying the runner manifest before it starts PHP.

The consumer project must require the exact selected SDK, have no repository
overrides or scripts, disable plugins and retain normal advisory policy. Its
lock must identify the normal registry ZIP distribution at the selected
reference. The runner checks the lock hash, installed Composer metadata,
SDK release metadata and reflected implementation paths. It refuses already
loaded consumer classes and proxy environment overrides. It does not replace
the caller's complete archive/file seal, registry/release evidence, protected
profile, canonical Composer verifier or execution admission.

The selected subject and lock are each read once from a bounded local regular
file descriptor. The same bytes are hashed and parsed, so a pathname replacement
cannot select a different document after digest verification. The subject is
limited to 16 KiB and the lock to 4 MiB. Successful identity output retains
the authenticated `subject_sha256` alongside its lock digest.

`COMPLETION_TEST_CLIENT_TOKEN` and `COMPLETION_TEST_WORKER_TOKEN` must contain
distinct caller-provisioned synthetic runtime roles. Credential headers and
outcome payloads never enter the trace. No credential file or vault is read.
The caller remains responsible for its selected bootstrap/profile, terminal
qualification and cleanup receipts; the accepted raw profile is not rerun.

The program uses the installed SDK's default native transport with metrics and
telemetry disabled. A two-worker loopback relay forwards the real runtime
responses. It records bounded task/attempt/owner/expiry metadata, carrier IDs,
callback order and hashes of actual request bytes. The relay implements no
Server endpoint or durable transition. An upstream error stays an error.

The basic scenarios are:

- `carriers`: three remote-activity workflows must exercise all three carried
  paths with inline lease answers and actual callbacks. Polling those same
  leases again does not qualify inline delivery.
- `pipeline`: five workflows, with a warm-up then five 20 ms local activities
  each, must prefetch before sending, perform useful local work while the
  parent answer is held, and join before the next completion. A second
  workflow reservation while that answer is pending fails.
- `pipeline-loss`: the same workload deliberately withholds one successful
  upstream answer as an unstructured 502. Retry must retain identical bytes
  and the original fence without rerunning the successful local work.
- `fallback`: an actual older producer must advertise no carried capability
  and receive no carrier request. This is distinct from pretending a current
  producer lacks support.

The controlled answer delay and loss are identified explicitly. They are not
an undelayed load comparison or a speedup measurement. The additional controls below, including undelayed load support, are connected
at Source scope. Every selected row needs its own actual
published-artifact result before the #328–#332 matrix is complete.

Each invocation has a selected 15–120 second product deadline, bounded HTTP
requests and a 15 second Worker shutdown grace within a 30 second cleanup
tail. Cleanup uses one-second requests directly to the selected loopback
runtime, bound to this invocation's workflow and selected run IDs. The Worker owns a separate
process group so maintenance helpers cannot survive forced cleanup. Workflow
termination on failure is limited to this invocation's unique handles. The
caller must budget the additional cleanup tail and remove its isolated
runtime/database independently; the runner never removes a caller's Server.
Failures and surviving registrations prevent a successful result. A violated
consumer assertion reports `product-fail`. Unclassified failures, unusable
observations, relay/process errors and incomplete cleanup report
`runner-blocked`. Neither outcome qualifies the scenario.

The output's image and runner references remain **caller-bound inputs**.
The program verifies endpoint version and installed package identity; it does
not attest a container digest or that a supplied runner reference names its
own copied files. The caller must supply the actual publication/index,
container inspection and runner-file binding alongside the result. The
program's `pass` covers only its declared installed-consumer scenario.

Keep the runner, `Support/CompletionPoll*.php`, the relay and both existing
process helpers in their relative paths: `tests/Support/CompletionPipelineScenario.php`,
`tests/Support/completion-pipeline-server.php`, `tests/Support/WorkerPipelineScenario.php`,
`tests/Support/completion-pipeline-http.php` and `tests/Support/RecordingMetricsRecorder.php`.
Repository PHPStan includes these new source files. The pure subject
and evidence unit tests use synthetic metadata only. Relay controls forward
fixed HTTP data to test byte/role forwarding, error preservation and one
successful-answer loss. They create no Server, package installation, profile
or published-product acceptance.

The remaining matrix uses the same accepted schema63 API. All controls below
have executable selections and remain unexecuted against an installed package
and Native runtime. Delay selection uses the
actual normal-poll `server_time` and original `lease_expires_at`. An absent
clock, unsuitable grant window or insufficient caller budget blocks that
control rather than substituting a worker clock or changing Server state.

| Control | Actual operation and required observation |
| --- | --- |
| `carrier-loss-activity`, `carrier-loss-workflow`, `carrier-loss-workflow-completion` | Withhold one actual successful completion answer. Recover its original poll ID and complete task/history identity. |
| `completion-transit` | Delay the real completion request before its carried claim. Require original-request charging and confirmation before callback when the reported reserve is insufficient. |
| `carrier-expiry` | Hold the real answer past its reported original expiry. Require fenced confirmation/refusal and no callback or publication under that expired generation. |
| `carrier-refence` | Re-register this invocation's same synthetic worker through the public worker API. Retain only token digests and require original activity-poll recovery to return the actual `poll_cached_task_expired` refusal; clean the owned replacement with its actual fenced receipt. |
| `workflow-refence` | Obtain a carried workflow task, replace the owned registration, check old operations before recovery, recover the original poll ID as a new attempt, and check old operations after recovery. Complete with the new authority. |
| `pipeline-buffer-age` | Delay a real registration-heartbeat reply after prefetch. Require workflow lease confirmation before the aged task executes. Registration heartbeat does not renew task authority. |
| `opposite-slots`, `capacity` | Hold actual opposite-kind leases through public polls/completions. Observe real carrier omission, then reservation release and later work without disabling the capability. |
| `prefetch-loss` | Withhold one real successful prefetch answer. Preserve the original ID, backoff and lease/history on recovery without another reservation. |
| `default-off`, `explicit-off`, `packet-defaults` | Exercise omitted and explicit-false options against the actual runtime. Keep the installed fixed-socket packet regression separate from native producer acceptance. |
| `load-local-off`, `load-local-on`, `load-remote-off`, `load-remote-on` | Direct runtime calls with no relay delay or per-request trace. Compare explicit OFF/ON cells for six workers, 200 workflows and five 20ms local or remote steps. Retain all three repetitions and regressions. |

The activity registration row and workflow registration row have distinct
contracts. The accepted [Server #1067 vector](https://github.com/durable-workflow/server/issues/1067#issuecomment-6703522814)
binds registration's atomic workflow-lease release. `workflow-refence` requires
`task_not_leased` before recovery and `workflow_task_attempt_mismatch` after
the same original poll ID freshly claims attempt n+1. History must remain
unchanged before that new attempt completes. No per-grant incarnation field
is invented. This is producer Source authority, not an installed result.

`packet-defaults` runs the installed SDK against the fixed HTTP fixture only
and explicitly reports `runtime_observed: false`. `opposite-slots` uses public
Client polls/completions rather than managed Worker capacity.

Load cells make direct runtime requests. They start no relay and record no
per-request trace. All six Workers register before measurement. Timing spans
the first sequential submission through the last verified result, including
control requests and polling. It is not a Server-internal last-close timing.
Each cell checks all 200 results and all 1,000 logical activity executions.
Worker-local counters are written once after shutdown. Remote workflow replay
may invoke the workflow handler repeatedly, while each logical step must
execute exactly once in this healthy, fault-free workload.

For a load scenario only, the sealed subject must additionally contain this
closed object, with the actual caller bindings in place of these examples:

```json
"load": {
  "backend": "sqlite",
  "database_identity": "fresh-isolated-cell-identity",
  "repetition": 1,
  "conditions_sha256": "<SHA-256 of the caller's host/resource/backend/durability conditions evidence>"
}
```

Backend is `sqlite`, `mysql` or `postgresql`; repetition is 1, 2 or 3. Use a
fresh isolated database and a unique subject/database identity for each cell.
The conditions evidence must bind actual host, architecture, CPU/RAM limits,
backend version and durability settings, plus the unchanged runtime options.
The runner does not create/reset databases or infer backend identity from a
label. Caller runtime/database teardown remains mandatory, including after a
cleanup failure. All Worker groups receive shutdown together under one global
15 second grace within the 30 second cleanup tail. An incomplete termination
sweep on a failed 200-workflow cell is explicitly blocked, never accepted.

Compare six individually hashed result files, three OFF and three ON, for one
backend and workload:

```sh
php scripts/conformance/compare-completion-poll-load.php \
  /results/off-1.json "$OFF_1_SHA256" /results/off-2.json "$OFF_2_SHA256" \
  /results/off-3.json "$OFF_3_SHA256" /results/on-1.json "$ON_1_SHA256" \
  /results/on-2.json "$ON_2_SHA256" /results/on-3.json "$ON_3_SHA256"
```

The comparator reads each bounded file once and binds the parsed bytes to its
hash. It rejects mixed tuple/workload/backend/conditions bindings, reused cell
identities, incomplete cleanup and missing/duplicate repetitions. It retains
all six times, both means, their ratio and whether ON was slower. It imposes
no performance pass threshold and reports `performance_acceptance: not_assessed`.
Publication, fresh-database and host/resource evidence remain caller-owned.
Descriptive timing alone is not a speedup or release acceptance.
