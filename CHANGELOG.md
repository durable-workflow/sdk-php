# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed

- An opted-in worker no longer waits a full poll timeout between steps after
  an accepted activity continuation (server#1129). The accepted commands can
  already have scheduled the run's next activity, so the next tick now sweeps
  without holding a workflow poll first. Before, each continued step waited
  about 5 seconds (the default `run()` poll timeout).

## [3.12.0] - Unreleased

### Added

- The Worker constructor accepts `enableActivityContinuation`, defaulting to
  `false` (#341). On a runtime advertising `activity_workflow_continuation`, an
  opted-in worker can replay bounded history with its successful activity result
  and submit eligible workflow commands with the same completion. The native
  producer commits the result and commands atomically. Missing or older
  capability, unsupported replay state and business-clock reads at the
  speculative result retain ordinary completion and polling. A Server decline
  also retains the ordinary path. Backend errors and uncertain completion
  outcomes remain errors and use the existing completion recovery rules.
- Continuations initially support sequential remote activities, timers, workflow
  completion and version markers. Local activities, parallel groups, selections,
  cancellation scopes, message waits/cursors and update/cancellation state stay
  outside this opt-in. Drain or disable opted-in workers before introducing
  older serving nodes. See the [worker guide](docs/portal/build/workers.md#activity-continuations).
  No throughput or latency improvement is claimed.

## [3.11.0] - 2026-11-08

### Upgrade notes

- Do not let older and 3.11 workers consume the same task queue on a Server
  that advertises `condition_wait_occurrence_deadline` (native Server 3.3.0 or
  newer). On an older Server, upgrade every worker on the queue to 3.11 first,
  then upgrade the Server. On a Server that already advertises the capability,
  drain or stop every older worker for the queue, including standby and
  autoscaling pools, before any 3.11 worker starts consuming it, then start
  only 3.11 workers. After any anchored chain exists, do not roll that queue's
  workers below 3.11: an older worker's reopen of an anchored chain is refused
  by the Server, nothing is written, and that worker can exit; a 3.11 worker
  can resume the run. See the [upgrade guide](docs/portal/operate/upgrading.md).
- Without the capability, new and legacy chains keep sending legacy commands.
  If the Server stops advertising the capability while an existing anchored
  chain needs a reopen, the 3.11 worker leaves that task's claim without
  completing or failing it, and a 3.11 worker can resume the run once a capable
  Server is back. No retry interval is promised, an in-place SQLite rollback of
  the Server is not supported, and a chain already mixed by an older Server is
  not repaired automatically.
- The bare SDK keeps PHP `^8.1`; the Laravel bridge constraints, the worker
  protocol (1.20) and control plane (2) are unchanged.

### Added

- Condition waits can keep their first open's wall-clock deadline across false
  wakes (#269). When the Server advertises `condition_wait_occurrence_deadline`,
  a new top-level `waitCondition()` chain opts in: each physical open of that
  authored call sends `deadline_policy: "occurrence"` and
  `condition_wait_timeout_budget_seconds` (the original timeout), while
  `timeout_seconds` stays the remaining replay budget. The Server then keeps the
  first open's deadline instead of restarting the timer at each reopen. A
  recorded chain keeps its first open's policy, so a chain started without it
  never gains it. Replay refuses an unknown policy, a missing or mismatched
  budget, or a chain whose physical opens disagree. Grouped condition waits do
  not opt in yet (#278). Without the capability, legacy and new chains send
  unchanged commands. An already anchored chain reopens only while the current
  Server advertises the capability: otherwise, for example after a rollback to
  a Server that would record a legacy reopen inside the anchored chain, the
  worker leaves the task claim with `condition_wait_occurrence_deadline_unavailable`
  before sending the reopen, without completing or failing the task: the claim
  stays until its lease or another ordinary authority transition ends it, no
  retry task is created, and a capable Server resumes the run.

## [3.10.0] - 2026-11-07

### Upgrade notes

- The Laravel service-mode bridge now follows published Workflow 2.21.0:
  PHP `^8.2`, patched Laravel `^12.69.0|^13.30.0`, and the five supported
  framework/PHP qualification cells. SDK 3.10 removes Laravel 10 and 11 support;
  existing SDK 3.9.x and Workflow 2.20.x applications retain their contracts
  while upgrading their platform. Stable-v1 authority is unchanged (#239).
  Provider auto-discovery now refuses unsupported or unknown framework/PHP
  versions before registering bridge configuration, clients or workers.
- The bare HTTP SDK retains PHP `^8.1`; Symfony 6.4/7/8 support is unchanged.
  Upgrade the complete task queue before recording v2 condition fingerprints;
  legacy fingerprints still require their exact source-line match. Read the
  [upgrade guide](docs/portal/operate/upgrading.md) before deployment, including
  its rollback boundary and atomic-source/OPcache restart requirements.
- Static credential validation deliberately rejects malformed UTF-8, edge
  Unicode whitespace and embedded controls before fallback or role selection.
  Valid credential bytes remain exact. The [authentication guide](docs/portal/operate/authentication.md#static-credential-validation)
  covers application, framework and process-handoff settings.

### Added

- Completion-carried next-task polls use the runtime's matching advertised
  capability and a free Worker slot (#328, #329, #330). Workflow completions
  can request an activity or workflow task; activity completions can request
  a workflow task. An omitted `workflow_poll` delivers no lease and does not
  mean the queue is empty. Runtimes without support keep ordinary polling.
- A carried lease delivered in a completion answer enters the ordinary payload,
  history and lease-supervision path without another poll (#331). Lost answers
  retain the original poll request ID for recovery. Original completion transit
  and buffered lease age consume the grant's remaining margin; lease owner,
  attempt and fencing authority are preserved.
- `Worker::create()` and the Worker constructor accept the explicit
  `enableCompletionPipelining` option, defaulting to `false` (#332). Managed
  `run()` can overlap a completion response after successful local activity
  work with one already-leased workflow task. It joins the original response
  before publishing that task's completion or failure. Public `tick()`,
  activity scheduling, cancellation, local failures, cooperative mode, custom
  transports, telemetry and metrics retain synchronous behavior. Opting in
  changes when completion errors become visible. No throughput improvement or
  default change is claimed.

- A schedule-to-start histogram for workflow/activity dispatch uses
  the runtime's capability-gated `ready_age_ms` claim metadata (shared metrics
  revision 2, central #233). Valid zero is retained; missing, unsupported or
  malformed ages are omitted. Existing instruments keep their names and tags.

- `ActivityContext::activityExecutionId` exposes the received logical activity
  execution ID for downstream idempotency keys. Missing or invalid legacy
  metadata stays null; task and attempt IDs are never used as substitutes.
  Prepared local callbacks receive their validated Server execution identity;
  inline local and Server-free callbacks without one report null
  (durable-workflow/.github#211).

- `Client::iterateWorkflows()` and `iterateWorkflowHistory()` lazily read
  complete workflow listings and exact-run histories, retaining one page at a
  time. Existing one-page methods stay available. Iteration preserves opaque
  cursors, namespace and filters, propagates later-page errors, and refuses
  malformed continuations or token cycles instead of silently truncating or
  looping (#247 in durable-workflow/.github).

- `WorkflowContext::upsertSearchAttributes()` accepts an optional complete map
  of canonical type declarations (#244). Explicitly typed calls bind replay to
  both values and recorded type identity. Existing one-argument calls retain
  value-only replay, and histories without type metadata retain legacy
  compatibility. Malformed recorded type identities are refused.

- `WorkflowContext::startChildWorkflow()` starts a child and returns a
  `ChildWorkflowHandle` once the runtime has committed the start (#185). The
  handle's `identity()` gives the child call ID, the child workflow ID and the
  initial run ID from canonical history while the child keeps running, and
  `await()` returns its result or throws `ChildWorkflowFailed`, following the
  same call through child continue-as-new and retries. The start sends
  `start_child_workflow` with `await: "started"` and needs a runtime that
  advertises `child_workflow_start_turn`; without it the worker abandons the
  task before submitting anything. `childWorkflow()`, `deferChildWorkflow()`
  and group APIs are unchanged, and `childWorkflow()` refuses an `await`
  option.

### Changed

- New condition waits use versioned `dwphp2:sha256:` predicate fingerprints
  (#293). Callable tokens exclude whitespace, comments and neighboring call-site
  arguments, while preserving literals and operator boundaries. A predicate
  change still fails replay. Indistinguishable same-line closures share a
  combined fingerprint; put predicates on separate lines before starting runs
  when they need independent identities. Unsupported source shapes or a PHP
  build without the tokenizer retain the existing v1 computation.
  Replaying v2 waits requires the tokenizer in every worker.
- Replay accepts a recorded v1 fingerprint only when its original source-line
  hash matches exactly. Physical reopens preserve their first recorded
  fingerprint, including its absence. V2 formatting tolerance therefore applies
  to waits first opened with v2, not retroactively to legacy runs.
- Upgrade the queue's workers together before opening v2 waits. Older workers
  reject v2 fingerprints, so rolling back below this SDK after v2 waits exist
  can fail replay. Deploy sources atomically and restart workers, including
  workers using OPcache.

### Fixed

- Composer distribution archives no longer include the test tree, avoiding the
  `tests/Fixtures` and `tests/fixtures` directory collision when vendor files
  are copied to a case-insensitive filesystem. Source checkouts keep the
  complete test corpus; runtime code, `resources/`, `docs/` and `examples/`
  remain in the archive.

- Static shared, control and worker credentials are validated in their original
  form before role selection and fallback, including framework configuration
  and Laravel process handoffs (#307). Invalid UTF-8, leading/trailing Unicode
  White_Space and embedded C0/DEL/C1 controls now fail with a setting-only error
  instead of being trimmed or silently ignored. This deliberately rejects
  malformed UTF-8 bytes previously accepted by PHP. Valid opaque spelling and
  literal-empty optional fallback remain unchanged; dynamic authentication is
  outside this static-token policy.

- A legacy sequential condition wait can recover its own recorded opening and
  deadline after the previous authored call's result is consumed (#269).
  Repeated physical opens still share their first deadline. Missing or reused
  wait identities and an ambiguous consumed outcome produce an explicit replay
  conflict instead of assigning a guessed deadline to the next call.

## [3.9.5] - 2026-11-07

### Fixed

- Reuse a Worker's activity lease helper across sequential activities after a
  confirmed finish, instead of starting a process and rebuilding its Client for
  each callback (#326). Every claim retains its own lease, business deadline and
  heartbeat authority. Dead or unconfirmed helpers are replaced, forked callbacks
  cannot use their parent's helper, and shutdown closes the idle process.
  Poll transit consumes the fresh activity grant's margin; slow, unknown or short
  grants require confirmation before another callback starts.
- Skip the initial workflow-task renewal for a fresh, fully fenced poll grant
  with a valid Server clock and sufficient remaining lease (#327). Request time,
  payload loading and history paging consume that margin. Servers without a
  qualifying clock and stale or short grants retain the existing renewal path.
- Update the optional Laravel HTTP example's Symfony HTTP Client, HttpFoundation,
  HttpKernel, Routing and YAML lock entries to their accepted security patches.

## [3.9.4] - 2026-11-04

### Fixed

- Replay checks a recorded `condition_wait_occurrence_id` against the call
  being replayed (#281). If a history records this SDK's id for a different
  `waitCondition()` or `deferCondition()` call (`php:condition-wait:N` with
  another N), replay fails with `condition_wait_occurrence_mismatch` instead of
  resuming the wrong call. Two physical opens recorded for different
  occurrences are never merged into one wait. A logical wait keeps the id of
  its first open, so a run whose first open was written before 3.9.3 (with no
  id) keeps replaying as before. Ids in any other format are not compared.
  - **Code changes:** ids are ordinals of the condition calls in a run, so
    inserting or removing a `waitCondition()` call before an existing one in
    code that already has runs in flight now fails replay for runs recorded
    with ids, as other non-deterministic changes do. Use version markers for
    such changes.
- Prepared local activity checkpoint ids, cancellation-scope descriptor
  hashes and the workflow definition fingerprint no longer depend on the host's
  `serialize_precision` (#282). They hash JSON that can contain floats, such as
  search attribute values in a checkpointed command, and now use the same
  round-trip precision as request bodies (#248). Under the default setting the
  hashes are byte-identical to before.

## [3.9.3] - 2026-11-04

**Upgrade from 3.9.2 now.** 3.9.2 can fail existing runs on native Server 3.0.x
that contain a repeated condition-wait open (#283). Users of 3.9.1 should
upgrade too: 3.9.3 keeps 3.9.2's fix that stops unrelated signals from
restarting a `waitCondition()` timeout (#269).

### Added

- `open_condition_wait` commands carry `condition_wait_occurrence_id`
  (protocol 1.17), `php:condition-wait:N` for the Nth `waitCondition()` or
  `deferCondition()` call in the run (#269). Every replay and every physical
  reopen of one call sends the same id, so the Server can recognize a repeated
  open of a wait that is still open instead of recording a second one.

### Fixed

- 3.9.2 workers could fail runs that had a repeated condition-wait open once
  the orphaned first open's timer fired (#283). The Server records a second
  physical wait when a queued workflow task re-sends a still-open wait
  (#269 part 2), and the first wait's timer still fires later. 3.9.2 treated
  that timeout as finishing the first wait, split it from the rest of the
  chain, and replay then failed with a non-determinism error. Whether a later
  open continues an earlier same-key wait now depends on the earlier wait's
  state when the later one opened: still open or satisfied means the same
  wait, and a timeout before the later open still makes a separate call, such
  as a grace period. When the earlier wait's deadline has already passed, the
  worker re-sends the wait with `timeout_seconds: 0`, so the Server times it
  out at once and `waitCondition()` returns `false`.
  - **Known limitation:** a repeated open that the Server records after the
    earlier wait has already timed out is still refused by replay. Server
    3.1.1 is planned to stop recording repeated opens of a still-open wait
    (server#1015), which removes that case.
- Request bodies keep full float precision whatever the host's
  `serialize_precision` is (#248). With a non-default value such as 3, a float
  search attribute of 1.2345 was sent and recorded as 1.23, and replay then
  refused the history as `search_attribute_value_mismatch`. Request documents
  are now encoded with round-trip precision, and the host setting is restored
  after each encode.

## [3.9.2] - 2026-11-04

### Changed

- `WorkflowHandleInterface` documents the `workflowId` and `selectedRunId`
  read-only properties that the SDK's handles and the test fake's handle
  already expose (#251), and the Laravel guide shows reading a generated
  workflow ID from the handle `start()` returns. The interface gains no
  methods, so application classes that implement it keep loading.

### Fixed

- An empty workflow ID now fails when the handle is created (#252). Before,
  `workflowHandle('')`, and the Laravel bridge's `handle(Workflow::class, '')`,
  returned a handle whose calls built routes such as `GET /api/workflows/`,
  and Server 3 answered `rust_capability_not_implemented`, which read like a
  missing server feature. `WorkflowHandle` and the test fake's handle now throw
  `InvalidArgumentException` for an empty workflow ID or an empty selected run
  ID (pass `null` to follow the current run). Every `Client` call also refuses
  an empty identifier in its request path before sending anything.
- A `waitCondition()` timeout now runs from the first time the wait opens
  (#269). The Server wakes an open condition wait on any signal and records it
  satisfied; when the predicate was still false the worker reopened the wait
  with the full `timeout_seconds`, so each unrelated signal moved the deadline
  forward (a 5-minute wait timed out after 422 s in one run). The worker now
  reopens with the time remaining since the first `ConditionWaitOpened`,
  rounded up to whole seconds and never below zero, matching embedded
  `await($condition, $timeout)`. Histories whose reopens carry the full timeout
  still replay. This needs no Server change. A wait that timed out is never
  treated as reopened: a later wait with the same key and predicate, such as a
  grace period, is a separate call measured from its own open.
  - **Mixed fleets and rollback:** a history in which this release reopened a
    wait with a reduced timeout cannot be replayed by workers on 3.9.1 or
    earlier; they fail with "reached complete_workflow before consuming its
    recorded durable history". Upgrade every worker on a task queue together,
    and do not roll a queue back below this release once such a reopen exists.
  - **Equal-timeout waits after a timeout:** 3.9.1 and earlier merged a wait
    into the previous one whenever both had the same key, predicate and
    timeout, even when the first had timed out. A run that waits again with
    the same key, predicate and timeout after a timeout (for example a retry
    loop) now replays the two waits separately, as they were recorded. A run
    recorded by an older worker under the merged reading can fail replay with
    a non-determinism error. Give each logical wait its own key. Waits inside
    `all()`, `parallel()` and selections are never merged with each other.

## [3.9.1] - 2026-11-04

### Fixed

- Friendly durations whose calendar total wraps inside Carbon are now rejected
  (#257). In 3.9.0, a 12–15 digit part such as `'635428518853 years'` or
  `'213503982334602 days'` overflowed Carbon's integer arithmetic. It was
  accepted as a small positive duration (581632 s and 61440 s). The SDK now
  throws `InvalidArgumentException` ("it is too large") when any part, or the
  sum of the parts, reaches 2^53 seconds, measured as a float with Carbon's
  default factors. `DateInterval` and `CarbonInterval` fields get the same bound.
  Every value 3.9.0 accepted below that bound converts to the same seconds.
- Correction to the 3.9.0 notes: numeric strings such as `'30'` keep their
  earlier conversion only in files without `strict_types`. With
  `declare(strict_types=1)`, `sleep('30')` used to throw a `TypeError` and is
  now accepted, which is an intended widening.

## [3.9.0] - 2026-11-04

### Added

- Friendly durations for workflow timers and timeouts (#254).
  - **Where:** `WorkflowContext::sleep()`, `deferTimer()`, and the `timeout` of
    `waitCondition()` and `deferCondition()`. Also the remote and local
    activity timeout options, the child workflow `execution_timeout_seconds`
    and `run_timeout_seconds` options, and the service operation
    `wait_timeout_seconds` option.
  - **Forms:** besides int or float seconds, a string such as
    `'30 minutes'`, `'1h 30m'` or `'1.5 hours'`, or a `DateInterval` or
    `CarbonInterval`.
  - **Conversion:** each value converts once to whole seconds, rounding up as
    before. `'30 minutes'`, `1800`, `new DateInterval('PT30M')` and
    `CarbonInterval::minutes(30)` record identical commands and history.
    - Strings convert exactly as the embedded `durable-workflow/workflow`
      `timer()` and `await()` do:
      `ceil(CarbonInterval::fromString($string)->totalSeconds)`.
    - Intervals are measured by their fields with
      `CarbonInterval::instance()->totalSeconds`. A Carbon 3 `diff()`
      interval therefore uses its fields (a 4-week month), not its real date
      span.
    - Carbon's default cascade factors make the result independent of the
      current date and time zone.
    - **Correction (#265):** the original notes also claimed independence
      from PHP and Carbon major versions. Very large durations containing
      millisecond or microsecond parts retain the installed Carbon major's
      floating-point behavior. For example,
      `'5555555555555 seconds 32037 milliseconds'` converts to
      `5555555555588` seconds on Carbon 2 and `5555555555556` on Carbon 3.
      SDK/embedded parity on each major is preserved. The regression sample
      points do not establish a universal precision bound, and no conversion
      behavior changes with this documentation correction.
  - **Stricter than the embedded package.** These throw
    `InvalidArgumentException` naming the parameter and the accepted forms:
    - unknown units, extra words or commas;
    - negative parts;
    - parts with more than 15 integer digits;
    - fractions of calendar units such as `'0.5 months'`, which Carbon
      truncates;
    - ISO 8601 strings (pass a `DateInterval` instead);
    - negative or relative intervals.
  - Activity and child workflow timeouts must be at least one second.
  - **Unchanged:** int and float arguments, and numeric strings such as `'30'`
    in files without `strict_types`. Timers and condition timeouts still round
    numeric strings up. Activity and child options still pass them through.
- `nesbot/carbon` `^2.72.6|^3.8.4` is now a required dependency, so every
  worker converts durations with the same parser as the embedded package. The
  floors are Carbon's patched releases for CVE-2025-22145, and the range
  overlaps every supported Laravel line. The production dependency boundary
  allows Carbon's Symfony translation, clock and polyfill dependencies. CI runs
  the unit tests on Carbon 2 as well as Carbon 3.

- Replay evidence for Workflow 2.19 redrive reuse of `MemoUpserted` and
  `SearchAttributesUpserted`, and for Core's reused signal-wait events
  (`SignalWaitOpened`, `SignalReceived`, `SignalApplied`). Each fixture is a
  redrive successor checked by a shared test validator: `WorkflowStarted`
  carries the successor identity, link and redrive envelope, with
  `resume_step_sequence` one past the last reused step; the reused prefix
  follows it; successor rows come later and bind to the successor run. A
  cold worker replays them with the recorded values and does not upsert
  again or reopen the wait. The reused times are not a replay time source
  here: cooperative cancellation `remaining()` starts at a successor
  delivery after the prefix. No runtime change was needed. This does not
  qualify PHP service workers for Core signal waits, which this SDK cannot
  author.

### Fixed

- Correction to 3.8.3: its cancellation-clock fixture
  (`redrive-reused-cancellation-clock`) was not a valid Workflow 2.19 redrive
  successor history. It had no redrive `WorkflowStarted` and placed the
  cancellation before the reused prefix. The 3.8.3 claim that cleanup after a
  reused activity completion or fired timer no longer loses the redrive delay
  from its budget is incorrect. In valid redrive histories the reused prefix
  always precedes the successor's cancellation delivery, so `remaining()` is
  computed from that delivery and cannot observe a reused event's earlier
  `reused_recorded_at`. Preferring `reused_recorded_at` changes no observable
  result and restores no cleanup budget. The runtime code is unchanged.
  - Reused activity completions, fired timers, side effects and version
    markers, and a successor cancellation after them, are now qualified with
    valid successor histories built to Workflow 2.19 (`redrive-successor-*`).
  - The 3.8.3 markers and timer fixtures also lacked the redrive envelope.
    The three 3.8.3 fixtures stay in the immutable corpus, byte-identical, but
    are named as non-qualifying evidence. Every redrive fixture must pass the
    successor check or be one of those named exceptions.
  - The successor check accepts the `TimerScheduled` Core copies for a reused
    fired timer: no reuse keys, a new `timer_id`, and directly followed by the
    reused `TimerFired` of the same timer and step.
- Replay now checks the values of a recorded `SearchAttributesUpserted`
  against the workflow's `upsertSearchAttributes()` call, as it already did
  for memo upserts, including an upsert a redrive successor reused from the
  failed run.
  - A changed value, PHP value type or key set raises
    `NonDeterministicWorkflow` (`search_attribute_value_mismatch`) instead
    of being accepted.
  - Both sides are normalized as Core does (trimmed strings and list items,
    an empty string as a deletion, sorted keys) and compared structurally
    and type-strictly. Ints and floats differ, `-0.0` equals `0.0`, and the
    result does not depend on JSON formatting or `serialize_precision`.
  - Keys and values outside Core's portable set refuse replay with
    `search_attributes_unportable`: non-string or non-portable keys,
    objects, maps, nested or non-string lists, non-finite floats, strings
    over 2,048 characters and list items over 255.
  - A recorded upsert without its `attributes` refuses with
    `search_attribute_update_missing`, the reason sdk-rust uses.
  - A second `SearchAttributesUpserted` at one workflow sequence refuses
    with `duplicate_search_attribute_update`. A memo or search-attribute
    upsert sharing a sequence with any other durable command refuses with
    `durable_command_sequence_collision` in either order. An explicit claim
    map decides which rows claim a sequence, even when the command's opener
    is absent. It follows Core's `WorkflowStepHistory`: activity rows
    (retry, heartbeat and cancellation acknowledgement included, but not
    the payload-free `ActivityOutcomeRejected` audit row), timer, child,
    condition and signal-wait rows (including terminal, cancelled and
    internal timeout timer rows), service-call rows, side effects, version
    markers, authored cancellation scope openings and continue-as-new. A
    `CooperativeCancellationDelivered` or `CancellationScopeDelivered`
    claims every sequence in its `sequence_span`, inclusive. Before, the
    later event could silently replace the earlier one or be ignored.
  - A recorded upsert must carry its own positive integer `sequence`.
    Replay no longer synthesizes one: a missing sequence refuses with
    `durable_command_sequence_missing`, and an invalid one with
    `durable_command_sequence_invalid`, as in sdk-rust.
  - Recorded `attribute_types` are not compared: this SDK's command cannot
    declare types, so this is value-only hardening and does not qualify
    typed search-attribute reuse.

## [3.8.3] - 2026-11-03

### Fixed

- Cooperative cancellation `remaining()` now advances to a redrive-reused
  step's original `reused_recorded_at` time instead of the time the successor
  run copied it, matching Core. Cleanup after a reused activity completion or
  fired timer no longer loses the redrive delay from its budget.

## [3.8.2] - 2026-11-03

### Fixed

- Service-mode Laravel support now names the patched minimum releases for
  GHSA-f9x3-7vq9-cph6: Laravel 10.50.4, 11.56.1, 12.69.0 and 13.30.0, and later
  releases of each major (`^10.50.4|^11.56.1|^12.69.0|^13.30.0`). 3.8.1
  published major-only ranges (`^10.0|^11.0|^12.0|^13.0`), which included
  releases from before the TokenGuard fix. Earlier Laravel 10, 11, 12 and 13
  releases are outside the supported service-mode boundary. The Composer
  framework metadata and `suggest` text and the Laravel adoption contract and
  its schema now publish these floors, matching Workflow 2.17.1's embedded v2
  authority.
- Each CI framework-compat Laravel cell now installs exactly its major's
  minimum release (`laravel/framework` 10.50.4, 11.56.1, 12.69.0 or 13.30.0),
  never a later one. It checks the installed version, from Composer's metadata
  and Laravel's `Application::VERSION`, before and after installing the SDK and
  its test tooling. Every adoption contract cell records both its caret support
  range and the exact release it exercises.
- The embedded-v2 to service-mode transition check now compares the exact
  Laravel and PHP constraints as well as the Laravel/PHP cells, and that each
  cell names and exercises its major's floor, so a future floor drift between
  the SDK and Workflow fails CI.
- The SDK still only suggests `laravel/framework`. It adds no Composer
  `conflict` and does not change an application's Laravel resolution.

### Deprecated

- Laravel 10 and 11 are past their upstream end of life, and some published
  Laravel advisories have no fixed 10.x or 11.x release. They remain in the
  service-mode support boundary only for bounded 3.8.x transition
  compatibility, matching Workflow 2.17.1, and will be removed in the next SDK
  minor (#239). Move to a maintained Laravel release (12 or 13).

## [3.8.1] - 2026-11-02

### Changed

- Service-mode Laravel bridge support now starts at Laravel 10, matching
  Workflow 2.17.0 (GHSA-f9x3-7vq9-cph6). Laravel 9 has no fixed release for
  that advisory, so upgrade a Laravel 9 application before adopting service
  mode. The Composer framework metadata, the Laravel adoption contract and its
  qualification matrix, and the CI framework cells now list Laravel 10 through 13.

### Fixed

- Activities with a `heartbeat_timeout` run again on published PHP Server 2.11
  and native Server 3.0 when activity lease maintenance is active. SDK 3.8.0
  abandoned these claims. The first runtime renewal reports
  `last_heartbeat_at: null` until the first application heartbeat
  (`activity_maintenance_bootstrap_failed`). Accepted heartbeat replies omit
  `heartbeat_deadline_at` (`invalid_activity_heartbeat`).
- A renewal that reports no progress grants none. It never moves the recorded
  progress time, and a stated `heartbeat_deadline_at` can only shorten the
  claim's heartbeat bound. A heartbeat reply without a deadline advances
  progress by the original `heartbeat_timeout`, which is the deadline the
  Server records. Present timestamps must still be canonical UTC, a stated
  deadline must still match the original interval, and progress later than
  Server time is still refused.

## [3.8.0] - 2026-11-02

### Added

- Optional SDK metrics that follow the shared cross-SDK contract. Client
  requests, worker polls and worker tasks each record a counter and a duration
  histogram through a pluggable `MetricsRecorder`. Pass it as `Client(metrics:)`;
  a worker inherits it, or takes its own through `Worker::create(metrics:)`.
  The default `NoopMetricsRecorder` discards observations, and recorder failures
  never change results, retries, deadlines or task authority.
- Labels are bounded. Request routes are reviewed templates shared with the
  Python SDK, and any other path becomes `unknown`. Workflow, run, task,
  worker and session identifiers, query strings, payloads, credentials and
  exception messages never become labels. Worker task outcomes describe the
  worker's processing, separate from workflow terminal state. A forked
  cooperative child process does not record through the parent's recorder.
- `InMemoryMetricsRecorder` for tests and diagnostics, and
  `OpenTelemetryMetricsRecorder` for an application-owned OpenTelemetry meter
  provider (needs the optional `open-telemetry/api`; no new dependency). The new
  Metrics guide covers OpenTelemetry export and Prometheus scraping through an
  OpenTelemetry Collector. See `examples/metrics-opentelemetry.php`.

## [3.7.0] - 2026-11-02

### Added

- Worker requests other than long polls now have a finite, configurable deadline
  on SDK-owned transports. These are registration, worker and activity heartbeats,
  workflow and activity task completion and failure, and deregistration.
  `workerRequestTimeoutSeconds` (default 60 seconds) is separate from the control
  `requestTimeoutSeconds`, so neither shortens the other. Laravel and Symfony
  configure it with `worker_request_timeout_seconds`. Previously a default
  worker could wait indefinitely on a Server that accepted the connection but
  never answered. A timeout raises a typed `ServerException` with `reason`
  `request_timeout`. Task polls keep their own budget, cooperative workers keep
  their per-operation budgets, and injected transports keep their own policy.

## [3.6.2] - 2026-11-02

### Added

- Declare minimum stable Server releases separately for the PHP 2.x and native
  3.x Server lines. Preserve the existing 2.x metadata for older release readers;
  worker protocol and capability negotiation remain required.

## [3.6.0] - 2026-10-27

### Added

- Activity contexts retain an immutable received retry snapshot and original
  attempt number through ordinary and supervised callbacks. Bounded final-attempt
  checks preserve absent/legacy/invalid policies as unknown and distinguish an
  explicit unlimited policy without filling defaults or discarding future fields.
- Nullable owning `workflowId` and `runId` let activities signal their original
  run. Local activities retain known parent IDs; older claims and Server-free
  tests preserve unavailable identity. Guides show retry checks and a run-pinned
  self-signal with a stable application event identity.

## [3.5.1] - 2026-10-25

### Fixed

- Negotiate worker poll progress with supporting Servers, preserving terminal
  status, refusal details and retry headers. Ordinary JSON remains compatible.
  Progress bytes do not extend the complete cURL request deadline.
- Report early worker poll connection failures and gateway cutoffs at warning
  level with observed duration, configured wait and timeout remediation.
  Retries retain the original poll identity.

## [3.5.0] - 2026-10-23

### Added

- `Client::withFailureReportLimit()` narrows the complete encoded failure-report
  budget. Transports may expose a lower known limit. Activity and workflow-task
  reporting accept optional stack traces, and activity reporting accepts
  diagnostics, after all existing arguments.

### Fixed

- Bound failure messages to 62 KiB, stacks to 128 KiB, combined details and
  diagnostics to 64 KiB, and complete encoded reports to 1 MiB by default.
  Markers and JSON escaping count toward these limits. Oversized details remain
  whole valid Avro values or are omitted with original-size diagnostics.
- Preserve original failure types, retry decisions, authored delays and claim
  identities. Local durable failures normalize before workflow code observes
  them, so initial execution, persistence and cold replay agree. Cooperative
  failure IPC has its own diagnostic budget, separate from business payloads.
- Keep ordinary successful payload budgets unchanged. Mixed failure/result
  completions use the existing upload planner without changing or slicing the
  business payload. Unfit required report content refuses explicitly.

## [3.4.1] - 2026-10-23

### Fixed

- Preserve whole-float numeric kinds and the sign of zero in outbound HTTP
  JSON, including nested workflow/schedule memo and search attributes. Integer
  metadata and Avro business payloads retain their existing representation.
  Request-size planning counts the same preserved JSON bytes before uploading
  payloads or submitting an oversized request.

## [3.4.0] - 2026-10-23

### Added

- A configurable total HTTP request deadline for ordinary control calls, with a
  60-second default and typed `request_timeout` failures. Set
  `Client(requestTimeoutSeconds:)` or the framework `request_timeout_seconds`
  setting. A timed-out mutation is not automatically resubmitted.
- A mature socket-client adapter for control calls without cURL, preserving
  PHP 8.1, HTTP-only installations without OpenSSL, proxy routing, certificate
  and hostname verification, and authentication. Incomplete HTTP transfers fail
  before their JSON can be accepted. Injected clients retain their own policies;
  worker polling, lease/heartbeat and payload-transfer budgets stay separate.

## [3.3.0] - 2026-10-23

### Added

- Typed `ServiceOperationOptions::targetNamespace` selection for durable
  workflow service calls, equivalent to the existing `namespace` array option.
  Constructor arguments, caller authority, waiting defaults and replay identity
  remain compatible. Direct service clients select their namespace with
  `Client::withNamespace()` and explicitly refuse this workflow option.

## [3.2.1] - 2026-10-23

### Fixed

- Keep workers running after temporary proxy or connection failures during a
  claimed workflow lease renewal, workflow completion or activity completion.
  Retries retain the original task fence and computed commands or result, so
  an uncertain completion does not invoke the handler again.

## [3.2.0] - 2026-10-21

### Added

- Optional, bounded metadata-only activity lease renewal against runtimes that
  explicitly advertise it, without a new worker protocol floor.
- Automatic lease and Worker-presence maintenance in an independent PHP CLI
  helper while ordinary activity callbacks retain their inline execution and
  application-owned service identities. Custom HTTP/authentication setups can
  provide an explicit fresh-process Client bootstrap.

### Fixed

- Fence late activity outcomes after original lease, business, progress or
  required session authority is lost. Maintenance cannot fabricate application
  heartbeats, restore an expired attempt or reset an original session TTL.

## [3.1.0] - 2026-10-21

### Added

- Optional execution spans and Monolog correlation for workflow/query tasks
  and real activity attempts. Applications own providers, exporters, sampling
  and bounded shutdown. Default installations do not require OpenTelemetry.
- Bounded W3C caller propagation when Server discovery advertises the optional
  observability capability. Run origins parent tasks; real dispatch, command
  and outcome contexts become links after existing authority checks. Baggage
  defaults empty and is excluded from links.
- Fiber scope restoration and fresh child-provider configuration for
  supervised callbacks. Replay does not emit activity attempts for recorded
  results. Existing request IDs and business arguments remain unchanged.

## [3.0.0] - 2026-10-20

### Changed

- Construct a fresh attributed workflow handler for every execution, replay,
  query and update. Register workflow classes or an explicit `WorkflowFactory`
  instead of prebuilt workflow objects. PSR-11 workflow bindings must be
  transient; Laravel and Symfony retain shared injected collaborator identities.
- Run workflow constructors and factories during replay. Keep external side
  effects in activities. Activity services and low-level callable lifetimes
  retain their application-owned behavior.

### Fixed

- Isolate constructor-created nested mutable state across workflow IDs, runs
  and replay attempts. Reject reused or changing factory results before polling
  and report later construction failures as recoverable replay errors.
- Retry expired idle worker I/O budgets with backoff and restore registration
  before acquiring more work. Task and cancellation authority deadlines retain
  their original bounds.

## [2.5.0] - 2026-10-18

### Added

- Replay cooperative cancellation at an original completion-waiting Nexus
  service call. Cooperative workers advertise support for the owned
  `workflow_run` binding. Delivery requires a compatible runtime and preserves
  the original cancellation request and cleanup deadline.

## [2.4.2] - 2026-10-15

### Fixed

- Preserve original query deadlines through loading, preparation and result
  submission. Observe advertised protocol 1.20 authority without renewing it,
  and abandon expired claims without stopping the Worker or invoking a late
  handler. Already-entered synchronous handlers remain non-interruptible.

### Added

- Optional `requireQueryTaskAuthority` pre-registration requirement, with
  explicit transport, legacy and protocol 1.19 compatibility limits.

## [2.4.1] - 2026-10-14

### Fixed

- Include actual lifetime history-cache counters in the worker's shutdown
  diagnostic. Logs and Laravel/Symfony events preserve those counters after
  retained history is cleared.

## [2.4.0] - 2026-10-14

### Added

- Read one workflow activity result with `Client::workflowActivityResult()` on
  Servers that advertise activity-result inspection. Existing payload decoding
  verifies and hydrates only the selected result.

## [2.3.9] - 2026-10-13

### Fixed

- Preserve computed workflow completions through explicitly fenced backend lock
  pressure. Retry identical content with bounded backoff under the original
  task, attempt and owner; reconcile a known committed completion without
  running its handler again. Shutdown and cancellation retain their bounds.
- Use the managed worker lifecycle in worker and shutdown guides. `run()` owns
  registration, heartbeat state and deregistration; a `tick()` loop alone does
  not establish that lifecycle.

## [2.2.7] - 2026-10-11

### Fixed

- Recover orderly worker shutdown from temporary backend pressure when Server
  advertises fenced deregistration. Requests and retries share one bounded
  shutdown budget and preserve the original registration identity. A replayed
  receipt cannot remove a replacement registration. Persistent shutdown
  failures remain visible without replacing an earlier worker error.
- Keep the existing single-attempt shutdown on older Servers. Custom transports
  without bounded I/O make one fenced attempt and report why retry is unavailable.

## [2.2.6] - 2026-10-08

### Fixed

- Keep workers alive when an external payload download temporarily fails
  during a workflow, activity or query poll. Recovery preserves the admitted
  poll request identity and uses the existing observable backoff and worker
  health checks. Authentication, expired references and payload integrity
  failures remain errors.

## [2.1.6] - 2026-09-29

### Fixed

- Request bounded workflow history pages in managed PHP worker polls. Long
  valid histories no longer require an unbounded Server poll response. Direct
  low-level Client polls keep their existing default request behavior.

## [2.1.5] - 2026-09-27

### Fixed

- Preserve an activity's computed result when payload-policy discovery fails
  before its completion request is sent. With Server 2.4.15 or later, the worker
  retries the same fenced completion after the typed backend-loss response,
  without rerunning the activity handler. Other discovery failures remain errors.

## [2.1.4] - 2026-09-26

### Fixed

- Retry an activity completion after a compatible Server reports temporary
  backend loss with an unknown outcome, preserving the original result and
  activity lease. If the first completion committed, recognize the matching
  completed-attempt conflict without rerunning the activity handler. Server
  2.4.13 adds the response; older Servers retain their existing behavior.

## [2.1.3] - 2026-09-26

### Fixed

- Retry a workflow-task completion after a compatible Server reports temporary
  database loss with an unknown outcome, checking the exact task, lease owner,
  and attempt before resending the same completion. If the first completion
  committed, discard the completed-task conflict without rerunning the handler.
  Server 2.4.12 adds the response; older Servers retain their existing behavior.

## [2.1.2] - 2026-09-26

### Fixed

- Retry a workflow-task heartbeat after a compatible Server reports temporary
  database loss with an unknown outcome, checking the exact task, lease owner,
  and attempt before resending the same heartbeat. Server 2.4.11 adds this
  response; older Servers retain their existing behavior.

## [2.1.1] - 2026-09-25

### Fixed

- Wake an idle PHP worker to check other task kinds when a compatible Server
  queues work on the same namespace and task queue. Server 2.4.7 adds the
  optional wake signal; older Servers keep their previous polling behavior.

## [2.1.0] - 2026-09-23

### Added

- Expose failed-run redrive and per-run activity provenance through the client.
  Redrive requires a Server with the run-targeted redrive endpoint; the
  published quickstart is qualified against Server 2.4.0.
- Let workers advertise a source-backed workflow-definition fingerprint so
  redrive can refuse changed or uninspectable workflow code.

### Fixed

- Replay completed activity results copied into a redrive successor without
  scheduling those activities again. Uncaught activity failures carry their
  recorded boundary; handled failures do not claim a resumable step.

## [2.0.11] - 2026-09-21

### Fixed

- Preserve HTTP status when an upstream proxy returns HTML, an empty body, or
  another non-object response. Diagnostics omit the unstructured response body.
- Managed polling and worker heartbeats recover from supported temporary
  upstream failures with capped backoff and responsive shutdown. Uncertain
  polls reuse their identity; already leased tasks are not delayed by heartbeat
  backoff. Ambiguous registration/completion and explicit refusals remain errors.

## [2.0.10] - 2026-09-11

### Fixed

- A repeated long-poll capacity refusal no longer prevents the managed worker
  from polling other task kinds. Explicit empty-task refusals honor backoff,
  heartbeats and shutdown before yielding to activity or query polling.
- Ambiguous transport, database and storage-admission failures retain their
  existing retry and poll-identity behavior; lease fencing is unchanged.

## [2.0.9] - 2026-09-08

### Fixed

- On supporting Servers, retry a draining refusal for a completion payload
  with its exact activity, workflow, or query lease and immutable payload slot.
  Client uploads, unknown capabilities, and hard storage fences remain blocked.
- Preserve computed outcomes during late upload pressure instead of executing
  the handler again. The Server's bounded allowance and namespace quotas still
  apply; stale leases remain errors.

## [2.0.7] - 2026-09-08

### Fixed

- Workers retry explicit storage-admission refusals while preserving poll,
  task and attempt identities. A rejected acknowledgement retains the computed
  result or failure rather than executing its handler again.
- Registration and heartbeat recovery remain interruptible. Shutdown does not
  turn a storage refusal into an activity failure or a successful completion;
  authentication failures, stale leases and ambiguous acknowledgements remain
  explicit errors.

## [2.0.6] - 2026-09-07

### Fixed

- Managed workers retry typed temporary database-unavailable responses during
  registration, heartbeat, and polling. Polls retain the original request ID
  when a task claim's outcome is unknown. Authentication failures, malformed
  responses, and stale-registration fences remain terminal.

## [2.0.5] - 2026-09-06

### Fixed

- Discarding suspended replay Fibers no longer raises a force-close error when
  `finally` contains durable calls. Activity, timer, memo, and operation-handle
  cleanup still follows normal replay, without retaining discarded executions.

## [2.0.4] - 2026-09-06

### Fixed

- Persisted workflow execution and run deadlines raise `WorkflowTimedOut`
  instead of `WorkflowFailed`. Selected-run results and caller wait timeouts
  retain their existing behavior.
- Managed workers continue polling after a completion is rejected because the
  selected run timed out. Task, attempt, and run identity must match; unrelated
  conflicts remain errors and low-level completion calls still expose HTTP 409.

## [2.0.3] - 2026-09-06

### Fixed

- Client reads and worker replay resolve runtime-owned external Avro payloads
  through the authenticated runtime instead of failing on a missing inline blob.
  Downloads retain the namespace and credential role and never use provider URLs
  or storage credentials.
- External payload reads verify reference shape, codec, response metadata, length
  and SHA-256. Downloads are bounded per response, do not follow redirects, and
  surface typed transport/integrity failures instead of lossy JSON fallbacks.
- Custom transports can implement `PayloadTransport`; the built-in PSR-18
  transport supports it. Inline payloads and existing JSON-only custom transports
  retain their previous behavior. `maxExternalPayloadBytes` configures the
  per-response wire-byte limit, which defaults to 64 MiB.

## [2.0.2] - 2026-09-06

### Fixed

- Managed workers recover from classified temporary connection failures during
  polling and worker heartbeats, with capped backoff and interruptible waits.
  Poll identities survive reconnects so Server can reconcile lost responses.
- Guzzle 7 and 8 transport classification keeps TLS/configuration errors,
  malformed responses, and authentication failures explicit. Custom transports
  may identify connection loss with `TransportException`'s
  `transientConnectionFailure` flag; opaque errors are not silently retried.
- Heartbeat retry handling does not recursively wait or repeat activity effects.
  Ambiguous registration and task-completion failures remain surfaced to the
  supervisor; existing lease fencing and terminal worker states are unchanged.

## [2.0.1] - 2026-09-03

### Added

- A task-oriented Message Streams guide and shipped PHP examples now show
  client-role appends with stable message identities alongside deterministic
  single-message and bounded-batch worker consumption.

### Changed

- Qualified the stable PHP SDK against the additive Server `2.1.0` release,
  which preserves worker protocol `1.19` while adding tenant-scoped runtime
  credential management to the Server control plane.

## [2.0.0] - 2026-09-01

### Added

- Service workers can execute deterministically recorded local activities,
  manage typed leased worker sessions, and use bounded sticky workflow caches.
  Replayed local activities never repeat recorded side effects, while expired,
  evicted, replaced, or build-mismatched cache entries force full durable replay.
- `WorkflowContext::upsertMemo()` now authors validated memo patches, replays
  `MemoUpserted` identities, carries opaque payload envelopes, and fails before
  completion when the connected runtime does not advertise memo updates.
- Fiber-backed service workflows can create a package-owned `Saga`, register
  ordinary activity compensations after forward success, and replay them in
  deterministic reverse order. Typed compensation failures retain both the
  failed forward step and failed compensation, while completed compensations
  are not rescheduled after restart or redelivery.
- `WorkflowContext::messageStream()` now consumes named repeated input in
  ordered bounded batches. Runtime-owned cursor and wait metadata survives
  replay, worker replacement, server restart, duplicates, and continue-as-new
  without exposing the reserved transport signal to workflow declarations.
- Fiber-backed service workflows can fan out deferred activities, child
  workflows, and timers with `WorkflowContext::all()` or `parallel()`, including
  mixed and nested groups. Replay preserves declaration-order results, validates
  the language-neutral group/path identity, and resumes partial groups without
  scheduling completed members again.
- Fiber-backed service workflows can use `WorkflowContext::getVersion()` to
  record one durable code-change decision and replay it across worker upgrades.
  `patched()` and `deprecatePatch()` provide the standard boolean patch
  lifecycle, with typed nondeterminism diagnostics for invalid calls or marker
  history.
- Fiber-backed service workflows can use `WorkflowContext::waitCondition()` to
  wait on deterministic state with a stable replay identity and optional
  timeout. The worker emits the Server's `open_condition_wait` command,
  re-evaluates the predicate after signals and updates, and reports a dedicated
  condition-wait diagnostic.
- Run-scoped Workflow Streams now expose typed lifecycle, resumable
  at-least-once consumption, append, close, and errored operations through the
  application-facing client contract. Workflow commands derive append
  idempotency from durable command identity so replay cannot duplicate stream
  records.
- Class-oriented worker authoring with attributes for workflow, activity,
  query, signal, and update contracts; registration validates definitions and
  supports PSR-11 dependency resolution, PSR-3 logging, lifecycle diagnostics,
  and graceful deregistration.
- Framework-independent workflow client fakes and worker handler harnesses with
  assertions for workflow interactions and registered handler behavior.
- A framework-neutral first-run guide and shipped plain-PHP example for Cloud
  and self-hosted Server, including role-scoped credentials, attributed handler
  registration, and Composer autoload discovery across supported layouts.
- A source-free published-package smoke workflow that installs and executes the
  shipped quickstart against a real Server or Cloud-compatible endpoint.
- Optional first-party bridges for Laravel 9-13 and Symfony 6.4-8, including
  native container registration, console workers, configuration validation,
  framework logging and events, and test-client replacement helpers.
- A Laravel-native injectable workflow client that derives the public type from
  a configured attributed service class and uses the configured task queue,
  with explicit ID and start-option overrides when application code needs them.
  Its facade fake records and asserts the same service-class call shape.
- A machine-readable Laravel adoption contract and ownership-first migration
  guide for v1 to v2 embedded, v1 to v2 service mode, and embedded v2 to
  service mode.

### Changed

- Published Laravel qualification now runs a PHP-authored saga with Python
  forward and compensation activities against exact standalone Server and
  managed Cloud targets, restarting the PHP worker during compensation and
  rejecting reordered or duplicate compensation history.
- Published Laravel qualification now cold-replays a partially completed mixed
  and nested group, then exercises activity-only, child-only, and fail-fast
  groups against exact standalone Server and managed Cloud artifacts. Symfony
  qualification retains its ordered activity-group coverage.
- Published Laravel bridge qualification now runs the container-injected
  workflow and activity through Artisan against separately credentialed
  standalone Server and managed Cloud targets, and proves the Laravel fake
  without either runtime.
- Laravel service mode now uses the same runtime URL, namespace, task queue,
  client-token, and worker-token vocabulary as the package quickstart. Worker
  startup reports an explicit registered-and-polling record, and clean
  transition jobs preserve stable-v1 or embedded-v2 history while exercising a
  new service-mode start with the same injected application dependency.
- Replaced prerelease generator-based workflow authoring with Fiber-backed
  straight-line execution. Durable operations now return recorded values or
  throw recorded failures directly at their call sites, while replay retains
  strict command-order, history-shape, and command-detail validation.
- Published the stable PHP SDK `2.0.0`, qualified with Server `2.0.0`.
  Worker registration explicitly advertises memo upserts and
  typed search attributes, with worker protocol `1.19` and explicit floors for
  portable worker affinity (`1.18`) and durable selection (`1.19`). During the
  worker-session lifecycle, session identifiers are canonicalized once so
  create, routing, renewal, explicit close, and shutdown target one identity.
  Package metadata names the exact SDK artifact and exact qualified Server
  artifact. The prereleases remain historical and receive no compatibility
  shim. Avro is the sole public
  Durable Workflow 2.0 payload codec, and non-Avro configuration fails before
  transmission. Managed registration emits an explicit empty validator list,
  while low-level registration documents and preserves that declaration and
  rejects malformed or non-empty claims before transport.
- Replaced the prerelease JSON wrapper with the fixed recursive
  `durable_workflow.protocol.Value` schema and Avro single-object framing.
  `AvroBinaryValue` makes the bytes branch explicit while PHP strings remain
  UTF-8 text. `AvroMapValue` preserves empty maps and numeric-looking string
  keys that native PHP arrays cannot represent, and the reader resolves
  recursive named branches without warning suppression.
- Added golden cross-language fixtures and a repeatable production-path size
  and latency benchmark over the shared checked-in corpus, with an enforced
  regression budget calibrated with explicit headroom.

- Release-plan recovery now consumes immutable, exact-version release-note
  preparation authority before publishing a newly recorded plan.

### Fixed

- Laravel can resolve and constructor-inject both documented application client
  interfaces while an Artisan worker is running with only a worker credential.
  Client credentials remain process-scoped and are validated when an injected
  interface performs its first operation; direct `Client` injection remains an
  explicit eager boundary.
- Workers now reject workflow, update, activity, and query tasks whose root
  `payload_codec` is missing or is not exactly `avro`, before payload decoding
  or user-handler execution, and report `unsupported_payload_codec` through the
  task failure path. Unsupported activity-task codecs are non-retryable so the
  Server terminates the incompatible task instead of redelivering it.
- Workflows can select the first durably committed activity, child workflow,
  timer, or condition result without cancelling siblings. Stable handles allow
  remaining operations to continue, be awaited later, or be cancelled explicitly.
- Laravel role processes can use `DURABLE_WORKFLOW_PROCESS_ROLE` with
  `DURABLE_WORKFLOW_PROCESS_TOKEN` as an explicit single-credential handoff.
  It rejects ambient, incomplete, invalid, and opposite-role authentication
  state. Published qualification uses the handoff after configuration caching
  so its worker and application children never inherit both scoped credentials.
- Laravel role clients now read scoped credentials from the operating-system
  process environment after configuration caching, avoiding stale framework
  environment repositories while preserving worker/client isolation.
- SDK installation now accepts Guzzle 8 as well as Guzzle 7, preserving clean
  installation in Laravel 13 applications while existing Guzzle 7 locks remain
  valid.
- Low-level schedule updates and workflow-task completions now validate only
  their schema-defined durable payload fields as fixed-schema Avro before any
  transport request, while preserving codec-looking memo, search-attribute,
  and failure metadata as user-owned data. Caller-supplied payload arrays must
  use an exact inline Avro envelope or a structurally valid external-storage
  reference, which is preserved for server-side resolution.
- Laravel now resolves shared or role-scoped credentials when each client is
  constructed, keeping secrets out of cached configuration while preserving
  the control/worker privilege boundary.
- Laravel and Symfony now bind separate control-only and worker-only clients for
  scoped Cloud credentials. Missing role credentials fail before transport,
  while the shared self-hosted token remains valid for both roles.
- Graceful worker shutdown now uses the worker-plane registration lifecycle
  route; operator-initiated worker removal remains on the control plane.

- Attribute-discovered workflow entry points, queries, and updates now start
  from clean handler object state for every replay or invocation, without
  losing constructor-injected collaborators. Activity services and explicit
  low-level callables retain their application-owned lifetimes.

- The generated API reference omits its responsive header-menu control when
  the top navigation has no destinations, leaving search and sidebar
  navigation as the useful narrow-viewport interactions.

- The generated API reference constrains and, when needed, wraps its SDK title
  at narrow viewport widths so the shared header controls remain usable without
  document-level horizontal overflow.

- The generated API reference reserves a clear reading lane beside its fixed
  back-to-top utility and removes that utility while mobile navigation or search
  overlays are open.

- The generated API reference now uses Cloudflare Web Analytics with its
  supported cookie-free module loader and token-only configuration, and no
  longer presents analytics consent or preference controls.

- Client construction rejects base URIs ending in the SDK-owned `/api` path
  segment before they can produce duplicated request paths.

- Explicit release recovery rejects terminally superseded plans before and
  after publication preflight while keeping completed-plan verification
  idempotent.

- Managed workers retry the server's fenced registration lock-pressure
  response with bounded backoff before entering the poll loop, while other
  registration failures remain terminal.

## [0.1.12] - 2026-07-19

### Fixed

- Managed workers recognize cancelled workflow tasks reported as no longer
  leased as terminal task races, while requiring the refusal to identify the
  task being acknowledged.

## [0.1.11] - 2026-07-18

### Changed

- Managed workers retry explicitly transient workflow, activity, and query
  poll refusals with observable capped backoff while preserving heartbeats and
  responsive shutdown; workflow-task execution also waits for a successful
  typed lease renewal after transient backend pressure. Unrelated server
  failures and invalid fencing outcomes remain fatal.

## [0.1.10] - 2026-07-17

### Changed

- Managed workers discard workflow, activity, and query tasks whose typed
  acknowledgements report that the leased task became terminal concurrently,
  then continue polling for unrelated work.
- Release recovery creates or verifies the exact planned source tag before
  publishing its GitHub Release, and public workflows qualify every supported
  target branch.

## [0.1.9] - 2026-07-16

### Added

- Automated recovery of the exact PHP SDK release selected by an immutable
  cross-repository release plan.

## [0.1.8] - 2026-07-16

### Added

- Replay-consumed workflow signal declarations and their parameter contracts
  in managed worker registration metadata.

## [0.1.7] - 2026-07-16

### Fixed

- Managed workers resolve their advertised SDK version from the installed
  Composer package instead of a source constant.

## [0.1.6] - 2026-07-15

### Added

- Reflected parameter contracts for declared workflow query and update
  handlers in managed worker registrations.

## [0.1.5] - 2026-07-14

### Added

- Per-workflow query and update command names in managed worker registrations
  so the server can address declared handlers on new runs.

## [0.1.4] - 2026-07-14

### Changed

- Managed workers adopt the server-advertised heartbeat cadence and refresh
  their registration between heartbeat-bounded workflow, activity, and query
  long polls.

## [0.1.3] - 2026-07-14

### Added

- Full workflow, activity, and query poll-response methods that preserve typed
  refusal and protocol metadata.

### Changed

- Managed workers stop polling after stale-registration, drain, and stop
  outcomes while ordinary empty polls remain idle.

## [0.1.2] - 2026-07-14

### Added

- Server-side schedule visibility filters and opaque continuation-token paging.

## [0.1.1] - 2026-07-14

### Added

- Immutable namespace selection and typed global workflow visibility pages.
- Search-attribute administration and namespace external-storage policy updates.
- Avro-backed service-operation start, execute, describe, and cancel APIs.
- Typed cluster discovery plus schedule page, continuation, and history route coverage.

## [0.1.0] - 2026-07-13

### Added

- Framework-neutral control-plane client with current-run and selected-run handles.
- Schedule and namespace management.
- PSR-18 transport and control/worker bearer-token authentication.
- Apache Avro generic-wrapper and typed-schema payload support.
- Remote PHP worker execution, heartbeats, history replay, query/update tasks,
  cancellation, and graceful shutdown.
- Generated API reference and supported PHP-version CI.

[Unreleased]: https://github.com/durable-workflow/sdk-php/compare/3.3.0...HEAD
[3.3.0]: https://github.com/durable-workflow/sdk-php/compare/3.2.1...3.3.0
[3.2.1]: https://github.com/durable-workflow/sdk-php/compare/3.2.0...3.2.1
[3.2.0]: https://github.com/durable-workflow/sdk-php/compare/3.1.0...3.2.0
[0.1.12]: https://github.com/durable-workflow/sdk-php/compare/0.1.11...0.1.12
[0.1.11]: https://github.com/durable-workflow/sdk-php/compare/0.1.10...0.1.11
[0.1.10]: https://github.com/durable-workflow/sdk-php/compare/0.1.9...0.1.10
[0.1.9]: https://github.com/durable-workflow/sdk-php/compare/0.1.8...0.1.9
[0.1.8]: https://github.com/durable-workflow/sdk-php/compare/0.1.7...0.1.8
[0.1.7]: https://github.com/durable-workflow/sdk-php/compare/0.1.6...0.1.7
[0.1.6]: https://github.com/durable-workflow/sdk-php/compare/0.1.5...0.1.6
[0.1.5]: https://github.com/durable-workflow/sdk-php/compare/0.1.4...0.1.5
[0.1.4]: https://github.com/durable-workflow/sdk-php/compare/0.1.3...0.1.4
[0.1.3]: https://github.com/durable-workflow/sdk-php/compare/0.1.2...0.1.3
[0.1.2]: https://github.com/durable-workflow/sdk-php/compare/0.1.1...0.1.2
[0.1.1]: https://github.com/durable-workflow/sdk-php/compare/0.1.0...0.1.1
[0.1.0]: https://github.com/durable-workflow/sdk-php/releases/tag/0.1.0
