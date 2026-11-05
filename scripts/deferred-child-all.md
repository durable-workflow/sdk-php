# Deferred child barrier consumer adapter

This source runner prepares the PHP consumer side of [#185](https://github.com/durable-workflow/sdk-php/issues/185)
and [Server #325](https://github.com/durable-workflow/server/issues/325).
It has not qualified an installed SDK or a native Server tuple. A future run
requires the published SDK, immutable native producer, runner revision, resource
budget and cleanup plan selected in those records. The autoload argument alone
does not attest a published package. Ordinary source checks cannot replace that
acceptance.

`fixtures/parallel-child-all.json` is copied byte for byte from the Server source
identified by `DeferredChildAllScenario::SOURCE`; its checksum is enforced by the
adapter. The retained historical Server PHP observation uses an older SDK and
is not an expected installed result for this runner. The five canonical cases
are single-child success, ordered results after completion order `[2, 0, 1]`, and
an early child failure, cancellation or termination. Two additional controls
exercise a retry and continue-as-new. Those controls preserve the initial child
call, link, instance and run tuple in its original schedule. The second run has
a distinct parent-child link. Subsequent events must use that run/link and the
original call, instance and group path. Retry records its predecessor in parent
history. Continuation proves its predecessor through both child histories; the
continue-as-new link is separate from the new parent-child link. These shapes
follow the [accepted native source return](https://github.com/durable-workflow/server/issues/325#issuecomment-6665208804).

The runner uses public `Client` and managed `Worker` APIs. It assigns a different
valid queue to each deferred child so one worker at a time can drive a fixed
completion order. Server's flat group contract permits independent member
queues. Each acknowledged turn exits, and the next turn starts in a new PHP
process without a shared replay cache. The replacement parent must return the
exact typed result or `ChildWorkflowFailed` shape without starting another child.
Duplicate parent and child completion attempts must be explicit no-ops or fenced
and leave complete canonical history unchanged. After an early failure closes
the parent under default abandon, successful siblings must add no parent history
or further ready task. The finite child ledger checks exact integer sequences,
workflow types, one start and at most one case-appropriate terminal per run.
Unknown child events, duplicate or conflicting terminals and events on closed
or replaced runs refuse, including malformed rows written by the first
completion. An early failure may leave a sibling started and unresolved under
abandon. Empty-queue observations require an explicit empty or
timeout poll with no refusal reason. Lifecycle or unavailable replies cannot
prove that the parent has no task. Both parent receipts bind the selected
workflow, run and queue.

For a separately admitted disposable tuple, the command is:

```sh
php scripts/qualify-deferred-child-all.php /consumer/vendor/autoload.php http://disposable-server:8080
```

An optional final case name selects one control. The server must use the
synthetic `claimed-task-admin`, `claimed-task-client` and `claimed-task-worker`
roles from the existing child lifecycle qualification setup. The runner makes
synthetic namespaces and retains them for inspection until the outer disposable
server is removed. It does not run against production or select credentials.
Only sanitized workflow identities, queue names, event types and synthetic
results reach stdout. Private turn receipts contain completion lease authority,
are created with mode 0600 and are removed together with private worker logs in
each turn's `finally` block. Authentication headers and registration tokens are
not retained. Each child process has a 35-second turn deadline and bounded
termination; the controller checks a 600-second total budget. The admitted outer
runner must also enforce its own absolute resource and cleanup limits.

This HTTP consumer adapter observes empty parent polls and two managed parent
completion turns. It cannot prove a database row count, competing native-node
serialization or cold database recovery. Server's cross-backend native tests own
those assertions. Started child handles inside `all`, nested, mixed, selected or
scoped groups and grouped conditions remain outside this adapter. The existing
`qualify-child-start-lifecycle.php` continues to own the separate sequential
started-handle and lifecycle policy cases.

Offline source controls run with:

```sh
vendor/bin/phpunit tests/DeferredChildAllAdapterTest.php
```

They use the actual SDK replayer and a managed worker with an inert transport,
including changed identity/path refusals and unrecorded receipt controls. They
make no network requests and do not invoke the runtime runner.
