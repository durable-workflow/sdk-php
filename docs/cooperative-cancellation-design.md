# Cooperative cancellation source design

This document describes the unfinished source candidate for shared cancellation
issue 136. The default worker protocol remains 1.19. Cooperation requires an
explicit protocol 1.20 opt-in and a compatible Server and Native backend. The
specification and published mixed-language qualification are still pending.

## Recorded context

After the committed authored cancellation boundary, workflow code can inspect
`WorkflowContext::cancellationContext()`. The delivered `WorkflowCancelled`
exception carries the same immutable object in `context`. Before that boundary,
the workflow context returns `null`. Observing a request through polling or a
heartbeat does not expose future cancellation metadata to earlier workflow code.

The PHP Source context provides `deadline()` and `remaining()`. The deadline is
the accepted cleanup deadline, bounded by the original root deadline. A scoped
child may have an earlier authority ceiling. Remaining seconds use the committed
cancellation delivery time, then advance when cleanup resumes from a resolved blocking
operation. A synchronous side effect, version marker, metadata update or legacy
inline local callback does not move that clock when its result is later
persisted. Selection uses its committed winner marker, then the particular
handle being awaited. Later sibling outcomes and unrelated history do not
change an earlier authored decision.

The clock preserves fractional seconds, does not move backward with recorded
clock skew, and clamps an expired budget to zero. Calling `remaining()` outside
the active workflow Fiber, on a detached snapshot, or at a boundary without a
valid recorded timestamp raises `LogicException`. It never substitutes the host
clock. Runtime supervisors independently enforce the original real deadline
while callbacks or transport calls are running.

```php
try {
    $context->sleep(60);
} catch (WorkflowCancelled $cancelled) {
    $request = $cancelled->context;
    $context->cancellationShield(function () use ($context, $request): void {
        $context->activity('release-reservation', [
            $request?->rootRequestId,
            $request?->reason,
        ]);
    });
    throw $cancelled;
}
```

The recorded object provides local and root request IDs, root workflow instance
and run IDs, the immediate parent request ID, original reason, requester, source,
root request time, original cleanup deadline and ordered lineage. Requester
metadata is limited to caller type, ID and label. The timestamp helpers return
immutable dates. A child keeps the root time and budget even if its local request
is accepted later. Cold replay restores the same snapshot from canonical request
history. Explicit cancellation checks and shielded cleanup retain it.

Candidate context v2 carries `scopeOrigin`, an immutable
`ScopedCancellationContext` containing the original root context and every
scope address in order. Its `deadline()` is the originating scope's budget,
while `rootDeadline()` preserves the original global deadline. The child
context's own `deadline()` may be earlier when parent authority is narrower.
The immediate parent request identifies the last scope hop, including multiple
scopes within the same run. The parser verifies that the run lineage derives
from that complete tree and rejects changed metadata, repeated addresses,
reentry into an earlier run or a larger budget. Reading this metadata does
not enable scope execution. Both v1 and v2 contexts remain readable.

### Committed operation-scope replay

The internal Replayer candidate can replay a committed scope delivery at an
single activity, timer, condition or child call, including a previously admitted
call with a populated frozen inventory. Source tests opt in
with `replayCommittedCancellationScopes`; the Worker does not enable this path.
The parser requires the original authored tree, accepted request, v5 preparation
and matching delivery event before entering workflow code. It preserves the
original request identity, scope lineage, deadline and recorded clock. A narrower
authority ceiling never replaces that immutable context or grants new authority.

At delivery, `WorkflowCancelled::context` is a `ScopedCancellationContext`.
Its root identity, reason, requester and source are directly available alongside
`scopeId`, `rootScopeId`, `deadline()`, `rootDeadline()` and deterministic
`remaining()`. The same object is visible while that scope's body unwinds.
After leaving the scope, the parent retains its own cancellation state. A
retained context's clock advances only at consumed blocking history, including
an unaffected operation awaited by the parent.

The authored call must match the original admission, including membership,
operation kind, Activity and child policies, timer delay and condition descriptor.
Replay consumes that call's recorded position once and preserves earlier
completed operations. A committed condition delivery remains authoritative even
when the same predicate is now satisfied. Native scalar fixtures cover all
three Activity and child policies, a timer, and timed and untimed conditions.

This profile supports workflow-local cleanup and verified frozen member
projections, fully admitted all-mode groups and committed unshielded descendant
replay. Pending deliveries, scope conflicts and simultaneous root cancellation
are refused. A shield suppresses
explicit cancellation checks. With prepared local admission enabled, a shield
can author a sequential local cleanup call in that same delivered scope. Its
descriptor carries only the original scope, request and delivery IDs. The SDK
derives the expected receipt from canonical history, including the original
preparation ID and captured authority ceiling. It validates all eight receipt
fields before executing a callback. Callback leases, execution timeouts,
application heartbeats and supervisor requests cannot exceed that ceiling,
even when the immutable request deadline is later. Replacement replay keeps
the same proof and budget. The Worker still keeps this profile disabled.

### Frozen receipt inspection

The internal `CancellationScopeDeliveryClaim` coordinates scalar preparation
and delivery on one task, owner and attempt. Preparation proves canonical
history and preserves the selected context, boundary and any existing
preparation identity or authority ceiling. That prepared history must be
replayed before delivery accepts its exact boundary and preparation.

The original monotonic request budget is shared by mutation, reconciliation
and every history page. Its wall-clock mapping is captured once. An
acknowledged tighter preparation ceiling restricts that same budget before the
first history read, and cannot later be extended or renewed. An expired
preparation or a final fraction shorter than a whole transport second refuses
further requests. A historical read-only receipt can still be inspected
without granting execution authority. This coordination uses Native fixture
shapes with explicitly synthetic current timestamps. It neither enables the
Worker profile nor demonstrates physical scoped callback supervision.

The internal claim client can verify populated Activity, timer, wait and child
receipts against complete canonical history. Each projection comes from the
prefix before the original preparation. Timers retain their original fire time
and wait-timeout links. Waits retain their signal or condition descriptor.
Children retain the latest start recorded before preparation, their original
call and instance, and their cancellation policy. Later continuations and
admissions cannot change a verified receipt.

The preparation also freezes the unshielded subtree in opening order. Every
descendant retains its accepted request, propagation event, context, operations
and original authority ceiling. An independently accepted descendant request
requires its recorded competing-root conflict and keeps its earlier deadline.
Shielded scopes and their descendants remain outside that subtree. Replacement
claims must preserve all previously verified facts.

The operation projection fixtures use Native's scalar producers, including
nested group metadata, against an isolated SQLite history store. They exercise
receipt parsing and canonical history verification. A verified projection or
delivery marker does not prove physical callback stop or authorize dispatch.
The Worker still refuses populated scope execution until selective supervision,
cleanup and connected replacement behavior are qualified.

Other durable commands in a delivered scope remain unsupported. Remote
activities, timers, children, continuations, metadata writes, side effects,
new scopes and remote descendant cleanup require a separate cleanup authority contract.
Canonical preparation/delivery on the live claim,
and connected replacement qualification remain the next
steps before enabling Worker scope execution.

The parser rejects mismatched local runs, request identities, budgets, invalid
lineage and a delivery snapshot that changes the accepted context. Heartbeat
observations retain the opaque history refresh route while canonical history
supplies the workflow-facing object. Older cancellation histories without the
rich snapshot continue delivering cancellation with `context === null`.

## Child policies

Child authoring accepts `CancellationPolicy` and `ParentClosePolicy` values from
`DurableWorkflow\Worker`, or their portable strings, through both ordinary and
deferred calls.

```php
$context->childWorkflow('python.child', [], [
    'cancellation_policy' => CancellationPolicy::WaitCancellationCompleted,
    'parent_close_policy' => ParentClosePolicy::RequestCancellation,
]);
```

`TryCancel` requests child cleanup and delivers parent cancellation without
waiting. `WaitCancellationCompleted` parks the parent until the child has a
recorded terminal outcome, releasing the task claim so the worker can run other
work. `Abandon` leaves the child independent and preserves the historical
default. These operation policies govern cancellation at the awaiting call.

Parent closure is a separate choice. `RequestCancellation` requests genuine
cooperative cleanup with the original lineage and deadline. A normally closed
parent starts one shared bounded budget. `RequestCancel` retains its legacy
terminal behavior. `Terminate` and `Abandon` retain their existing meanings.

The worker compares both policies with recorded child history during cold
replay, including parallel and selection calls and cancellation delivery.
Omitted historical fields mean the original `Abandon` defaults. Later start or
terminal events that omit fields retain the scheduled snapshot. Changing a
policy after it was recorded fails replay. Invalid or conflicting history also
fails explicitly. Cooperative choices require the explicit source opt-in and
compatible backend described above. A worker without that opt-in refuses the
commands before completion with `child_cancellation_policy_not_supported`, its
worker identity and the required protocol. Server also checks the immutable
claim capability and installed backend before accepting these policies.

## Remote Activity policies

Ordinary and deferred remote Activities accept the same `CancellationPolicy`
enum or its portable string value. Omitted policy retains the historical
`TryCancel` behavior and does not add a field to the command.

```php
$context->activity('rust.remote-work', [], [
    'cancellation_policy' => CancellationPolicy::WaitCancellationCompleted,
]);
```

`TryCancel` requests cancellation and continues without waiting for the stop
receipt. `WaitCancellationCompleted` delays delivery to workflow code until the
original remote attempt's physical stop acknowledgment is recorded. `Abandon`
keeps the Activity independent after parent cancellation. It requires a finite
positive integer `schedule_to_close_timeout`, whose original deadline bounds
the independent work. It does not extend or reuse the parent's cleanup budget.

Every explicit remote policy requires cooperative worker opt-in, protocol 1.20
and a compatible installed backend. Unsupported workers identify the operation,
worker identity and required protocol before submitting the command. Server
checks the original immutable claim as well. Local Activity policy authoring
remains unavailable while its lifetime and admission contract are unfinished.

Replay compares the authored policy with the original canonical Activity
history, including cancellation delivery, parallel and selection matching.
Later events that omit the policy retain the original snapshot. Unknown,
conflicting or changed policies fail replay explicitly. Historical histories
without a policy retain `TryCancel`.

The connected Source suite exercises explicit Try and Wait without application
heartbeats, physical callback stop and stale-result refusal. Wait also checks
that the durable stop receipt precedes cancellation delivery. The bounded
Abandon case checks a callback still running after parent closure, its durable
completion under the original total timeout, unchanged parent cancellation and
rejection of another outcome from the completed attempt. These cases qualify
the Source tuple rather than the published mixed-language release gate.

## Connected child-policy qualification

The connected integration suite includes one workflow worker serving a parent
and its child on the same queue. With `WaitCancellationCompleted`, the parent
releases its task claim, the child receives a genuine cooperative request and
finishes shielded cleanup, and the parent resumes on a successor claim at the
same authored boundary. The second case kills the only workflow worker after
the released reply and starts a fresh replacement. Both cases assert one root
identity, distinct local request IDs, original metadata and deadline, duplicate
request identity, completed cleanup and terminal cancellation before the
original 30-second deadline. This replacement point is before child cleanup.
The published mixed-language gate still requires SIGKILL during cleanup.

Run CI's connected source qualification with `cooperative_qualification=true`,
an exact `server_commit` and an exact `native_commit`. The optional Native input
enables `DURABLE_WORKFLOW_CHILD_POLICY_QUALIFICATION=1` and a readonly runtime
source overlay. It does not change the image's published Composer authority or
qualify a published Native artifact. Without the input, the existing connected
lane uses the Server's pinned backend and skips only these candidate child cases.
Actions retains source provenance, scenario histories and JUnit results.

## Remote callback-stop receipts

The PHP process supervisor reports a remote callback stop only after its callback
process is gone and its relay has been joined. A failed stop confirmation refuses
publication. Cancellation observation remains independent of application
heartbeats. After joining, the worker reads the original claim's canonical
cancellation receipt and calls `Client::acknowledgeActivityCancellation()` with
that attempt, owner and local request ID. The Server retains the root identity
and original deadline. A duplicate returns the original receipt.

An expired lease, lost connection, worker shutdown, terminal legacy cancellation
or malformed observation cannot supply a cooperative stop receipt. A callback
that never started is not reported as joined. Receipt discovery and reporting
share one five-second monotonic budget. Known transient connection and upstream
failures retry at most three times per request, retaining the captured attempt,
owner and request identity. Reporting is also capped by the original cleanup
deadline. A lost accepted reply returns the original receipt on retry. A refused
proof or exhausted budget produces a diagnostic and does not regain result
authority. Stop acknowledgment describes
the supervised callback, not reversal of external side effects. Other processes
and downstream systems need their own cooperating cancellation and fencing.

Joined callbacks emit a distinct diagnostic when no cooperative cancellation
was observed. A changed claim identity or malformed cancellation proof emits
`worker.activity_cancellation_acknowledgement_failed`. These diagnostics carry
`callback_stopped: true` and the original task and attempt IDs. They distinguish
physical process stop from a retained stop acknowledgement without publishing
results or manufacturing cancellation authority.

The source-bound connected lane checks the receipt for blocked callbacks with
and without application heartbeats, live and replacement workflow workers,
duplicate acknowledgment and stale completion/failure refusal. Local callback
receipts and durable activity waiting policies still need their separate
workflow claim authority and qualification.

## Prepared local callback execution

The explicit source candidate suspends prepared local calls before invoking
application code. A shielded call after canonical cancellation delivery carries
only the accepted local request ID and delivery history event ID in its
preparation descriptor. The SDK compares the returned cleanup snapshot with the
canonical root ID and original deadline. Poll observations, missing delivery
identity, legacy context and unshielded calls cannot grant cleanup authority.

Cold replay returns the same proof and authored sequence. It requests recovery
for an unresolved started attempt and preparation after a recorded retry.
Committed prefix results replay before admission, including cleanup side effects.
Runtime admission, retry and recovery remain responsible for fencing attempts.

Supervisor control renews the activity and hosting workflow leases without
recording an application heartbeat. An actual application heartbeat has its own
endpoint and canonical receipt. Only a fully validated acknowledgment advances
the SDK's heartbeat deadline. Start, total and original cleanup deadlines remain
fixed. A heartbeat cannot revive an elapsed attempt or exceed the original root
budget. Rejected replies leave the previous acknowledged deadline intact.
`ActivityContext::heartbeat()` carries application details in Native's bounded
progress-details object, using the same shape as remote activity heartbeats.

Prepared argument, recovery, checkpoint and result uploads can use a single
draining fallback after an explicitly unadmitted response. The fallback requires
the Server's advertised prepared completion schema and explicit protocol 1.20.
It preserves the original workflow claim, operation identity and payload bytes.
Every prepared operation shares the same claim allowance with ordinary workflow
completion. The Server checks live authority and fixed deadlines without
renewing leases, recording application heartbeats or extending cleanup time.

The managed worker exposes an additional source opt-in,
`enablePreparedLocalActivities: true`, which requires
`enableCooperativeCancellation: true` and explicit protocol 1.20. Registration
requires the Server's installed prepared-local capability. The worker checkpoints
sequential prefix commands, reloads canonical history on the original claim,
then admits the callback before forking it. It publishes only the backend
attempt's canonical outcome. A recorded retry releases the claim to Native's
durable scheduler instead of sleeping or retrying application code inline.

An unresolved started callback goes through recovery with an unknown stop
state. A lost outcome acknowledgment never reruns the callback in that claim.
Lease control and application heartbeats retain the original backend attempt,
owner and workflow claim epoch. The callback supervisor reports cancellation
stop only after joining the physical process, before replaying cleanup.

Server timestamps establish a conservative monotonic authority budget shared
by control, worker keepalive, payload transfers and result publication. Fixed
execution and cleanup deadlines cannot move when leases renew. With whole-second
transport timeouts, the worker stops admitting I/O in the final fraction of a
second instead of rounding past its authority deadline.

The Source candidate also admits complete `all()` groups containing local
activities through `prepared_local_activity_groups`. Atomic admission records
every member before callbacks start, with a maximum of 100 members. Concurrent
supervision checkpoints each result only after joining its callback, while
maintaining the original shared claim and deadline. Selection groups and groups
containing turn-closing waits still refuse before callbacks start. Connected
Source qualification covers physical stop and cleanup SIGKILL recovery for the
PHP-parent, Python-child, Rust-activity and PHP-local-activity cascade. The default
protocol remains 1.19.

## Remaining qualification

The internal scope transport also admits Server preparation and delivery
controls. They retain the original claim owner, epoch, scope request and authored
call range. Both require a caller-supplied shared bounded request budget. The
transport rejects supplied deadlines, borrowed preparation IDs and malformed
call ranges. An uncertain reply is returned as an error without retrying or
claiming delivery. Its raw response is not callback authority. Canonical
preparation/delivery receipt validation and live recovery qualification must
precede general Worker scope execution. The bounded scalar source profile below
has an explicit opt-in and remains disabled by default.

Prepared local Activities can author `CancellationPolicy::TryCancel` or
`CancellationPolicy::WaitCancellationCompleted` in `cancellation_policy` when
the negotiated protocol 1.20 worker enables prepared local execution and runtime
discovery lists that policy. Registration advertises
`prepared_local_activity_cancellation_policies` for the negotiated policy path.
The original issued claim and installed backend must support the policy.
Descriptors preserve it through preparation, recovery and atomic groups.
Cold replay refuses a changed policy. Omission preserves historical TryCancel.
Legacy inline execution, local Abandon, explicit null and an undiscovered
policy are refused before callback execution. An unsupported group starts no
callbacks and sends no checkpoint writes.

TryCancel fences publication and releases the durable await without claiming
physical stop. WaitCancellationCompleted requires the original owner's joined
stop receipt or canonical callback outcome before delivery can proceed. Both
retain the original root identity and immutable deadline. Supervisors observe
cancellation independently of application heartbeats.

Connected Source qualification covers these local policies through cleanup
SIGKILL, worker replacement, the same delivery boundary and the original
30-second deadline. The mixed cascade authors local Wait and qualifies
PHP/Python/Rust remaining-time helpers. Nested scopes, the Python/Rust local
policy consumers and exact published artifacts remain required. The runtime
continues enforcing the original deadline and fencing task and activity ownership.

Connected qualification must cover a PHP parent, Python child, Rust remote
activity and PHP local activity, including callback stop without application
heartbeats and SIGKILL during cleanup. A replacement must replay the same boundary
and finish cleanup before the original 30-second deadline. Supported workflow
lease, heartbeat and repair settings must be recorded with that test. The exact
published artifacts and one cascade inspection view are required before release
claims.

## Canonical scope boundary receipt source profile

The internal client can prove preparation and delivery for a single call with
an empty projection or frozen Activities, timers, waits, children and descendants.
It derives the Activity IDs and
cancellation descriptor hashes from the original pre-preparation history,
preserving policy, local execution mode and schedule-to-close deadline. A sibling
cannot enter that inventory, and response loss or replacement cannot substitute
new members. This proves admission facts, not callback stop or cleanup authority.
Both phases and complete history paging
share the caller's original bounded request budget. Delivery requires a
previously verified preparation and retains its request, lineage, authored
range, preparation event and captured authority ceiling. One transient lost
reply may reconcile the same mutation. A pending stop, explicit refusal,
invalid receipt, incomplete history or exhausted budget aborts the claim.

The history parser can inspect committed preparation before delivery, while
ordinary cleanup replay still requires committed delivery. Receipt facts do
not grant callback authority. Worker scope execution stays disabled by default and the
candidate protocol remains unfrozen and unpublished. Committed single-call
projection replay and fully admitted `all()` groups have internal source profiles.
The group profile preserves flat and nested Activity, timer, child and condition
members, their original paths and cancellation policies, the complete authored
range and the existing cleanup receipt. A committed delivery wins even when a
condition is now satisfied. Replayed cleanup retains the original request,
lineage, preparation/delivery events and narrower authority deadline. Returning
from the cancelled scope leaves its parent unaffected.

The group profile requires every original member to have been admitted before
preparation. Partial groups, local or selection groups and direct signal-wait
groups remain refused before workflow entry. PHP currently reads signals through
conditions rather than authoring `SignalWaitOpened` calls. Committed descendant
replay uses one ancestor marker for its original frozen unshielded subtree.
Delivery into the active descendant preserves its own accepted request and
lineage. Ancestors retain their own context while the body unwinds, and the
outer scope remains unaffected. Prepared local cleanup in any included scope
uses that same preparation/delivery pair and its captured narrower authority
ceiling. Replacement replay preserves all eight cleanup receipt fields.
Shielded branches are excluded. Forged members, altered ancestry, deadlines,
propagation events, omitted operations and changed authored calls fail before
cleanup. Competing roots and overlapping subtree deliveries remain refused.

The internal pending-request profile selects a scalar call from canonical scope
history without exposing cancellation to workflow cleanup. It preserves results
committed before the original request and lets that request win over later
results. A descendant selects its accepted original ancestor request without
crossing a shield or borrowing an independent root. An existing preparation
retains its exact call, event identity and narrower authority ceiling, including
on replacement replay. Changed or skipped prepared boundaries fail explicitly.
Only a committed delivery exposes its context and resumes cleanup. The profile
emits a selection intent rather than dispatching effects. Pending local,
selected and group delivery remain refused.

## Managed Worker scalar scope source profile

`enableCancellationScopes: true` explicitly enables scalar preparation/delivery
and committed scope replay in a protocol 1.20 cooperative Worker. It is an
unfrozen source profile, defaults to false, and does not advertise complete
portable cancellation-scope capability. Ordinary cooperative workers continue
refusing scope requests and deliveries.

The managed claim loop retains one original task, owner, attempt, selected
request and bounded request budget. Any retained prefix checkpoint uses that
same budget. The Worker proves preparation, replays its canonical history,
then delivers the original frozen boundary and replays matching canonical
delivery before workflow cleanup can observe the request. Reply loss preserves
the same mutation, preparation identity and authority ceiling. An exhausted
budget, missing history or changed authority aborts the claim. Replacement
workers preserve an existing preparation or replay an existing delivery.

The same source opt-in composes a run request with authored scopes. Its accepted
run context is the canonical parent of inherited scope requests. Scoped delivery
and shielded prepared cleanup replay before root delivery, then root cleanup can
run after leaving the scope. Both retain the original root identity and deadline.
Root delivery checks the active scope and each operation's recorded or deferred
membership. A scoped leaf cannot become a root operation by returning its handle
or awaiting its group outside the scope. A committed root boundary over scoped
work fails before another scope intent or cleanup can execute.

Pending local, selected and group boundaries and overlapping delivered subtrees
remain explicitly unsupported. Live selective physical supervision, connected
scoped cleanup recovery, all portable consumers and exact published-artifact
qualification remain required for general scope execution and release claims.
