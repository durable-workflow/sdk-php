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
that never started is not reported as joined. A refused or lost receipt produces
a diagnostic and does not regain result authority. Stop acknowledgment describes
the supervised callback, not reversal of external side effects. Other processes
and downstream systems need their own cooperating cancellation and fencing.

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

Parallel groups containing local activities currently return
`prepared_local_parallel_admission_unavailable` before callbacks start. The
installed sequential contract cannot atomically admit that group. Completing
parallel admission and connected physical-stop and cleanup SIGKILL qualification
is required for the full PHP-parent, Python-child and Rust-activity scenario.
The default protocol remains 1.19.

## Remaining qualification

Portable activity operation policies, nested scopes and deterministic
remaining-time helpers still need completion. Remaining time must use the
replayed workflow clock. Do not subtract the host clock from the deadline in
workflow code. The runtime continues enforcing the original deadline and fences
task and activity ownership.

Connected qualification must cover a PHP parent, Python child, Rust remote
activity and PHP local activity, including callback stop without application
heartbeats and SIGKILL during cleanup. A replacement must replay the same boundary
and finish cleanup before the original 30-second deadline. Supported workflow
lease, heartbeat and repair settings must be recorded with that test. The exact
published artifacts and one cascade inspection view are required before release
claims.
