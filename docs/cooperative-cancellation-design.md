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
