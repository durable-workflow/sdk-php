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

## Remaining qualification

Portable child and activity operation policies, nested scopes and deterministic
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
