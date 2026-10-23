# Durable child launch handles

Status: proposed product contract for [PHP SDK #185](https://github.com/durable-workflow/sdk-php/issues/185).
This is not an available SDK API or a qualified runtime capability. The runtime
dependency and SDK implementation must be coordinated before publication.

## User outcome

A workflow can start a child, obtain the identity of the committed child, do
other work, then await that child's eventual result. It needs no unrelated
activity, timer, selection winner, control signal or visibility request to make
the parent continue after the child starts.

`WorkflowContext::childWorkflow()` continues to return the eventual result.
`deferChildWorkflow()` continues to prepare a command for a group. Existing
selection handles keep their parent workflow/run properties. None of these
objects is silently reinterpreted as a started child handle.

## Proposed authoring surface

```php
// Proposed API. Not currently supported by published SDKs/runtimes.
$child = $context->startChildWorkflow('billing.reconcile', [$account], [
    'parent_close_policy' => ParentClosePolicy::Abandon,
]);

// Returning here means the runtime committed the start and issued a fresh
// parent task. These are canonical child identities, not prepared options.
$identity = $child->identity();
$context->activity('notify.child-started', [$identity->workflowId]);
$result = $child->await();
```

The names are proposed PHP API names, not a new portable wire schema. The
runtime contract must be agreed independently of this language surface.

The initial supported surface is committed identity and awaiting the result.
Preparing a group member remains distinct from starting it. If a prepared-child
type is exposed later, its unresolved state is explicit and it cannot advertise
a child workflow/run ID before canonical scheduling exists.

## Identity and replay

The handle binds to all of:

| Identity | Meaning |
| --- | --- |
| Parent workflow ID and parent run ID | The authoring execution and authority scope |
| Parent operation sequence | The original child-call slot in that execution |
| `child_call_id` | The durable parent-issued child call |
| Child workflow instance ID | The generated child instance, stable through its run chain |
| Initial child run ID | The first concrete run recorded for this child call |

These are opaque values. Preserve their original spelling and case. Do not use
`workflow_link_id` as the stable child-call identity: the published continuation
probe shows that the link changes while `child_call_id` remains stable.

Resolve the initial identity from the original canonical scheduling/start
records. Late or cold replay must return the same initial identity, even when
the loaded history already contains a continued run or terminal result. Never
use the latest run in the entire history as an unrecorded mutable getter that
can change a workflow's earlier decisions during replay.

Awaiting follows the original child call's canonical resolution through child
continuation and retries. It does not start another child or consume another
authoring sequence. If terminal run information is exposed, it is an explicit
resolution snapshot, separate from the immutable initial run ID. Conflicting,
missing or mismatched committed identity is a typed refusal, not a guessed ID.

Cold replacement and duplicate delivery reconstruct the handle from history.
They require no process-local cache, visibility query or external side effect.
A handle from another parent execution, namespace or parent run cannot be used
as though it belonged to the current Fiber.

## Required runtime dependency

The published baseline records the child start but provides no new parent task
in the observed pre-completion polls. Completing the original claim closes
that claim. Reusing it for later commands is refused. Child completion provides
the positive control: a fresh parent task then arrives and the normal cold
replayer recovers the continued child's typed result.

The coordinated runtime implementation must provide a genuine parent turn
after committing a requested child start, while the child can remain open.
The new turn must carry committed identity/history and a fresh task claim.
Duplicate acknowledgments or redelivery must not start another child. SDK code
must not resume durable authoring on the closed claim merely because its HTTP
completion acknowledgment arrived.

An advertised capability must distinguish this behavior from the legacy
result-waiting child command. An unsupported runtime must refuse the new
authoring mode before a child is submitted. An unknown option that is silently
discarded by an older normalizer is not negotiation. The wire field, capability
name, history/cursor guarantees and supported artifact baseline remain part of
the coordinated runtime work, not an SDK-local convention.

Do not synthesize the turn with a polling loop, timer, marker, signal, unrelated
operation or direct HTTP query from workflow code. Legacy child behavior and
its ordinary task cost remain unchanged unless this capability is selected.

## Lifecycle and supported operations

Preserve existing parent-close and child cancellation policies, including
Abandon. Merely dropping a handle does not create cancellation. Cooperative
requests retain the original lineage, request identity, deadline and stale
attempt fences. Obtaining or awaiting a handle cannot renew those budgets.

Child signals, queries, updates, termination and explicit cancellation need
their own qualified durable, child-call-scoped authoring contracts. They are
unsupported by this initial surface and must be diagnosed before a side effect
or command is emitted. A general client handle is not a safe substitute for
such a contract. Never silently target an unrelated replacement run.

Caller-selected child IDs remain [Workflow #850](https://github.com/durable-workflow/workflow/issues/850).
The default remains an engine-generated ID. This API must not quietly accept
arbitrary ID options as if they worked.

Ownership and still-open operations at parent continue-as-new remain
[Workflow #868](https://github.com/durable-workflow/workflow/issues/868). Do not
invent a PHP-only ownership transfer. Parent-run reuse must refuse until the
coordinated policy establishes the safe behavior. Parent rollover acceptance
cannot be claimed from the child-continuation probe.

## Qualification before delivery

Use the existing published-artifact probe to preserve the baseline. The feature
then needs focused SDK tests and normal installed acceptance for:

- Prepared versus committed state and a real start acknowledgment without an
  extra operation or visibility request.
- Exact call/instance/initial-run identities on cold replay and duplicate
  parent task delivery, with one canonical child start.
- Typed success and failure, child continuation and retry chains, and explicit
  initial versus terminal run targeting.
- Namespace isolation, wrong-parent and stale-claim refusal, parent-close
  policies, cancellation authority/deadlines and the agreed parent rollover
  behavior.
- Compatible sequential, deferred, group and selection APIs.
- A complete runnable service-mode start/work/await example, with unsupported
  operations and lifecycle behavior stated plainly.

The current probe is `scripts/probe-child-start-contract.php AUTOLOAD URL`.
It uses only the documented disposable token-auth runtime, normal installed SDK
autoloading, capacity heartbeats and bounded raw polls. It records canonical
identities, original claims, duplicate refusal, cold pending replay and child
continuation/resolution. Generated receipts belong on the owning issue, not in
source control. A passing baseline probe does not implement this feature or
establish parity with another SDK or runtime.
