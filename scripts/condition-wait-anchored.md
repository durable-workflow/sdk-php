# Installed anchored condition-wait qualifier (#269)

`qualify-condition-wait-anchored.php` checks the occurrence-deadline (anchored) mode of a
normally installed SDK against a disposable native Server that advertises
`condition_wait_occurrence_deadline` (native Server 3.3.0 or newer). It is a
qualification script, not SDK runtime code.

```sh
php vendor/durable-workflow/sdk/scripts/qualify-condition-wait-anchored.php \
  vendor/autoload.php http://127.0.0.1:8080
```

Use only a token-auth runtime that exists for one run, with the synthetic fixture
credentials `claimed-task-admin`, `claimed-task-client` and `claimed-task-worker`, no
host port and no production route. Without the capability the script refuses (exit 2)
before creating anything; the legacy cells stay with `qualify-condition-wait-native.php`.

## Scope and witness

- **Fresh Native runs only.** Each case creates its own namespace and queue, starts its
  run itself, and binds the run's Native history export through
  `ConditionWaitAnchoredEvidence::freshNativeHistory($export, $startedRunId)`.
  The export preserves the `StartAccepted` / `WorkflowStarted` pair and its shared
  start-command identity. All history sequences and recorded event times are checked.
- **One worker per queue.** Each case starts exactly one worker process and waits until it
  is the only active worker on that queue, so no unobserved worker can author a recorded
  open.
- **Witness.** The worker's `@internal` delivered-history observer reports one `history`
  entry per replay pass, each `completion`, and each `accepted` completion. Every report is
  appended to a private witness file (no payloads, arguments or tokens are kept).
- **Assertions.** `ConditionWaitAnchoredEvidence::falseWake()` and `ownBudget()` in
  replay-input-only mode: the exact stored anchor A on every open and timer, the original
  budget, never-early firing within the existing 2 s dispatch bound, each recorded open
  bound to its accepted completion through the recorded `workflow_task_id`, completion 0
  authoring the full budget, and each reopen's `timeout_seconds` equal to the Replayer's
  remaining time computed from exactly that attempt's replay input.

| Case | Workflow | Driver |
| --- | --- | --- |
| C1 | `cwn.false_wake` (20 s) | one false wake about 5 s after the first open; times out at A |
| C1-repeat | `cwn.false_wake` (20 s) | two false wakes about 5 s apart; times out at A |
| C4 | `cwn.own_budget` | wait A satisfied; wait B (20 s) gets a false wake, then the matching signal |

**Output.** stdout carries only the final JSON document: each case's namespace, queue,
run ID, witness size, history size and evidence. Progress lines (`ok: ...`) go to stderr.
The private temporary evidence directory keeps each raw Native export before validation
and the corresponding worker witness. Its path is included in successful JSON or the
bounded refusal line. A disposable-container controller should retain this directory
outside the container, with a file-size bound, so failed assertions remain inspectable.
On a refused assertion the script writes the case and the helper's refusal code, then one
`refused: ...` line, each at most 300 bytes, to stderr and exits 1; it does not end with an
unhandled PHP fatal. The worker processes' own stdout and stderr are routed to stderr and
are not capped by the script, so a cell controller must bound the streams it retains.
Exit codes: 0 pass, 1 refused or failed, 2 capability absent, 64 usage.
