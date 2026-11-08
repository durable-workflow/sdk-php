# Installed native condition-wait qualifier (#269)

`qualify-condition-wait-native.php` checks condition-wait behavior of a normally
installed SDK against a disposable native Server. It is a qualification script,
not SDK runtime code, and it uses only the public `Client` and `Worker` APIs.

```sh
php vendor/durable-workflow/sdk/scripts/qualify-condition-wait-native.php \
  vendor/autoload.php http://127.0.0.1:8080
```

Use only a token-auth runtime that exists for one run, with the synthetic
fixture credentials `claimed-task-admin`, `claimed-task-client` and
`claimed-task-worker`, no host port and no production route. The script refuses
unless the Server advertises `condition_wait_idempotent_reopen`. Its worker declares
every signal each workflow type receives (`ConditionWaitNativeEvidence::DECLARED_SIGNALS`),
each with a signature matching the one argument it sends (`string`, or `int` for
`decision`). Native refuses an undeclared signal and validates arguments against the
declared parameters. The first run starts only after the worker is active. Right
after each case's initial start, the script reads that run's observation and requires
`command_contract.source == durable_history` plus exactly the expected
`signal_contracts` parameter metadata (`ConditionWaitNativeEvidence::durableContract`).
Continue-as-new successors carry no contract in Native 3.2.0, so for those the script
only records the observed source (`successor_contract_sources`). A refused signal
reports only the Server's status, reason, validation-error shape and scalar
`signal_admission` fields (at most 600 bytes).

## Cases and history assertions

`Support/ConditionWaitNativeEvidence.php` holds the assertions as pure functions
over one run's history, using native Server 3.2.0 event shapes: each open is a
new `condition_wait_id`, a condition timeout is a `TimerScheduled` with
`timer_kind: condition_timeout`, and a timeout ends with `TimerFired` (native
records no `ConditionWaitTimedOut`).

| Case | Workflow | Assertion |
| --- | --- | --- |
| C1 | `waitCondition` with a 20 s timeout and a false wake after about 5 s | The reopen carries the remaining budget (within 1 s) and its timer fires at the first open plus 20 s (within 2 s). |
| C2 | An unrelated signal during an activity queues a second workflow task before the wait opens | No second open overlaps an unresolved open of the same key; every wait is satisfied or timed out. |
| C3 | Terminate or cancel a run with an open wait; continue as new after a fired timeout | Terminate and cancel record `TimerCancelled` for the open condition timer; the successor run has no inherited wait. |
| C4 | Wait A is satisfied, then the workflow authors wait B and B gets a false wake | B reopens with its own remaining budget, not A's. |
| C5 | Decision turns with false wakes, across continue-as-new | Each run settles as in C2. |

The script prints one JSON document with every case's run IDs, event types and
evidence, plus `run_ids`, `c1_run_ids` and `c1_timeout` for the physical check.

## Physical state (Server-owned)

Database state is checked by Server's read-only
`condition-wait-physical-check.sql` (SHA-256
`a162896dfca506eea79eeb00745b5fb27e209436d0ec3c6ce6d69580a3fe93a3`, from
[sdk-php#269](https://github.com/durable-workflow/sdk-php/issues/269#issuecomment-6715131069)).
The caller runs it in the cell's MySQL database as a SELECT-only account with
`@run_ids`, `@c1_run_ids` and `@c1_timeout` taken from this script's output. No
result rows means pass. The SQL is not part of this package.

## Controls

`tests/ConditionWaitNativeEvidenceTest.php` covers each assertion with a passing
native history and a refused one: the original full-timeout reopen and moved
deadline, an overlapping duplicate open, a satisfied wait that keeps its timer,
a terminate without timer cancellation, and a new wait that inherits an earlier
budget.
