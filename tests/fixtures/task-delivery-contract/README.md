# Task delivery source contract

`offer-accept.v1.json` is byte-identical to Server commit
`955da4e870ef76e7df1bcd24beaae037df63a8f4`, path
`tests/Fixtures/contracts/task-delivery-offers/offer-accept.v1.json`:

- Git blob: `92df9dd2243521a509bfc8b6ce61a7be16711060`
- SHA-256: `cac2bc6852a09db9526c623c3683772ebbba7d3f1d0b401fb92ae2026cd32b11`
- Size: 18,142 bytes; 49 scenario IDs.

The source contract is reviewed in Server #1033 for the unimplemented native
#931 capability. Its status remains draft contract data, without an executor
or qualification result. `WorkerTaskDeliveryKindsTest` drives the real PHP
Client/delivery state through an in-memory transport for the five `kind-*`
rows only. Supplementary PHP controls cover malformed kind sets, immutable
selection, clones and uncertainty. They do not execute the other native
claim/receipt, seal/key, database, proxy, restart or capacity scenarios. Reading
or enumerating those rows supplies no execution result. Tokens are synthetic
opaque test text, not sealed native packets or real credentials.

`acceptance-refusals.v1.json` is byte-identical to Server commit
`31b281ac3891143eae9c6c5ace9c65b63e404e17`, tree
`d0d1cf302f2076c3307d8bc71c8196d6b6d3c062`, path
`tests/Fixtures/contracts/task-delivery-offers/acceptance-refusals.v1.json`:

- SHA-256: `64a936382d4ce829d0e1de50e75247223711bf4011c830b257143b0a56c237ae`
- Size: 74,169 bytes; 175 unique IDs: 117 decoded wire, 34 raw wire and
  24 authority scenarios.
- Exact companion source review:
  https://github.com/durable-workflow/server/issues/931#issuecomment-6659734780

`WorkerTaskDeliveryWireTest` sends the 117 decoded wire cases through the actual
PHP progress-envelope transport and the 34 original-byte cases through both
ordinary and progress-envelope responses. Each runs for workflow, mixed-kind
workflow selecting update validation, activity, query and dedicated update
validation. Exception-context cases preserve each present field; fields that
cannot be represented by PHP's strict exception constructor fail as unreadable,
without coercion. Invalid UTF-8 hex is decoded byte-for-byte before transport.
Supplementary client-state controls cover clones, pending-request isolation and
replacement incarnations. These are in-memory PHP source tests. The 24 Server
authority/receipt/race scenarios are inventoried only; neither those requirements
nor native transactions, runtime behavior or installed packages are qualified.
