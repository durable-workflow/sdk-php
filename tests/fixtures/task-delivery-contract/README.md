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
