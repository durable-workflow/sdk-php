# Avro Value benchmark

`composer benchmark-avro-value` loads the checked-in medium corpus at
`resources/protocol/avro-value-benchmark-v1.json` (SHA-256
`588771404977f2a95fe7d8969c24a15e1c7dd78fe498af9aa2406f82be54b666`).
The bytes sentinel is adapted only for the fixed typed path; compact JSON and
the removed wrapper use the corpus's documented JSON representation.

A release qualification run in the Linux PHP 8.4 worker measured about 269 µs
to encode and 148 µs to decode. The enforced 450/250 µs defaults allow roughly
1.7x shared-runner variance while still detecting a material regression. Set
`AVRO_VALUE_ENCODE_BUDGET_US` and `AVRO_VALUE_DECODE_BUDGET_US` to calibrate a
different qualification runner.
