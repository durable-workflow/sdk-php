# Static credential fixture provenance

`static-credentials-v1.json` is the byte-exact approved shared fixture from
[Server commit 23972b1d12fcaa64788480eda0a31de7a5347422](https://github.com/durable-workflow/server/blob/23972b1d12fcaa64788480eda0a31de7a5347422/tests/Fixtures/AuthConfiguration/static-credentials-v1.json),
Git blob `d27da5be4fff5d9ebabaf6da029b2b1b45ba23f3`, normally integrated by
[Server #1032](https://github.com/durable-workflow/server/pull/1032).

The frozen file is 60,716 bytes including its trailing newline, has 319 unique
case IDs, and has SHA-256
`0d4f01b763d72850943acbd9a74a0cc5da1ca6db5bc1b6fa5ca37fda11810acb`.
Its source-adoption approval is [Security's exact-row freeze](https://github.com/durable-workflow/server/issues/554#issuecomment-6646233914).
Do not reformat or replace its values with a PHP-specific interpretation.

The PHP adapter executes the common scalar cases, PHP malformed-byte cases,
SDK preselection cases and PHP adapter cases through the public constructors
and framework credential loaders with synthetic inputs. Server-only settings
and decoded binary signing keys are outside the PHP token surface. OS
variable values cannot contain NUL, so those scalar cases are covered by direct
Client/TokenAuthentication/ServiceConfiguration strings rather than attempted
process-environment setup. Positive selection compares exact in-memory bytes;
it is not an HTTP-header or authentication-success claim.
