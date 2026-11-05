# Static credential consumer fixture runner

This source work prepares the caller-owned check proposed in
[SDK #307](https://github.com/durable-workflow/sdk-php/issues/307#issuecomment-6667259865).
It has not solved or installed its dependency intent and is not an installed
package, HTTP authentication or Server result.

The shared `Support/StaticCredentialFixture.php` loader is also used by the
existing source regressions. It verifies the canonical fixture's complete
60,716 bytes, SHA-256 and 319 unique IDs before decoding its PHP inputs.
The fixture remains at `tests/fixtures/auth-configuration/static-credentials-v1.json`.
The source controls independently freeze all 296 scalar/byte original values,
including malformed UTF-8, with a length-delimited inventory computed using
Python's Unicode encoder and the original hex inputs. Server-only cases and
the binary-key boundary remain inventory entries, not PHP scalar passes.

`tests/fixtures/static-credential-consumer/composer.json` is the proposed
dependency intent. Its exact interpreter/package versions and known framework
source references are inputs to a later complete advisory-enforced graph;
there is no lockfile, installed subject or registry SDK substitution here.
Do not resolve it or install packages as a side effect of source controls.

The caller-owned runner is being authored to cover direct public credential
construction, both service configuration paths, Laravel's process resolver and
real provider/lazy-client/worker-factory paths, and Symfony's real bundle/client/
worker-factory paths. It will compare the fixture's original bytes and exact
setting-only refusals with a refusing transport spy and no worker loop. It will
not run a runtime smoke suite, acquire tools or manage a runtime/controller.

The offline loader/regression controls use an existing source vendor:

```sh
vendor/bin/phpunit tests/StaticCredentialFixtureTest.php tests/StaticCredentialValidationTest.php
```

For eventual installed execution, the caller must separately freeze the runner
and its data, obtain the normal published SDK identity, close the dependency
graph and seal all archives before package execution. Copy only the selected
runner/support/data files into the caller-owned packet; do not mount SDK source
or a source-vendor tree as the consumer. Actual Composer package references and
reflected implementation paths must bind the installed package. Synthetic
credential values remain internal to comparisons; only case/target/role,
verdict and setting names may be reported. No real credential or endpoint is
needed or selected by this source work.

The final combined-minor publication gates on #293 remain independent:
published Workflow support authority, published native flat-child-all and the
normally installed SDK tuple, and final combined candidate qualification still
precede freeze/tag. #219 remains excluded; the future major interface change
is separate.
