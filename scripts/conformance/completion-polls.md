# Installed completion-poll consumer

This is caller-owned source preparation for SDK #328–#332. It has not run
against a published package or Native image. Source controls do not qualify
Server admission, an installed upgrade, artifact publication or performance.

The caller separately selects and verifies the immutable published Server
index/container, normally installed SDK archive and sealed dependency graph,
and maintained runner files. Native 3.2.0 preparation is recorded in SDK
#293; no SDK version, image publication or invocation follows from this runner.
The template has unselected identities and refuses before consumer autoload.

The program does not install packages, fetch tools, start Server, upgrade a
database or change accounts/credentials. It requires an already-running
caller-owned disposable runtime at an explicit `http://127.0.0.1:port` address.
It creates only uniquely named synthetic workflows and one SDK Worker under
the supplied namespace. The caller must own that runtime and namespace and
include this effect in its separately selected execution packet. A loopback
address alone does not establish operating permission.

Invoke from a fresh process after the caller has bound the complete packet:

```sh
php scripts/conformance/qualify-completion-polls.php \
  /consumer/vendor/autoload.php /packet/subject.json "$SUBJECT_SHA256"
```

The consumer project must require the exact selected SDK, have no repository
overrides or scripts, disable plugins and retain normal advisory policy. Its
lock must identify the normal registry ZIP distribution at the selected
reference. The runner checks the lock hash, installed Composer metadata,
SDK release metadata and reflected implementation paths. It refuses already
loaded consumer classes and proxy environment overrides. It does not replace
the caller's complete archive/file seal, registry/release evidence, protected
profile, canonical Composer verifier or execution admission.

`COMPLETION_TEST_CLIENT_TOKEN` and `COMPLETION_TEST_WORKER_TOKEN` must contain
distinct caller-provisioned synthetic runtime roles. Credential headers and
outcome payloads never enter the trace. No credential file or vault is read.
The caller remains responsible for its selected bootstrap/profile, terminal
qualification and cleanup receipts; the accepted raw profile is not rerun.

The program uses the installed SDK's default native transport with metrics and
telemetry disabled. A two-worker loopback relay forwards the real runtime
responses. It records bounded task/attempt/owner/expiry metadata, carrier IDs,
callback order and hashes of actual request bytes. The relay implements no
Server endpoint or durable transition. An upstream error stays an error.

The initial scenarios are:

- `carriers`: three remote-activity workflows must exercise all three carried
  paths with inline lease answers and actual callbacks. Polling those same
  leases again does not qualify inline delivery.
- `pipeline`: five workflows, with a warm-up then five 20 ms local activities
  each, must prefetch before sending, perform useful local work while the
  parent answer is held, and join before the next completion. A second
  workflow reservation while that answer is pending fails.
- `pipeline-loss`: the same workload deliberately withholds one successful
  upstream answer as an unstructured 502. Retry must retain identical bytes
  and the original fence without rerunning the successful local work.
- `fallback`: an actual older producer must advertise no carried capability
  and receive no carrier request. This is distinct from pretending a current
  producer lacks support.

The controlled answer delay and loss are identified explicitly. They are not
an undelayed load comparison or a speedup measurement. Actual expiry/refencing,
additional opposite-slot/refusal controls and honest undelayed backend/load
comparisons remain required in the selected published-artifact acceptance.
The initial driver is not the complete #328–#332 release matrix.

Each invocation has a selected 15–120 second product deadline, bounded HTTP
requests and a 15 second Worker shutdown grace. The Worker owns a separate
process group so maintenance helpers cannot survive forced cleanup. Workflow
termination on failure is limited to this invocation's unique handles. The
caller must budget the additional cleanup tail and remove its isolated
runtime/database independently; the runner never removes a caller's Server.
Failures and surviving registrations prevent a successful result.

The output's image and runner references remain **caller-bound inputs**.
The program verifies endpoint version and installed package identity; it does
not attest a container digest or that a supplied runner reference names its
own copied files. The caller must supply the actual publication/index,
container inspection and runner-file binding alongside the result. The
program's `pass` covers only its declared installed-consumer scenario.

Keep the runner, `Support/CompletionPoll*.php`, the relay and the existing
`tests/Support/CompletionPipelineScenario.php` process helper in their relative
paths. Repository PHPStan includes these new source files. The pure subject
and evidence unit tests use synthetic metadata only and create no Server,
package installation, profile or published-product acceptance.
