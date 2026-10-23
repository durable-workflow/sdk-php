# Published service worker profiling

This diagnostic supports the investment decision in [#113](https://github.com/durable-workflow/sdk-php/issues/113).
It measures an ordinary installed worker and isolated cold replay of real
canonical history. It does not retain live execution or establish a speedup.

Create a separate ordinary Composer consumer requiring exactly
`durable-workflow/sdk:3.4.1`. Do not use a path repository or the source tree's
autoloading. The initial baseline uses PHP8.3.36 and published Server2.11.0:

```text
durableworkflow/server@sha256:b29157eb2cf844916debbbb3c01b3257672e0dece778a9c6fa539043a3038f02
```

Use the repository's `docker-compose.claimed-task-recovery.yml` disposable
bootstrap/server topology. Set its required Server, HAProxy and normal-consumer
variables; start only `bootstrap server` under a unique Compose project.
HAProxy is needed only to parse the existing file. Apply separate CPU/memory
limits to the runtime and worker. The fixture credentials are public disposable
credentials; this diagnostic must not run against a production service.

Mount this source at `/sdk` and the normal consumer at `/consumer` in a separate
published PHP container on that project's network. For example:

```sh
php /sdk/benchmarks/service-replay.php /consumer/vendor/autoload.php http://server:8080 32 1 1
```

Arguments select activity cycles per run, concurrently submitted runs and a
final durable timer in seconds. Limits are1000 cycles,16 runs and10 seconds.
Each trial requests shutdown after all workflow completions. A600-second trial
deadline requests shutdown after the next HTTP response; the injected mature
Guzzle client independently bounds each request. This is a diagnostic resource
limit, not a lifecycle or cancellation policy for application workflows.
`DW_PROFILE_TRIAL_SECONDS` selects30–1800 seconds for a separately recorded
trial; an expired trial remains an incomplete result.
Fresh workload runs receive that allowance plus300 seconds as their explicit
business run timeout, with an execution timeout of at least3600 seconds.
Transport deadlines remain separate. Earlier trials used the ordinary600-second
run timeout; preserve those failed/unfinished results rather than extending
their original deadlines. The receipt records PHP's memory limit and exact
harness/bootstrap hashes as well as the outer cgroup budgets.

`DW_PROFILE_PROGRESS=1` writes a small heap/call-count observation to stderr
every ten claimed tasks. Retain stdout and stderr separately. Keep a bounded
container log and inspect the exited container before retiring it when a long
trial fails to produce a terminal receipt.

The independent activity maintainer uses the accompanying explicit bootstrap,
with matching endpoint, namespace, disposable credentials and HTTP settings.
Its child-process CPU and peak RSS are reported separately from parent-process
CPU/RSS. The worker container's available cgroup limit/peak/events describe the
aggregate process group. Unsupported cgroup files are reported as null. Use a
memory+swap limit when measuring; inspect OOM events and
unfinished work rather than treating an incomplete run as throughput.

The receipt separates:

- Managed worker wall time and parent/child CPU; startup and fenced shutdown
  are included, while initial submission and final readback are outside that
  measurement interval.
- HTTP durations and actual response body bytes for polls, history pages,
  completions, heartbeats and other requests. Poll response bytes include
  inline history and protocol metadata; they are not history bytes alone.
  These observations do not include independent maintainer HTTP requests.
  HTTP recording adds observer overhead, including decoding poll documents;
  retain an uninstrumented control before attributing complete CPU costs.
- Server-clock queue waits from canonical task records and exact timer
  scheduling/firing records. Poll wall time includes network/runtime work and
  must not be labeled queue delay or timer scheduling delay.
- Three isolated fresh-Replayer measurements of the complete history and each
  available10/100/1000-event canonical prefix. Prefixes may end at a pending
  operation. Repeated commands must agree; none is submitted or executed.

Exact result, completed workflow count, physical activity effect count and
duplicate effects are checked before accepting a trial. Original claims,
history hashes, package identities and diagnostics are retained in the JSON
receipt. Generated receipts belong on the owning issue/artifact, not in Git.

Ordinary unversioned routing is the initial control. `DW_PROFILE_BUILD_ID`
registers and promotes a disposable deployment through the published control
API before submitting ordinary workflows. Run compatibility is read back and
must match before measurement. A start-body build field is not a substitute
for deployment promotion. Do not compare a stalled named-build trial with a
drained unversioned trial as a performance gain. Inspect the runtime's actual
compatibility identity and cache hit/miss/forced-cold counters; requesting a
build or enabling a history cache does not prove prefix reuse occurred.

Run independent measured repetitions, concurrent workloads and identical
resource budgets before an investment decision. Function-level replay/task
CPU attribution, retained-execution comparison, cache-heavy/miss-heavy controls
and the complete1GiB aggregate-worker study remain separate qualification.
No live-Fiber feature or full#113 acceptance follows from this harness.

An experimental XHProf2.3.10 call graph attributed later worker calls to
suspended Fiber frames. Its original profile is retained on#113 and excluded
from function CPU attribution. The isolated Replayer measurements use process
CPU around a completed call with a fixed pure handler. They establish that
component's cost; complete managed-worker CPU attribution remains separate.

Retire only the diagnostic's uniquely named containers, volumes/network and
consumer after retaining exact source, recipe, results and failed originals.
