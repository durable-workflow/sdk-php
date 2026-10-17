# Laravel HTTP hosting qualification: interim report

These observations were collected October 16–17, 2026. Each result retains its
original application profile. The current four-worker candidate is runtime
source `e81b7c80e8854c7ce97fbd2e95e7ce17c8f10772`; earlier two-worker results
use `f027697c83e753f708386d3487746c13a9a8c1e5`.
The [Composer lock](../composer.lock) and
[RoadRunner configuration](../runtime/roadrunner.yaml) identify the application
packages and HTTP pool. The October 16 results use published Server 2.8.6,
immutable image index `b3c53408`, with Workflow 2.5.22. The [input manifest](inputs.json)
now selects the next backend cohort; it does not retuple those observations.
The SDK is installed normally from Composer.
All 166 installed SDK source/resource/schema files match its released commit.

**Qualification is incomplete.** The four-worker FPM mixed runs failed. A
confirmed backend command deadlock is tracked in
[Workflow #819](https://github.com/durable-workflow/workflow/issues/819).
Its compatible library fix is now included in the newly published Server
selected below. Fresh affected cancellation and signal controls pass; full
hosting acceptance remains pending.
No final hosting-capacity or Octane performance recommendation is established.

## Current published backend

The October 17 cohort selects Server 2.8.7 / `964f9e3d`, immutable image index
`27c08e15`, with normally installed Workflow 2.5.32 / `78075b6f`.
The application, SDK lock, HTTP pools, workload, original deadlines and backend
allocations remain unchanged from the last four-CPU API profile. That Server
release has its own source, image and ordinary upgrade acceptance. It does not
qualify these HTTP measurements. All earlier failed originals remain failed.

The [fresh functional controls](https://github.com/durable-workflow/sdk-php/pull/169#issuecomment-6243029697)
verify all 166 normally installed SDK files and the exact application source.
FPM passes alternating request identities, namespace refusals, five lifecycle
originals, four cold typed reads, eager/lazy resolution and real master reloads.
The initial 30-second warm run completes 30 workflows and 180 context requests
with zero errors, drops, repairs or retried tasks.

| Independent control | Original outcomes | Stopped cache: hit / miss / eviction / forced cold |
| --- | --- | --- |
| TTL 1 second, capacity 100 | One completes with exact history prefix, values and deadlines | 0 / 3 / 1 / 1 |
| Capacity 1, TTL 300 seconds | Two complete with exact prefixes, values and deadlines | 0 / 6 / 3 / 4 |
| Ten-second API outage | Same original completes within its original 90-second cutoff | 0 / 1 / 0 / 0, final replacement only |

These four originals have sixteen completed first-attempt tasks and zero
repairs, retries or pending tasks. Actual process settings and loaded Laravel
configuration are checked before cache admission. The outage causes three
supervised CLI replacements and leaves one stale registration. It does not
establish recovery within one process. A separate deliberate CLI SIGKILL also
passes same-original cold recovery with the eight-event final history and
unchanged deadlines. Normal stacks deregister fully and stop without OOM or
automatic restarts; intentional crash/outage results retain their stated limits.

The first intended expiry control accidentally used the default TTL 300 because
the experiment helper's exported defaults overrode Compose's requested settings.
Its workflow completed, but its expiry assertion failed. A second setup was
retired before business admission. After correcting only that helper, the fresh
cache controls above verify the effective settings before starting new originals.
Application and vendor bytes are unchanged. The earlier attempts remain
unqualified for their intended cache settings.

## Method and remaining qualification

Both profiles use nginx, four HTTP workers and one independent Artisan durable
worker in the current recipe. The completed short comparisons below used two
HTTP workers. Fresh paired comparisons and both complete mixed soaks are still
required for the current recipe and corrected backend.

The prospective [worker-pool override](../compose.worker-pool.yaml) runs two
independent Artisan workers at 0.5 CPU/160 MiB each. HTTP and nginx allocations
are unchanged, keeping the application total at 2 CPUs/1 GiB. Its actual Laravel
lifecycle, shutdown, cache, recovery and load checks must run on that new
configuration. Earlier one-worker results do not qualify it.

Each cell starts with a fresh Server database, namespace, role
credentials, application storage and processes. OPcache, cached configuration,
the dependency lock, RoadRunner binary and generator stay fixed. RoadRunner
enables only its HTTP pool and loopback control RPC. It has no jobs pool.

The measurement host ran Linux/amd64 with eight logical CPUs, 16 GiB RAM and no
swap. Its reported CPU was AMD EPYC Rome. Component limits are:

| Component | CPU | Memory, no swap |
| --- | ---: | ---: |
| HTTP application | 0.9 | 640 MiB |
| Durable CLI worker | 1 | 320 MiB |
| Application nginx | 0.1 | 64 MiB |
| Separate Server API | 2 in the earlier pairs, 4 in the October 17 cohort | 1 GiB |
| Separate MySQL | 1 | 2 GiB |
| Separate Server queue worker | 0.2 | 384 MiB |
| Separate Server maintenance | 0.2 | 128 MiB |
| Separate Redis | 0.1 | 128 MiB |
| Separate k6 generator | 0.3 | 512 MiB |

The application allocation totals 2 CPUs and 1 GiB. The backend and tools are
additional resources, not hidden inside that limit.

Each cell has a 30-second warmup. Measured windows offer six context reads and
one new one-activity workflow per second for 120 seconds, with eight context and
16 workflow virtual users. Each original keeps its 35-second observer,
600-second run deadline and 900-second execution deadline. The drain limit is
36 seconds. Independent installed PHP readers verify the exact int64, null and
Unicode values and histories. SQL checks original identities, first-attempt
tasks, repairs, pending work and microsecond deadline offsets.

The comparison plan orders pairs FPM/Octane, Octane/FPM, FPM/Octane, followed by
a fresh FPM drift check. Each profile also requires a fresh 300-second mixed
soak. Its five-original
cycle contains two short workflows, one timer, one queried/signalled workflow
and one queried/cancelled workflow. Two real HTTP reloads are requested at
120 and 240 seconds. Their actual times and replacement worker PIDs are read
back. Automatic HTTP recycling remains enabled at 500 requests per worker.

## How to read the measurements

Context, submission and status timings are separate HTTP operations. Workflow
latency is observed by polling at 250 ms intervals. It includes backend work
and that polling delay. Application CPU per terminal workflow includes the
fixed context workload, variable status polls, nginx and the CLI worker. It is
not callback CPU or a measure of durable replay savings.

The sampler remains active through drain and verification. The tables' CPU
deltas cover offered load and its actual drain; the independent post-run
verification is separate. CPU and throttling use cgroup counters. Process RSS may count shared
pages more than once. Charged cgroup memory is reported separately. Sampled
peaks can miss short spikes; cgroup lifetime peaks also include warmup.
Individual process reads can disappear during recycling and are marked
unavailable rather than counted as measured zeroes.

Backend access counters measure produced response-body bytes. Workflow poll
envelopes contain inline history and protocol metadata, so those counts are
not pure history bytes. Produced bytes can exceed delivered bytes after an
aborted response. Cache diagnostics are read from the actual stopped worker
and include that cell's prerequisites, warmup and measured window.

## Published-package controls

These controls ran on `f027697c` with the earlier two-worker HTTP profile.
Their linked receipts retain that scope. The changed HTTP pool and backend
still require their affected controls before final acceptance.

The [lifecycle and cold-replacement receipt](https://github.com/durable-workflow/sdk-php/pull/169#issuecomment-6222923826)
covers alternating users, request headers and logs, auth/namespace refusals,
five lifecycle originals, exact typed reads, eager/lazy SDK resolution and
HTTP reloads. An actual CLI SIGKILL followed by a fresh worker completes the
same original with its six-event prefix preserved in the final eight events,
one activity, five first-attempt tasks, zero repairs and unchanged deadlines.

The [cache and connection receipt](https://github.com/durable-workflow/sdk-php/pull/169#issuecomment-6223066356)
records these independent controls:

| Control | Result | Actual stopped cache counters: hit / miss / eviction / forced cold |
| --- | --- | --- |
| TTL 1 second, capacity 100 | One original completes with its full prefix | 0 / 3 / 2 / 0 |
| Capacity 1, TTL 300 seconds | Two originals complete with their full prefixes | 0 / 6 / 3 / 4 |
| Actual 10-second API outage | Same original completes within its original 90-second cutoff | 0 / 1 / 0 / 0, final replacement only |

All four cache/outage originals preserve typed results and exact deadlines,
with 16 completed first-attempt tasks and zero repairs. The outage produces an
actual cURL error 56 and three automatic CLI supervisor restarts. It does not
prove recovery within one process. The intentional kill and outage leave one
killed registration each. Their original cleanup wrappers fail the overly
strict zero-registration predicate; that wrapper result remains unsuccessful.
The task resources are removed. Ordinary measured cells retain the stricter
zero-registration and zero-restart checks.

Earlier load, controller and older-package observations remain separate
[failed or scoped reference outcomes](https://github.com/durable-workflow/sdk-php/pull/169#issuecomment-6222599030).
They are not substituted for the final published tuple's measurements.

## Results

Fresh repetitions and reload soaks are required after the corrected package is
published and qualified in its normal Server consumer. Earlier results keep
their original application/backend identities and outcomes.

### Published 2.8.7 four-worker FPM run

The October 17 mixed run uses the published backend above, unchanged application
runtime `e81b7c80`, four HTTP workers and the four-CPU API allocation. It **fails**:

| Observation | Result |
| --- | ---: |
| Admitted originals | 265 of 300 required |
| Observed completed / cancelled | 201 / 51 |
| Completed context requests | 1,743 of 1,800 required |
| Dropped offers | 92 |
| Canonical repairs | 82 |
| Canonical completed / cancelled / waiting | 210 / 51 / 4 |

Both real master reloads replace all four HTTP workers. All 51 observed
cancellation requests and 51 signal requests return their expected 204, and
102 query replies pass their committed-activity checks. The earlier cancellation
deadlock is not observed in this run. That scoped result does not qualify the
load target or establish a cause for the remaining backlog and repairs.

The original driver also has a separate failure: two iterations start outside
the offered window, fail its boundary assertions and trigger immediate threshold
abort at about 302 seconds. Thirteen observers are interrupted before the declared
36-second drain finishes. The independent audit then correctly refuses an
unfinished signal original. Those transport interruptions are not attributed to
a runtime crash. Dropped offers and repairs independently keep this run failed.

A later read finds 82 distinct completed workflow tasks with one repair and one
attempt each. Sampled repaired tasks waited four to six seconds before their
first lease. This does not establish why they waited. All 265 activities complete
once in the recorded canonical audit; original run/execution deadline offsets
remain exact. The API records 521.84 CPU seconds, without CPU throttling; the
application records 84.08 CPU seconds. There is no OOM, swap or sampled host CPU
steal. Every normal role exits 0 without automatic restarts, all registrations
are removed, and the stack is retired.

The prospective driver correction refuses late admissions without inventing
replacement offers and evaluates failures after the original observers drain.
The 35-second per-original observer, 36-second drain, rates, VU limits and all
success thresholds remain unchanged. This original is not rerun or relabeled.
Fresh load acceptance remains required after the correction and diagnosis.

The synthetic [drain regression](check-load-drain.sh) exercises the actual k6
driver against an HTTP fixture, not a workflow runtime. Five originals each
remain pending for four seconds during a five-second offered window. A separate
case injects one HTTP failure at the last context request. The old driver aborts
with only two terminal observations; the corrected driver observes all five and
still returns k6's failure exit 99. Its clean case observes all five and exits 0.
CI runs both cases without credentials or a real backend. Run locally with Docker
and jq using `LOAD_DRAIN_RUN=unique-drain-test bash qualification/check-load-drain.sh`
from the example directory. This check does not qualify load, durable execution
or a higher capacity.

### Published-SDK claim diagnostic

A separate local diagnostic uses normal SDK 2.4.2 / `25d796ad` with all 166
source/resource/schema files verified, the published 2.8.7 backend and the same
pure SDK workflow/activity definitions as this application. It exercises eight
fresh short, timer, signal/query and cancellation originals per configuration.
It does not run Laravel HTTP, offered load or a performance comparison. The
shared four-CPU host has existing swap and is not reserved for measurements.

| Functional observation | One CLI process | Two CLI processes |
| --- | ---: | ---: |
| Combined SDK limit | 0.5 CPU / 320 MiB | 0.5 CPU / 320 MiB |
| Completed / cancelled originals | 7 / 1 | 7 / 1 |
| Completed / cancelled tasks | 30 / 1 | 30 / 1 |
| Completed activities | 8 | 8 |
| Repairs / retried tasks / final registrations | 0 / 0 / 0 | 0 / 0 / 0 |
| Maximum observed ready-to-first-lease interval | 5.402 seconds | 2.009 seconds |
| Longest of three actual queries | 5.451 seconds | 2.046 seconds |

Both batches retain their original 35-second observers, exact typed results and
600/900-second deadlines. Each process stops normally without OOM or automatic
restart. Guzzle's ordinary request/statistics hooks record only allowlisted
poll identities, paths, timing and response metadata. Vendor bytes are unchanged.

In the one-process original, the worker spends 13.478 seconds in actual HTTP
requests during the approximately 13.6-second active interval. Its workflow
poll replies during queued work take at most 0.470 seconds. The initial held
query poll returns `task_queue_changed`; subsequent sweeps lease workflow tasks.
The 5.451-second query observes `workflow_task_pending` until its own workflow's
pending resume completes, then receives the expected answer. These observations
support trying independent worker concurrency. They do not identify the cause
of the earlier mixed-run repairs, prove a missing wakeup, or establish a gain
or capacity. The two-process batch is fresh evidence, not a rescue of the first.

The next hosting qualification uses the prospective pool override, the corrected
driver and fresh application processes. Original rates, budgets and success
thresholds stay fixed. Full hosting acceptance remains open.

### Four-worker FPM mixed runs

Both runs use runtime source `e81b7c80`, the published backend in the manifest
and the unchanged 300-second mixed workload. The second run increases only the
Server API allocation from two to four CPUs, with its memory still 1 GiB.

| Observation | Two-CPU API | Four-CPU API |
| --- | ---: | ---: |
| Admitted original workflows | 233 | 105 |
| Observed completions | 174 | 76 |
| Observed cancellations | 44 | 18 |
| Context responses | 1,728 | 727 |
| Dropped offers | 140 | 61 |
| Observer errors | 1 | 1 |
| Repairs on canonical readback | 118 | 35 |
| Result | Failed | Failed |

The [two-CPU API run](https://github.com/durable-workflow/sdk-php/pull/169#issuecomment-6223818842)
passes both real master reloads and all HTTP requests, but fails the offered
workload and zero-repair criteria. Later canonical readback has 185 completed,
44 cancelled and four waiting originals. The API records 514.73 CPU seconds
over 302.99 elapsed seconds, including 38.32 seconds throttled. This is one
observed resource limit, not a proven explanation for every missed criterion.

The separate four-CPU API run stops at a real cancellation HTTP 500. MySQL's
actual error 1213 lock graph shows signal and cancellation transactions for
different runs acquiring task/failure index locks in a cycle. MySQL rolls back
cancellation completely. Its original remains waiting with the same six-event
prefix and no failure record. Later aggregate readback has 82 completed,
18 cancelled and five waiting originals. Workflow #819 owns the reproduction,
compatible package correction and affected published consumer.

Increasing API CPU is not an established fix, and neither run qualifies the
four-worker recipe. Their resources and failed originals were retired after
the immediate diagnosis. The narrow failure description remains in the owning
issue. No Octane mixed run or fresh four-worker paired comparison is claimed.

### Earlier two-worker profile

The earlier source `f027697c` completed three short-window pairs and an FPM
drift check. The [paired receipt](https://github.com/durable-workflow/sdk-php/pull/169#issuecomment-6223479757)
records these measured windows, each with 120 completed workflows and 720
context reads per profile:

| Pair order | FPM app CPU seconds | Octane app CPU seconds | FPM / Octane observed workflow p95 ms |
| --- | ---: | ---: | ---: |
| FPM, Octane | 18.089 | 15.774 | 3,312 / 2,515 |
| Octane, FPM | 18.528 | 14.809 | 3,146 / 1,554 |
| FPM, Octane | 14.843 | 14.791 | 1,583 / 1,555 |

The third pair's application CPU differs by only 0.35%. The receipt records
mean charged application memory across these pairs of 88.82 MiB for FPM and
191.57 MiB for Octane. These limited observations do not establish a consistent
speedup or durable execution gain. FPM remains the ordinary starting profile.

Its first FPM mixed soak [failed](https://github.com/durable-workflow/sdk-php/pull/169#issuecomment-6223653640):
100 admitted originals, 70 observed completions, 18 observed cancellations,
457 context responses, 282 dropped iterations and two HTTP 502s. Independent
nginx logs show connection resets at the first FPM master reload. Subsequent
canonical readback has 78 completed, 18 cancelled and four waiting originals,
with 16 repairs. No OOM or automatic process restart occurred.

That failed outcome is unchanged. Its short-window results do not qualify the
four-worker recipe or establish mixed-traffic capacity. The prospective recipe
keeps the same CPU/memory allocation and workload, increases both HTTP pools
and gives FPM a bounded in-flight request grace period. New measurements are
required for the changed profile.
