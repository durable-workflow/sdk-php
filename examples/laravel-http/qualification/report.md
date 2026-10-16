# Laravel HTTP hosting qualification: interim report

These observations were collected October 16, 2026. Each result retains its
original application profile. The current four-worker candidate is runtime
source `e81b7c80e8854c7ce97fbd2e95e7ce17c8f10772`; earlier two-worker results
use `f027697c83e753f708386d3487746c13a9a8c1e5`.
The [input manifest](inputs.json), [Composer lock](../composer.lock) and
[RoadRunner configuration](../runtime/roadrunner.yaml) identify the published
packages, images and HTTP pool. The SDK is installed normally from Composer.
All 166 installed SDK source/resource/schema files match its released commit.

**Qualification is incomplete.** The four-worker FPM mixed runs failed. A
confirmed backend command deadlock is tracked in
[Workflow #819](https://github.com/durable-workflow/workflow/issues/819).
Its compatible library fix is published, but the affected Server image and
hosting comparison still need qualification.
No final hosting-capacity or Octane performance recommendation is established.

## Method and remaining qualification

Both profiles use nginx, four HTTP workers and one independent Artisan durable
worker in the current recipe. The completed short comparisons below used two
HTTP workers. Fresh paired comparisons and both complete mixed soaks are still
required for the current recipe and corrected backend.

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
| Separate Server API | 2 | 1 GiB |
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
