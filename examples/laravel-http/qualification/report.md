# Laravel HTTP hosting qualification

This report covers ordinary PHP-FPM and optional Octane/RoadRunner hosting of
the Laravel service example. HTTP processes submit and inspect operations.
Independent Artisan CLI workers execute durable instructions against Server.
HTTP process reuse does not retain workflow Fibers or eliminate replay.

Both profiles pass the bounded functional, recovery and load qualification
below on the published tuple. Start with FPM for this recipe: the comparison
does not establish a consistent application CPU or workflow-latency gain from
Octane, which uses more memory. Production capacity remains workload-specific.

## Reproducible inputs

The October 18 cohort uses normally installed SDK 2.4.2 / `25d796ad`, published
Server 2.8.7 / `964f9e3d` / immutable image index `27c08e15`, and Workflow
2.5.32 / `78075b6f`. All 166 installed SDK source/resource/schema files match
the released commit. The application source is checked before admission.
The common load-driver revision is `af741d1`; application runtime/configuration
remains `e81b7c80`. Later report-only edits do not change those measured bytes.

The application runs PHP 8.3.35. The [Composer lock](../composer.lock) resolves
Laravel 12.69.3 and Octane 2.21.0. RoadRunner 2025.1.15 has SHA-256
`c67dd4dc6d14c0120a7f8a2edcb035eddc693db880d349f3be33a38728b89efb`.
The lock SHA-256 is
`a41fe273640b8562608a0c1de468826c1e983e817bf2a056c1b20e00142eec34`.
The [input manifest](inputs.json) pins backend and measurement tools.
RoadRunner enables its HTTP pool and loopback control RPC, with no jobs pool.

Both profiles have four HTTP workers. This cohort uses
[`compose.worker-pool.yaml`](../compose.worker-pool.yaml) for two independent
CLI workers, each with its own registration and history cache. The default
Compose recipe has one CLI worker. Results from this pool must not be claimed
for the one-worker configuration.

The isolated Linux/amd64 host has eight logical CPUs, 16 GiB RAM, AMD EPYC Rome
and no swap. Application, backend and tools run in separate containers on that
host. Backend calls use the internal HTTP network. Each cell starts fresh
application storage, processes, Server database, namespace and role-credential
records. The application lock,
OPcache, cached configuration, backend limits and generator remain fixed.

| Component | CPU | Memory, no swap |
| --- | ---: | ---: |
| HTTP application | 0.9 | 640 MiB |
| Two durable CLI workers | 0.5 each | 160 MiB each |
| Application nginx | 0.1 | 64 MiB |
| Separate Server API | 4 | 1 GiB |
| Separate MySQL | 1 | 2 GiB |
| Separate Server queue worker | 0.2 | 384 MiB |
| Separate Server maintenance | 0.2 | 128 MiB |
| Separate Redis | 0.1 | 128 MiB |
| Separate k6 generator | 0.3 | 512 MiB |

The application total is 2 CPUs / 1 GiB. Backend and tool costs are additional.

## Method

The fixed schedule offers six authenticated context reads and one original
one-activity workflow per second. Each cell warms up for 30 seconds. Measured
windows last 120 seconds. Mixed soaks last 300 seconds and cycle two short
workflows, one timer, one queried/signalled and one queried/cancelled original.
Real HTTP reloads occur at 120 and 240 seconds during a soak. The actual
replacement PIDs are checked. Both profiles configure a 500-request recycle limit.

The driver reserves 64 context VUs and 16 workflow VUs. The context reserve
covers six offers per second through the existing ten-second transport cap.
Query requests may take at most 30 seconds, capped by the original observer's
remaining time. Each original keeps a 35-second observer, 600-second run
deadline and 900-second execution deadline. The outer drain is 36 seconds.
No failed or late original is replaced by a new admission.

Independent installed PHP readers verify the exact bigint, null and Unicode
results and original histories. SQL verifies identities, activity counts,
first attempts, repairs, pending tasks and microsecond deadline offsets.
Dropped offers, request/check failures, unfinished originals, OOM, swapping or
unexpected restarts fail acceptance. Normal cells must deregister fully.

The repeated comparison orders FPM/Octane, Octane/FPM and FPM/Octane, followed
by a fresh FPM drift cell. All use the same inputs. Successful short windows
cannot substitute for a mixed soak or recovery test.

### Interpreting resource measurements

Context, submission and status timings are separate HTTP operations.
Workflow latency includes the 250 ms status-poll interval and backend work.
Application CPU includes the fixed context workload, variable status polls,
nginx and CLI workers. CPU per completed workflow amortizes that whole workload;
it does not isolate callback CPU or measure replay savings.

Sampling covers every application/backend/tool role through drain and
verification. CPU deltas cover offered load and its actual drain; independent
post-run verification is separate. Cgroup counters measure CPU/throttling and
charged memory. Process RSS can double-count shared pages. Sampled peaks may
miss short spikes; cgroup lifetime peaks also include warmup. Disappearing
process reads are unavailable, not measured zeroes.

Backend counters measure produced response-body bytes, including protocol
metadata and inline histories. They are not pure history bytes and may exceed
delivered bytes after an aborted response. Stopped worker cache diagnostics
include that cell's prerequisites, warmup and measured window. Zero cache hits
must be reported as zero.

## Functional controls

Request identity, namespace/auth refusal, optional headers and logs, lazy/eager
SDK resolution, real reloads, five lifecycle originals and four cold typed
reads pass in the reported source cohorts. The HTTP credential and worker
credential are separate. Each process keeps one fixed authority.

The [FPM cache/recovery controls](https://github.com/durable-workflow/sdk-php/pull/169#issuecomment-6243887984)
and [independent Octane controls](https://github.com/durable-workflow/sdk-php/pull/169#issuecomment-6244222635)
use the published tuple above. Cache columns
are actual stopped counters: hit / miss / eviction / forced cold.

| Control | Originals / first-attempt tasks per profile | FPM counters | Octane counters |
| --- | --- | --- | --- |
| Capacity 100, TTL 1 second | 1 / 4 | 0 / 3 / 1 / 1 | 0 / 3 / 1 / 1 |
| Capacity 1, TTL 300 seconds | 4 / 16 | 0 / 12 / 7 / 8 | 0 / 12 / 9 / 8 |

All preserve the entire original prefix, typed result and exact deadlines with
zero repairs, retries or pending work. Cache settings are read from both
running processes before admission. The four-original eviction test populates
two independent caches before releasing any original. The existing two-original
`cache.php eviction` command remains the single-worker control.

Killing both CLI workers in each profile preserves the same original through
cold replacement: eight final events, one activity and five first-attempt
tasks. Intentional SIGKILL exits are 137 without OOM. The original two killed
registrations remain explicit; replacement workers deregister normally.

Stopping each isolated API for ten seconds produces actual transport errors.
The original completes with four first-attempt tasks. FPM's workers restart
three and six times; Octane's restart three times each. This proves supervised
process replacement, not same-process recovery. Only the exact original
failed-deregistration IDs are permitted to remain. All final stack roles exit
zero without OOM, with actual restart counts checked individually.

## Current load results

### Three alternating short pairs

All six paired cells and the final FPM drift cell pass: 120 offered/admitted/completed workflows, 720
context reads and 360 completed first-attempt tasks each. Errors, dropped
offers, repairs, retries and pending work are zero. Independent typed/history
reads and exact original deadlines pass. Every stack shuts down normally with
zero registrations. Each row below is a separate fresh cell in execution order.
Latencies are p50 / p95 / p99 in milliseconds.

| Cell | Context HTTP | Start HTTP | Status HTTP | Workflow observed |
| --- | ---: | ---: | ---: | ---: |
| FPM 1 | 6.85 / 8.50 / 9.75 | 257.71 / 374.55 / 480.31 | 64.13 / 77.07 / 82.55 | 1,281 / 1,614 / 1,682 |
| Octane 1 | 5.24 / 6.53 / 8.20 | 261.84 / 326.38 / 470.17 | 63.54 / 78.71 / 91.57 | 1,289 / 1,658 / 1,756 |
| Octane 2 | 5.24 / 6.52 / 8.37 | 262.77 / 373.53 / 545.42 | 62.48 / 74.75 / 81.29 | 1,279 / 1,642 / 1,721 |
| FPM 2 | 7.11 / 8.84 / 10.70 | 260.24 / 350.59 / 526.64 | 63.49 / 78.45 / 88.24 | 1,273 / 1,624 / 1,674 |
| FPM 3 | 6.59 / 8.19 / 9.37 | 243.50 / 300.69 / 445.57 | 60.34 / 71.04 / 78.83 | 1,245 / 1,556 / 1,589 |
| Octane 3 | 4.75 / 5.85 / 6.67 | 239.75 / 304.61 / 417.70 | 56.82 / 68.16 / 76.56 | 1,229 / 1,529 / 1,593 |
| FPM drift | 6.45 / 7.96 / 8.63 | 241.05 / 319.95 / 446.68 | 58.83 / 68.81 / 73.03 | 1,236 / 1,539 / 1,568 |

Memory columns are mean / sampled peak MiB. CPU excludes the separately
measured generator. The application and backend include all nine service roles.

| Cell | App CPU seconds / per completed workflow | App + backend CPU seconds | App RSS MiB | App charged MiB | App + backend RSS MiB | App + backend charged MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| FPM 1 | 16.84 / 0.140 | 293.84 | 318.17 / 323.60 | 138.80 / 144.38 | 1,370.09 / 1,450.57 | 874.32 / 923.47 |
| Octane 1 | 17.10 / 0.143 | 299.83 | 498.30 / 502.68 | 324.09 / 329.16 | 1,545.67 / 1,585.48 | 1,061.92 / 1,106.83 |
| Octane 2 | 16.91 / 0.141 | 297.81 | 497.76 / 501.94 | 323.64 / 328.27 | 1,542.86 / 1,598.89 | 1,057.78 / 1,109.04 |
| FPM 2 | 17.18 / 0.143 | 294.15 | 318.18 / 323.05 | 138.42 / 143.66 | 1,360.24 / 1,403.87 | 869.44 / 911.87 |
| FPM 3 | 16.23 / 0.135 | 281.01 | 319.68 / 324.63 | 138.58 / 143.96 | 1,365.63 / 1,463.71 | 873.66 / 927.04 |
| Octane 3 | 15.72 / 0.131 | 280.14 | 494.94 / 498.59 | 322.41 / 326.48 | 1,543.15 / 1,628.75 | 1,055.40 / 1,105.88 |
| FPM drift | 15.94 / 0.133 | 278.12 | 318.38 / 324.00 | 138.68 / 144.69 | 1,306.73 / 1,399.14 | 871.40 / 933.98 |

Three-cell mean application CPU is 16.75 seconds for FPM and 16.58 seconds for
Octane, a 1.05% difference. Individual pair directions differ. Charged application
memory averages 138.60 versus 323.38 MiB, an additional 184.78 MiB for Octane.
The final FPM baseline uses 5.34% less application CPU than the first FPM cell,
and its observed workflow p95 is 4.61% lower. That drift is larger than the
cross-profile mean CPU difference. No consistent CPU or execution gain is
established. Octane's context p95 is 1.97–2.34 ms lower in the paired cells.

Measured short windows run from October 18 at 01:24:22 through 01:51:04 UTC,
including actual drains. Each cell's runtime and raw samples are deleted after
audit. The temporary host and its task access key are deleted at 01:51:48 UTC.

### Component and transport costs

Each entry below is mean CPU seconds / mean throttled seconds / maximum sampled
charged MiB across that profile's three short cells. Per-component peaks occur
at different times and must not be summed as a concurrent peak.

| Component | FPM | Octane |
| --- | ---: | ---: |
| HTTP application | 11.57 / 0.00 / 43.96 | 11.48 / 0.00 / 228.05 |
| nginx | 0.98 / 0.01 / 2.66 | 0.94 / 0.00 / 2.84 |
| CLI worker 1 | 2.10 / 0.00 / 48.84 | 2.06 / 0.00 / 49.62 |
| CLI worker 2 | 2.11 / 0.00 / 49.70 | 2.09 / 0.00 / 49.65 |
| Backend MySQL | 69.30 / 0.30 / 610.10 | 70.39 / 0.40 / 611.89 |
| Backend Redis | 7.42 / 14.41 / 6.91 | 7.50 / 14.70 / 7.04 |
| Backend API | 184.27 / 0.00 / 100.40 | 186.16 / 0.00 / 100.33 |
| Backend queue worker | 0.88 / 0.52 / 49.72 | 0.92 / 0.62 / 49.91 |
| Backend maintenance | 11.04 / 45.17 / 46.71 | 11.05 / 45.49 / 45.46 |
| Separate k6 generator | 4.08 / 1.20 / 45.16 | 4.10 / 1.21 / 45.65 |

Complete service snapshots are at most 3.97 seconds apart with zero unavailable
service samples. Maximum sampled host steal is 2% for FPM and 4% for Octane,
with no swap. Backend throttling is recorded above rather than attributed to
HTTP hosting.

The following are three-cell means of actual backend requests / produced
response-body MiB. Poll counts include empty polls. These envelopes contain
metadata and inline histories; the byte totals do not isolate history payloads.

| Backend operation | FPM requests / MiB | Octane requests / MiB |
| --- | ---: | ---: |
| Workflow-task poll | 666.67 / 17.08 | 664.00 / 17.02 |
| Activity-task poll | 668.67 / 15.60 | 666.00 / 15.54 |
| Query-task poll | 668.67 / 15.54 | 666.00 / 15.48 |
| Other successful GETs | 616.33 / 71.78 | 627.67 / 71.82 |
| Other successful POSTs, 200 | 646.00 / 15.13 | 646.00 / 15.13 |
| Original admission POSTs, 201 | 120.00 / 0.15 | 120.00 / 0.15 |
| Health | 12.00 / 0.06 | 12.00 / 0.06 |

Stopped CLI cache totals over prerequisites, warmup and the three short cells
are FPM **0 hits / 936 misses / 59 evictions / 471 forced cold replays**, and
Octane **0 / 936 / 64 / 471**. No history-cache hit or replay saving is
demonstrated by these runs. The 100-entry/300-second defaults remain unchanged.

### Mixed reload soaks

Both fresh 64-context-VU mixed cohorts pass on the same inputs. Each admits all
300 offered originals, observes 240 completions and 60 cancellations, and
completes all 1,800 contexts. All response/history checks pass. Independent
typed reads and SQL find 300 activities completed once, 1,080 completed
first-attempt tasks, 60 cancelled tasks and zero repairs, retries or pending
work. The sixty unclaimed tasks are cancelled terminal tasks, not backlog.
Original 600/900-second deadline offsets remain exact.

| Profile | Application CPU seconds | Charged memory mean / peak MiB | Context HTTP p95 ms | Observed workflow p95 ms | Timer due-to-claim range ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| FPM | 53.51 | 150.10 / 162.93 | 8.92 | 3,257 | 170–1,491 |
| Octane | 56.18 | 333.59 / 375.79 | 10.70 | 5,821 | 197–2,811 |

Both scheduled reloads replace all four HTTP workers in each profile. All
eleven stack roles subsequently exit zero without OOM or automatic restarts;
registrations return to zero. There is no swapping. Maximum sampled host CPU
steal is 2% for FPM and 1% for Octane. Runtime, databases and raw samples are
deleted after audit. These are bounded soaks and do not establish long-term
capacity or a general speedup.

The [16-context-VU FPM cohort](https://github.com/durable-workflow/sdk-php/pull/169#issuecomment-6243824493)
passes its exact scope: 300 admitted/terminal originals, 240 completed and
60 cancelled, 1,800 contexts, zero errors/drops/repairs/retries/pending work.
Both reloads replace all four workers. Original histories, typed values and
deadlines pass. All eleven roles exit zero without OOM/restarts and
registrations return to zero.

Its [separate Octane original](https://github.com/durable-workflow/sdk-php/pull/169#issuecomment-6243983972)
is **failed**: all 300 workflows and response/history checks pass, but twelve
context offers are dropped at reload. k6 reports exhausting sixteen context
VUs; actual context latency reaches 4.002 seconds within the ten-second limit.
The current 64-VU reserve is a new common measurement input. The unchanged
16-VU driver fails a constructed nine-second context-delay control; all four
corrected driver controls pass, with injected HTTP failure retaining exit 99.
No application defect or gain is inferred from generator exhaustion.

## Earlier failures and scoped observations

Earlier originals remain failed or scoped as recorded in their owning links.
They are not rerun in place or substituted for current results.

- The first Octane pool expiry control completes its original with an actual
  eviction, exact history/deadlines and zero repairs, but its helper wrongly
  requires a forced cold replay after expiry. Actual counters are 0 hits,
  3 misses, 1 eviction and 0 forced cold replays. The documented expiry rule
  permits that zero. The corrected helper and a separate fresh original pass;
  the failed control stays failed. Capacity still requires forced cold replay.
- [First two-CLI-worker mixed run](https://github.com/durable-workflow/sdk-php/pull/169#issuecomment-6243742751):
  all 300 admissions, but only 239 completions/59 cancellations observed and
  three context drops. The driver confused generic waiting with a committed
  activity. The corrected driver waits for the example's explicit query phase
  within the original cutoff. Old and corrected synthetic controls establish
  that driver correction; the failed application run remains failed.
- [Earlier published 2.8.7 one-worker mixed run](https://github.com/durable-workflow/sdk-php/pull/169#issuecomment-6243209312):
  265 admissions, 252 observed terminal outcomes, 92 dropped offers and 82
  repairs. Driver threshold abort also interrupted thirteen observers before
  drain. Later diagnosis does not establish a cause for every delay or repair.
- [Earlier four-HTTP-worker runs](https://github.com/durable-workflow/sdk-php/pull/169#issuecomment-6223818842):
  insufficient offers/repairs in one cell and an actual MySQL cancellation
  deadlock in another. [Workflow #819](https://github.com/durable-workflow/workflow/issues/819#issuecomment-6243219869)
  delivers that specific correction through normal source and published HTTP
  consumers. This does not qualify the earlier load results or prove a common
  cause for other observations.
- [Earlier two-HTTP-worker short pairs](https://github.com/durable-workflow/sdk-php/pull/169#issuecomment-6223479757)
  finish 120 workflows and 720 contexts per cell, but one pair differs by only
  0.35% application CPU. The associated [mixed soak fails](https://github.com/durable-workflow/sdk-php/pull/169#issuecomment-6223653640)
  with reload connection resets and unfinished work. Those old profiles do not
  establish a consistent gain or qualify the current four-worker recipe.
- [Published-SDK one/two-process diagnostics](https://github.com/durable-workflow/sdk-php/pull/169#issuecomment-6243209312)
  support testing independent concurrency with exact typed histories and no
  repairs in their eight-original batches. They are functional observations
  on a shared host, not a performance comparison.

Completed cell databases, histories, raw samples and transport are deleted
after immediate readback. This readable report and owning GitHub results retain
the useful conclusions. Temporary dependency/build state and the isolated host
are retired when the qualification phase ends.
