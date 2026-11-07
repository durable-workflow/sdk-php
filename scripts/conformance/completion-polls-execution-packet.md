# Installed completion-poll execution packet (SDK 3.10.0)

This packet runs the installed consumer matrix for SDK #328–#332 against the
published SDK 3.10.0 package. It uses only the existing runner
(`qualify-completion-polls.php`, `compare-completion-poll-load.php`) and its
held-root admission (`completion-polls.md#held-consumer-root`). It is a list of
bounded, reviewed steps for one owner to run in order. It is not a scheduler.

Nothing here is selected. Each phase marked **effect** needs its own fresh
selection after the inputs in [Remaining values](#remaining-values) exist.

## Fixed inputs

| Input | Value |
| --- | --- |
| New producer | Native Server 3.2.0, source `3c6f1eda9c6d2ebc0ee5bec8ccf431900052be9e`, index `sha256:6916f6f3bb1ee58d071f10f888068fd321cd1576fb70dc97ce8e9e61da493d62`, linux/amd64 manifest `sha256:2a1c9659d6f00a8c891e901835054c12cdc913a91167717d9d508c3ca1a95653`, config `sha256:3979dbc8061da903edb50eb7b03b90051c2ba435c2defaca1f8c40c5318763b2` |
| Older producer (fallback row only) | Native Server 3.0.5, source `bc24589db02161a21febbb30211309537fd1e24e`, index `sha256:3cb6e5bf8ec766ac2abd933369b40a77af62794b6d7b921ce21d3fb263e88b72`, linux/amd64 manifest `sha256:2659280f3ad58d1812ce00f44a927beed9781237143b8487ef12e1301eed551e`, config `sha256:0abb6928655d3a56e3ea9fb061f7edefd479a731eade94a30346dde9f7634800` |
| MySQL 8.0 (load) | `docker.io/library/mysql@sha256:62fb722c78b24245ddff1796a0fcee4a49cc5b87e0aaaf20c92d1da9e0a2497b`, config `sha256:6cd09145362dfe6831b14545de3d5fd6cc75c37cfd6ef8561429c1fc73518b39`, 8.0.46 |
| PostgreSQL 16 (load) | index `sha256:f9a7b9ebf6f56201f949db745e47bc423c0f0f8294bafe991cc10bf74b029657`, amd64 manifest `sha256:43f9275df91e012c3da3d86bea5d7bb5e67a473296cc906c5f9e80b8176b1a37`, config `sha256:ba07bdb15f0f2ac1ba7a88891da731cae65ac36d663fdb7de8f698ea47e0e381`, 16.15 |
| Composer | public PHAR SHA-256 `7a2d379d5b8ffdaa028580ef26494c36d2feef4b178d3dd1473a4dbc5e17c8d6`, CA bundle SHA-256 `50f79f98dac192b20dbf73f35c56f0b0c557ab267ff5199d1117cc1180b70785`, canonical verifier source `242c` run before each Composer request |
| Embedded framework support | Workflow `^2.21`; 2.21.0 authority `cb9420019476d400e1df329f063cfb1a3b86ab18` stays in force until General hands off a published 2.21.1 |
| Host | the selected linux/amd64 lab host, confirmed with a fresh capacity and Docker sample at each admission; `$CPQ` is one owner-created private working directory (mode 0700) on it, named only in the effect decision |

## Phases

Every container is named `sdk-php-cpq-<phase>-<cell>`, carries the label
`durable-workflow.owner=sdk-php`, runs unprivileged with `--cap-drop ALL`,
`--security-opt no-new-privileges`, `--read-only` and a bounded `/tmp` tmpfs,
uses `--pull never` after acquisition, and publishes no host port. One cell
runs at a time. Times below are hard ceilings enforced with `timeout`; a
breach stops the phase.

### Phase P — runner PHP image, build only (effect)

The runner needs `pcntl`, which the cached official PHP 8.4.27 image lacks.
`php-pcntl-build.sh` adds only `pcntl`, compiled from the source tarball the
base image already contains. Phase P runs it in **one ordinary bounded
container** and commits that container. It does not use BuildKit or any
builder cache, so every resource bound is the container's cgroup, and the
container plus the committed image are the only things it owns. This phase
is independent of the producer receipts.

Fixed identities:

| Item | Value |
| --- | --- |
| Base | `php@sha256:dd0e2b902f1edfb6ca02fbcb391c28b1554c7d35a4e36e5925876b90aa804851` (OCI image manifest, 2,878 B, linux/amd64, Debian 12, created 2026-10-28T01:25:32Z, ten layers ending `c500804b…`); PHP 8.4.27 NTS, non-debug, API `20240924`; modules include `curl` and `posix`, not `pcntl` |
| Bundled source | `/usr/src/php.tar.xz`, SHA-256 equal to the image's `PHP_SHA256=dcf93923b0e38e0d5252b25b796520b5a47761a08b3488b6d0b5d1115764d3f0` |
| Build script | `scripts/conformance/php-pcntl-build.sh`, 1,167 B, SHA-256 `6ec44aaaa1cc287b4345286623cd9ffbca8ba4b02e78eff434f67398b07d057b`, taken from its exact Git blob |
| Expected `pcntl.so` | 70,128 B, SHA-256 `caeb0d94d6b8b1853aed199acd575dac92d1bb74e6b3de2cfba56befdcdfa8de`. The script refuses any other result before installing it. |
| Expected ini | `extension=pcntl\n`, SHA-256 `93099d97d237257a925ee62ecf79886c2afa5bfc6731006ac631f2c622db7677` |
| Controller | `scripts/conformance/build-php-pcntl-image.sh`, 12,800 B, SHA-256 `6a987628fc427f34a27a6bcf954e56c55abd5ceb570fce72f959452c4c4ce5e5`, taken from its exact Git blob |
| Container | `sdk-php-cpq-p-build`, label `durable-workflow.owner=sdk-php` |
| Tag | `sdk-php-runner-php:8.4.27-pcntl-6ec44aaaa1cc` |

Steps, one owner, in order:

1. Fresh capacity and Docker sample on the selected host: at least 4 free
   CPUs, 4 GiB available memory and 2 GiB free Docker storage.
2. Stage `php-pcntl-build.sh` and `build-php-pcntl-image.sh` from their exact
   Git blobs, create one empty owned work directory, and run:
   `timeout 900 scripts/conformance/build-php-pcntl-image.sh php-pcntl-build.sh <work-dir>`.
   The controller is the reviewed executable for this phase. The outer
   `timeout 900` bounds the whole controller, including cleanup.

The controller:

- **Preflight, with no Docker write.** It checks:
  - the build script bytes (`6ec44aaa…`), read **once**: the same bytes are
    hashed and passed to the container;
  - that the work directory is owned and empty;
  - the base image's exact identity, ten layers and configuration;
  - that neither container name (`sdk-php-cpq-p-build`, `sdk-php-cpq-p-verify`)
    nor the tag exists.

  Any refusal exits 2.
- **Three-state probes.** An object is *absent* only when Docker answers with
  an explicit "No such container/image/object" error. A timeout, daemon error
  or capped output is *unknown*. Unknown refuses in preflight and counts as
  residue in cleanup. It is never treated as absence.
- **Bounded commands.** Every Docker command runs under its own wall bound
  (create 60 s, start 300 s, inspect/diff/tag/probe 30 s, commit 120 s,
  verification runs 60 s, removals 30–60 s). Its stdout and stderr go to
  capped files in the work directory: 1 MiB for the build log and 64 KiB for
  everything else. Output over a cap counts as failure. This bounds the bytes
  delivered by `docker start -a`, not just the daemon's log file.
- **Markers before effects.** `marker.create` is written before
  `docker create`, and `marker.commit` before `docker commit`. Cleanup acts on
  the markers, so an unknown create or commit outcome is still cleaned up.
- **Exact ownership.** Each run gets a random run identity, attached as the
  label `durable-workflow.phase-p-run=<run>` to the build container, the
  committed image and the verify container. Before removing anything, cleanup
  resolves the container name or tag to an ID and reads that object's labels.
  It removes only objects carrying this run's exact label, by ID, then proves
  each ID absent. Under its name or tag, a foreign object is left untouched.
  Nothing is pruned.
- **Build.** One container with cgroup bounds:
  - `--cpus 2 --memory 1g --memory-swap 1g --pids-limit 256`;
  - network none, all capabilities dropped, no-new-privileges;
  - a 512 MiB `/tmp` tmpfs;
  - a 1 MiB json-file log.

  The script is passed as the `sh -c` argument, so no mount and no
  interactive-stdin flag enters the container or the committed image. A start
  timeout kills the container.
- **Before commit.** Exit code 0 and no OOM; `SizeRw` ≤ 1 MiB; `docker diff`
  exactly the two files and their parent directories.
- **After commit.**
  - The tag names the committed image ID.
  - `Env`, `Entrypoint`, `Cmd`, `User` and `WorkingDir` equal the base's.
  - The labels are exactly owner plus run.
  - The layers are the base's ten plus exactly one.
  - A network-none, read-only, uid-1000 container from the image reports, in
    order: `pcntl.so` `caeb0d94…`, ini `93099d97…`, `pcntl` loaded once, the
    base's own `php-config --configure-options` hash, and fork/wait child exit 7.
- **Outcome.**
  - Exit 0: the receipt JSON (image ID, tag, run, hashes, `SizeRw`, layers)
    goes to stdout, and only the image is retained.
  - Exit 1: any failure after an effect. The container, verify container and
    any run-labeled image are removed and proven absent.
  - Exit 3: a removal cannot be proven. This is residue: it never reports as
    success or as a clean failure.

Controls:
- **Fake Docker CLI** (`tests/PhpPcntlImageControllerTest.php`, 28 cases), each
  checking exit code, stdout and remaining containers, images and tag:
  - success, plus a check that the consumed `-c` script equals `6ec44aaa…`;
  - seven preflight refusals with no Docker write: foreign tag, foreign
    container name, base identity, changed script, non-empty work directory,
    raised start bound, and a daemon error while probing names (not absence);
  - twelve post-effect failures with no residue and no false success:
    partial create, build failure, wall timeout (container killed), output
    flood, oversized writable layer, extra diff path, commit failure, partial
    commit, configuration drift, label drift, extra layer, failed pcntl check;
  - a foreign container holding the build name after a failed create: exit 1,
    no removal attempted, foreign container untouched;
  - five residue cases → exit 3: container removal fails; container state
    unknown after removal; image listing fails; image removal fails; image
    state unknown after removal.
- **Real Docker on the selected host, stopping before any commit:**
  - a 2-second start bound against the ~6-second build gave exit 1 and killed
    the container. Cleanup resolved the name to an ID and matched this run's
    label, removed the container by ID, and proved it absent from Docker's
    "No such container" answer. No container or image was left.
  - a foreign container already holding the build name gave exit 2, with no
    create attempted and the foreign container untouched.

Nothing is pushed or published, and the image never gets a public registry name.

Candidate control on the selected host (this exact create/start primitive, without
commit, so no image was produced):
- 6 s;
- exit 0, no OOM;
- limits read back as 2 CPUs / 1 GiB memory and swap / 256 PIDs / network none / all capabilities dropped / `/tmp` 512 MiB;
- `SizeRw` 147,456 B;
- `docker diff` exactly the two files and their parent directories;
- `pcntl.so` `caeb0d94…`, ini `93099d97…`;
- container removed, nothing left.

An earlier variant mounted the script and left a `/phase-p` mount-point stub
in the diff. Passing it on standard input removed that stub.

### Phase A — acquisition (effect)

1. Pull the four images above by digest only. Record each local image ID and
   `docker image inspect` config digest. A local image ID that is a platform
   manifest, not a config, must be linked to the listed config by manifest
   read. An alias is not accepted.
2. Bounds: 300 s total, network to Docker Hub and GHCR only.
3. Cleanup: none (images are retained for the run, then removed in phase F).

### Phase B — consumer root assembly (effect)

One held root at `$ROOT=$CPQ/consumer` (owner-created, mode 0700).

1. Stage the 22 sealed runner files into `$ROOT` from the exact reviewed Git
   blobs: for each path in `CompletionPollSubject::RUNNER_FILES`,
   `git cat-file blob "$RUNNER_REF:<path>" > "$ROOT/<path>"`. Do not use
   `git archive`, which honors `/tests export-ignore` and drops the six
   `tests/Support` runner files. Do not take them from the installed
   SDK distribution, which excludes `tests/`, or from a source checkout's
   vendor tree. Recompute the canonical runner manifest and require
   `$RUNNER_MANIFEST_SHA256` before PHP is started.
2. Stage the Composer trust inputs, all read-only, outside `$ROOT`:

   | Input | Exact identity |
   | --- | --- |
   | Composer PHAR | the retained authenticated Composer PHAR, 3,642,137 B, SHA-256 `7a2d379d5b8ffdaa028580ef26494c36d2feef4b178d3dd1473a4dbc5e17c8d6`; the verifier requires Composer 2.10.3 or newer on the 2 line |
   | CA bundle | the retained authenticated CI CA bundle, 238,856 B, SHA-256 `50f79f98dac192b20dbf73f35c56f0b0c557ab267ff5199d1117cc1180b70785` |
   | Extra-roots directory | one owned **empty** directory |
   | Verifier | `durable-workflow/.github` `.github/actions/verify-composer-trust/verify.php` at `242c2a7fb23459db2e1ada5f84dbee6d61d9b1e5`: blob `1783f0620e585c4c4998744216bd8fce418de637`, 5,969 B, SHA-256 `d7eea290e04e4eef382cc977584a736b1781b18994c5f462f600c0c47e8c5371` |

   The phase-B effect decision names the retained sources privately; they are
   never downloaded again. Copy the PHAR and CA bundle into an owned held
   directory `$CPQ/trust` (mode 0500):
   1. Refuse unless each source is a regular file and not a symlink (`test -f` and `! test -L`).
   2. Copy with `cp --no-dereference`.
   3. Check each copy's byte count and SHA-256 against the table.
   4. `chmod 0444`.

   Environment for every Composer process:
   - `SSL_CERT_FILE`, `CURL_CA_BUNDLE`, `GIT_SSL_CAINFO` and `COMPOSER_CAFILE` all name the CA bundle.
   - `SSL_CERT_DIR`, `GIT_SSL_CAPATH` and `COMPOSER_CAPATH` all name the empty directory.
   - `DW_CI_CA_SHA256=50f79f98…0785`.
   - Owned `COMPOSER_HOME`, `COMPOSER_CACHE_DIR` and `TMPDIR`.
   - `COMPOSER_NO_INTERACTION=1`.
   - No `COMPOSER_AUTH`, `COMPOSER` override, `GITHUB_TOKEN`, proxy, `REQUESTS_CA_BUNDLE`, `PIP_CERT`, `NPM_CONFIG_CAFILE` or `CARGO_*` CA variables.
   - Run as uid 1000; no `COMPOSER_ALLOW_SUPERUSER`.

   Immediately before each Composer request, run
   `php verify.php "$PHAR" "$ROOT"` and require exit 0 with `sha256` equal to
   the CA hash above. It refuses a different bundle, any non-empty extra-roots
   directory, disabled TLS or a different effective `cafile`/`capath`.
3. Write `$ROOT/composer.json`:
   `{"require":{"durable-workflow/sdk":"3.10.0"},"config":{"allow-plugins":false,"audit":{"block-insecure":true}}}`.
   No `repositories`, `scripts`, `minimum-stability` or platform overrides.
4. In container `sdk-php-cpq-b-install` (`$PHP_IMAGE`, 2 CPU, 2 GiB, 256 PIDs,
   network to `repo.packagist.org`, `api.github.com`, `codeload.github.com`
   only, owned `COMPOSER_HOME`, cache and `TMPDIR` under `$CPQ/composer`,
   no auth/proxy/token variables, trust environment from step 2): run the verifier, then
   `php composer.phar install --no-dev --no-plugins --no-scripts --no-interaction --prefer-dist`
   (after `require`, with the verifier before each request). 240 s active, 30 s
   cleanup.
5. Record: lock SHA-256 (`$LOCK_SHA256`); the Packagist row and its
   dist reference/URL; the dist ZIP from the owned cache and its SHA-256 and
   size; the ZIP's package-relative inventory in the canonical form, which must
   equal `$SDK_INVENTORY_SHA256` computed from `vendor/durable-workflow/sdk`;
   and the canonical `vendor/` inventory (`$VENDOR_INVENTORY_SHA256`).
6. `chmod -R a=rX "$ROOT"`: readable by everyone, including the runner's uid
   10001, and writable by no one. Every later container mounts it read-only at
   `/consumer` only as the source of its private copy (phase D step 2). This
   host directory is not the execution boundary.

### Phase C — subject packets (no effect)

Fill `completion-polls-subject.template.json` once per cell from the
[cell table](#cells). Common values: `runner_reference=$RUNNER_REF`,
`sdk_version=3.10.0`, `sdk_reference=$SDK_TAG_COMMIT`, `lock_sha256`,
`runner_manifest_sha256`, `vendor_inventory_sha256`, `sdk_inventory_sha256`
from phase B, `namespace=default`, `endpoint=http://127.0.0.1:8080`.
`admission_expires_at` is the phase's selection time plus its ceiling below,
never more than 7,200 s ahead. Record each packet's SHA-256.

### Phase D — functional cells (effect, 18 invocations)

Per cell, in table order:

1. Start `sdk-php-cpq-d-<n>-server` from the cell's producer image with
   `DB_CONNECTION=sqlite`, `DB_DATABASE=/app/storage/database.sqlite` on a
   per-cell tmpfs `/app/storage`, `DW_AUTH_DRIVER=token`, and fresh random
   `DW_AUTH_TOKEN`, `DW_OPERATOR_TOKEN` and `DW_WORKER_TOKEN` (32 bytes each,
   held only in the owner's process environment). `--network none`: the
   runner joins this container's loopback, and nothing else can reach it.
   2 CPU, 2 GiB, 256 PIDs. Wait for `/api/ready` 200 through the image's own
   bash `/dev/tcp` health probe (`docker exec`), startup ceiling 60 s.
2. Run `sdk-php-cpq-d-<n>-runner` from `$PHP_IMAGE` (by image ID) with:
   - `--network container:sdk-php-cpq-d-<n>-server`, so `127.0.0.1:8080` is the cell's Server;
   - `--read-only --cap-drop ALL --security-opt no-new-privileges`;
   - `--user 10001:10001`, a uid with no account on the host (check `getent passwd 10001` is empty at admission);
   - `/consumer` and the packet directory as read-only binds;
   - a container-private execution root
     `--tmpfs /held:rw,exec,nosuid,nodev,size=512m,uid=10001,gid=10001,mode=0700`;
   - `COMPLETION_TEST_CLIENT_TOKEN=$DW_OPERATOR_TOKEN` and `COMPLETION_TEST_WORKER_TOKEN=$DW_WORKER_TOKEN`.

   The command copies the consumer root into the private tmpfs and runs only
   from that copy:

   `sh -c 'cp -R /consumer/. /held/ && chmod -R a-w /held && exec php /held/scripts/conformance/qualify-completion-polls.php /held/vendor/autoload.php /packets/<n>.json <sha256>'`

   This is the execution boundary. The tmpfs exists only in this container's
   mount namespace. A host file writer cannot reach it: it would need root, or
   the runner's uid (10001), which no host user has. The runner then:
   - refuses unless its root is exactly such a tmpfs mount with nothing mounted
     beneath it (`completion_admission_boundary`);
   - authenticates every copied byte (`admit()`);
   - re-hashes the runner, lock and full vendor inventory immediately before
     requiring the autoloader, when reading installed identity, before every
     router launch, and before reporting a result (`completion_admission_changed`).

   A change to `/consumer` during the copy is caught by admission. A change
   after the copy cannot reach `/held`.

   Limits: 2 CPU, 2 GiB (the tmpfs counts against it), 256 PIDs. Ceiling:
   deadline + 30 s cleanup.
3. Stop and remove both containers and the network (teardown ceiling 30 s).
   Prove absence by name and label.

Per-cell ceiling 240 s; phase ceiling 18 × 240 s = 72 min, under one
7,200 s authority.

### Phase E — load cells (effect, 36 invocations)

Three sub-phases, one per backend, each 12 cells (4 scenarios × 3
repetitions) with its own selection and authority (12 × 240 s = 48 min).

- **SQLite**: as phase D, fresh tmpfs database per cell.
- **MySQL**: per cell, an owned `docker network create --internal
  sdk-php-cpq-e-<n>-net`, then `sdk-php-cpq-e-<n>-mysql` on it (1 CPU, 1 GiB,
  128 PIDs, tmpfs data dir) with fresh random credentials, then the Server on
  the same network (instead of `--network none`) with `DB_CONNECTION=mysql`, `DB_HOST`, `DB_PORT=3306`,
  `DB_DATABASE`, `DB_USERNAME`, `DB_PASSWORD`.
- **PostgreSQL**: as MySQL with `sdk-php-cpq-e-<n>-postgres` and
  `DB_CONNECTION=pgsql`, `DB_PORT=5432`.

`load.database_identity` is the per-cell database container name;
`load.conditions_sha256` is the SHA-256 of the recorded host/resource/durability
conditions JSON for that sub-phase. Load cells use direct runtime calls (no
relay), as the runner already does.

After each backend, run `compare-completion-poll-load.php` twice (local and
remote): three `off` then three `on` result files with their SHA-256 values.
The comparator reports descriptive ratios with
`performance_acceptance: not_assessed`. No speedup or release threshold is
claimed.

### Phase F — cleanup (effect)

Remove every `sdk-php-cpq-*` container, network and volume, `$CPQ`,
and the four images if they were not present before phase A. Prove absence with
`docker ps -a`, `docker network ls` and `docker volume ls` filtered by the
owner label, and record a final Docker sample.

## Outcomes

A product assertion failure is `product-fail`. Unavailable observations,
process errors, a breached ceiling or incomplete cleanup are `runner-blocked`.
Neither passes. On either, stop the phase, keep its original evidence and
diagnose without an automatic retry. A repeat needs a fresh selection.

## Cells

Functional (phase D, SQLite):

| # | Scenario | Producer | Deadline (s) |
| ---: | --- | --- | ---: |
| 1 | `carriers` | 3.2.0 | 60 |
| 2 | `pipeline` | 3.2.0 | 60 |
| 3 | `pipeline-loss` | 3.2.0 | 60 |
| 4 | `fallback` | **3.0.5** | 60 |
| 5 | `capacity` | 3.2.0 | 60 |
| 6 | `carrier-loss-activity` | 3.2.0 | 60 |
| 7 | `carrier-loss-workflow` | 3.2.0 | 60 |
| 8 | `carrier-loss-workflow-completion` | 3.2.0 | 60 |
| 9 | `completion-transit` | 3.2.0 | 120 |
| 10 | `carrier-expiry` | 3.2.0 | 120 |
| 11 | `carrier-refence` | 3.2.0 | 120 |
| 12 | `pipeline-buffer-age` | 3.2.0 | 120 |
| 13 | `prefetch-loss` | 3.2.0 | 60 |
| 14 | `default-off` | 3.2.0 | 60 |
| 15 | `explicit-off` | 3.2.0 | 60 |
| 16 | `packet-defaults` | 3.2.0 (fixed socket; `runtime_observed: false`) | 60 |
| 17 | `opposite-slots` | 3.2.0 | 60 |
| 18 | `workflow-refence` | 3.2.0 | 60 |

Load (phase E): `load-local-off`, `load-local-on`, `load-remote-off`,
`load-remote-on` × repetitions 1–3 × SQLite, MySQL, PostgreSQL, all on 3.2.0
with deadline 120 s. Cells 19–54.

Total: 54 invocations across 22 scenario families.

## Remaining values

These are the only values not yet available. Everything else above is fixed.

| Value | Source |
| --- | --- |
| `$RUNNER_REF` | Merge commit of this runner change on main. `$RUNNER_MANIFEST_SHA256` is already fixed at `94a9604fa33f029c7f6b37b2a6889dd9268cb6cc8ba5f47747cce595474a614a` and must recompute equal at that commit. |
| `$SDK_TAG_COMMIT`, Packagist row, dist ZIP SHA-256/size/inventory | General's 3.10.0 tag and release, then normal Packagist availability. Proposed: tag the same main commit as `$RUNNER_REF`, so one commit serves as both the SDK release and the runner source. |
| `$LOCK_SHA256`, `$VENDOR_INVENTORY_SHA256`, `$SDK_INVENTORY_SHA256` | Phase B output |
| Native 3.2.0 qualifier acceptance | Server's corpus run 42407065494 and the four Compose receipts, with independent acceptance |
| SQLite and PostgreSQL Server settings | Confirm `DB_CONNECTION=sqlite`/`pgsql` and their variables against the accepted Compose receipts. The published Compose file shows only the MySQL settings. |
| `default` namespace on 3.2.0 and 3.0.5 | Confirm it exists without bootstrap, from the same receipts |
| `$PHP_IMAGE` | Image ID and tag from phase P (General chose the derived-image path) |
| Per-phase selections | General's fresh selection with literal windows for phases P, A, B, D, E (×3) and F, after independent review |
