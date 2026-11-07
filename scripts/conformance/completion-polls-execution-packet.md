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
| Controller | `scripts/conformance/build-php-pcntl-image.sh`, 12,883 B, SHA-256 `003c415e8f2c63eab54a60b457a847b2c40cf04b0c208d4b9bc0d3b716c2fec3`, taken from its exact Git blob |
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
  committed image, and both verification containers (image check and base
  check). `docker create` and both `docker run` checks pass `--pull=never`, so
  the daemon cannot pull an image even if one disappears after preflight. Before removing anything, cleanup
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
- **Fake Docker CLI** (`tests/PhpPcntlImageControllerTest.php`, 31 cases), each
  checking exit code, stdout and remaining containers, images and tag. The fake
  refuses any `create` or `run` without `--pull=never`.
  - success, plus checks that:
    - the consumed `-c` script equals `6ec44aaa…`;
    - all three container commands carry `--pull=never` and this run's label;
  - an image check or base check that fails and leaves its container: exit 1,
    with the container removed by its run label;
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

Run `timeout 330 scripts/conformance/acquire-cell-images.sh <empty-owned-work-dir>`,
staged from its exact Git blob, with the same clean environment as phase P
(fixed system `PATH`, empty owned `DOCKER_CONFIG`, local daemon, no
`PHASE_A_*` overrides). The script pulls each image by its exact linux/amd64
**platform manifest** digest:

| Name | Pull reference (platform manifest) | Config | Index | Layers | Compressed |
| --- | --- | --- | --- | ---: | ---: |
| server-3.2.0 | `ghcr.io/durable-workflow/server@sha256:2a1c9659…1a95653` | `3979dbc8…63b2` | `6916f6f3…3d62` | 4 | 50,034,939 |
| server-3.0.5 | `ghcr.io/durable-workflow/server@sha256:2659280f…eed551e` | `0abb6928…4800` | `3cb6e5bf…8b72` | 4 | 49,469,666 |
| mysql-8.0.46 | `docker.io/library/mysql@sha256:62fb722c…2497b` | `6cd09145…8b39` | — | 11 | 233,581,538 |
| postgres-16.15 | `docker.io/library/postgres@sha256:43f9275d…b1a37` | `ba07bdb1…e381` | `f9a7b9eb…9657` | 14 | 160,301,047 |

The script:

1. **Reads metadata only before any pull.** For each image it reads the raw
   manifest from the registry (`docker buildx imagetools inspect --raw`) and
   requires:
   - its SHA-256 equals the pinned digest;
   - it is a single-platform image manifest, not an index;
   - its `config.digest` equals the pinned config.

   This is the manifest-to-config link: Docker's containerd image store does
   not report the config digest locally.
2. **Probes each local reference three-state.** Unknown refuses. Present
   means pre-existing: it is not pulled and is never owned or removed.
3. **Checks free storage.** Docker storage must have at least 3 × the total
   compressed bytes plus 2 GiB free.
4. **Pulls each absent image once** (`docker pull --quiet --platform linux/amd64 <ref>`).
   - Each pull is bounded by the time left in the 300-second total; the whole
     script is also wrapped in `timeout 330`.
   - Output is capped at 64 KiB.
   - A pull marker naming the exact reference is written before each pull.
   - There is no controller retry. The daemon's own per-request transport
     retries are not controllable from the CLI and are not claimed absent.
5. **Verifies each local image:** ID and `Descriptor.digest` equal the
   manifest digest, linux/amd64, layer count equal to the manifest's.
6. **On any failure,** removes exactly the references this run pulled
   (`docker image rm <ref>`) and proves each absent; otherwise it exits 3.
   Partial layer content that a failed pull left in the shared containerd
   store has no per-run identity. It is neither claimed removed nor pruned:
   containerd's own garbage collection owns it, and its bound is the total
   compressed size above.

Exit 0 prints a receipt (per image: reference, manifest, config, index,
layers, pulled or pre-existing). Exit 2 means refused before any pull, 1 a
clean failure, and 3 residue.

**Metadata-only check on the selected host** (`--metadata-only`, pulls nothing):
all four manifests matched their digests and configs; total compressed
493,387,190 B; free Docker storage 896 GB; all four images absent locally.

**Controls** (`tests/AcquireCellImagesTest.php` with `tests/Support/fake-docker-acquire.sh`).
The fake replays the four real captured manifests from `tests/fixtures/phase-a`,
whose SHA-256 values equal the pinned digests.
- **Metadata-only:** no pull.
- **Success:** four exact manifest pulls on linux/amd64.
- **Pre-existing image:** neither pulled nor removed, even after a later pull fails.
- **Refusals with no pull:** tampered manifest bytes; unknown local state; a raised total bound.
- **Failures that leave nothing this run pulled:** a pull that fails after
  creating its reference; a pull past the total bound; a later pull failing;
  local identity differing from the manifest.
- **Residue (exit 3):** removal fails; state unknown after removal.

All of these ran with the identical fake on the selected host. The PHPUnit
form needs `python3`, as the script does.

The four images are retained for phases D–E and removed in phase F by exact
reference, but only those this run pulled.

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

1. Start `sdk-php-cpq-d-<n>-server` from the cell's producer image, by its
   manifest digest, with `--pull=never --read-only --cap-drop ALL
   --security-opt no-new-privileges` and the owner label. The settings below
   come from the frozen Server source (3.2.0 `3c6f1eda`, 3.0.5 `bc24589d`):
   `rust/src/main.rs` serve path, `Dockerfile.native`, `rust/src/runtime/namespaces.rs`.
   - **User and mounts.** The image runs as `1000:1000` and declares
     `VOLUME ["/app/database", "/app/storage"]`. Mount both as owned tmpfs:
     `--tmpfs /app/database:rw,nosuid,nodev,size=256m,uid=1000,gid=1000,mode=0700`
     and `--tmpfs /app/storage:rw,nosuid,nodev,size=64m,uid=1000,gid=1000,mode=0700`.
     With every declared volume path covered by a tmpfs, Docker creates no
     anonymous volume. The cell checks `.Mounts` shows only `tmpfs` types,
     and removes any `volume` mount by its exact name at teardown.
   - **Database.** `DB_CONNECTION` defaults to `sqlite`, and the image sets
     `DB_DATABASE=/app/database/database.sqlite`. Set both explicitly
     anyway. The schema is created at startup.
   - **Auth.**
     - `DW_AUTH_DRIVER=token`; a release build refuses `none`.
     - `DW_AUTH_TOKEN` is required, must not be empty, and is accepted for every role.
     - Optional role tokens: `DW_WORKER_TOKEN` and `DW_OPERATOR_TOKEN`.
     - Each token is fresh, random and 32 bytes, held only in the owner's
       process environment.
     - Release builds need no `DW_RUST_EXPERIMENTAL`.
   - **Namespace.** Runtime open seeds the `default` namespace when it is
     absent, in both versions (`initialize_namespace_registry`), so
     `namespace=default` needs no bootstrap.
   - **Network.** `DW_BIND_ADDRESS=0.0.0.0:8080` (image default). Use
     `--network none`: the runner joins this container's loopback, and
     nothing else can reach it.
   - **Limits.** 2 CPU, 2 GiB, 256 PIDs. `SIGTERM` stops it gracefully.
   - **Readiness.** Wait for `GET /api/ready` 200 through the image's own bash
     `/dev/tcp` probe (`docker exec`), as the published Compose health check
     at `3c6f1eda` does. Startup ceiling 60 s.
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
repetitions) with its own selection and authority: 12 × 240 s = 48 min for
SQLite, and 12 × 270 s = 54 min for MySQL and for PostgreSQL.

- **SQLite**: as phase D, fresh tmpfs database per cell.
- **MySQL.** Per cell:
  1. `docker network create --internal --label durable-workflow.owner=sdk-php sdk-php-cpq-e-<n>-net`.
  2. Start `sdk-php-cpq-e-<n>-mysql` on that network:
     - `--pull=never --read-only`, the owner label;
     - 1 CPU, 1 GiB, 128 PIDs;
     - `--cap-drop ALL --cap-add CHOWN,SETUID,SETGID,DAC_OVERRIDE,FOWNER --security-opt no-new-privileges`, because the official entrypoint starts as root and drops to the `mysql` user with `gosu`;
     - tmpfs at the declared `VOLUME /var/lib/mysql` (size 1g), plus `/var/run/mysqld` and `/tmp`;
     - fresh random `MYSQL_DATABASE`, `MYSQL_USER`, `MYSQL_PASSWORD` and `MYSQL_ROOT_PASSWORD`.
  3. Start the Server on the same network instead of `--network none`, with:
     - `DB_CONNECTION=mysql`;
     - `DB_HOST=sdk-php-cpq-e-<n>-mysql`, `DB_PORT=3306`;
     - `DB_DATABASE`, `DB_USERNAME`, `DB_PASSWORD`;
     - `DB_SSLMODE=disable`. Unset, the driver default applies; the network
       is internal only, and MySQL accepts `disable`, `require` and
       `verify-full`.

  The Server waits for the database to become reachable at startup. The
  startup ceiling for MySQL and PostgreSQL cells is 90 s, which allows first
  initialization on tmpfs, so their per-cell ceiling is 270 s.
- **PostgreSQL.** The same, with `sdk-php-cpq-e-<n>-postgres`:
  - tmpfs at the declared `VOLUME /var/lib/postgresql/data` (size 1g, which
    is `PGDATA`), plus `/var/run/postgresql` and `/tmp`;
  - `POSTGRES_DB`, `POSTGRES_USER` and `POSTGRES_PASSWORD`;
  - Server settings `DB_CONNECTION=pgsql`, `DB_PORT=5432` and `DB_SSLMODE=disable`.

Each database's declared volume path is a tmpfs, so no anonymous volume is
created. Each cell's teardown removes its containers, its network and any
`volume` mount it finds, by exact name, and proves them absent.

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
| `$RUNNER_REF` | **Fixed:** `4e6c6b7b4318010b73233377ad36be805828fedd`, the merge of #354, which General froze as the 3.10.0 target. `$RUNNER_MANIFEST_SHA256` = `94a9604fa33f029c7f6b37b2a6889dd9268cb6cc8ba5f47747cce595474a614a`, recomputed equal there. |
| Tag, Packagist row, dist ZIP SHA-256/size/inventory | General's 3.10.0 tag and release at the frozen target `4e6c6b7b`, after canonical Native 3.2.0 acceptance, then normal Packagist availability |
| `$LOCK_SHA256`, `$VENDOR_INVENTORY_SHA256`, `$SDK_INVENTORY_SHA256` | Phase B output |
| Native 3.2.0 qualifier acceptance | Server's corpus run 42407065494 and the four Compose receipts, with independent acceptance |
| `$PHP_IMAGE` | **Fixed:** `sha256:80a91784a0ae46c4cecd33b72f30188285fc32be8087af7caa7fa00b0fe8a5f0` (phase P PASS) |
| Per-phase selections | General's fresh selection, with literal windows, for phases A, B, D, E (×3) and F, after independent review. Phase P is done. |
