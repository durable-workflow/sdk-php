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
| Host | lab4, linux/amd64, confirmed with fresh `grafana machines --json` and `grafana docker` at each admission |

## Phases

Every container is named `sdk-php-cpq-<phase>-<cell>`, carries the label
`durable-workflow.owner=sdk-php`, runs unprivileged with `--cap-drop ALL`,
`--security-opt no-new-privileges`, `--read-only` and a bounded `/tmp` tmpfs,
uses `--pull never` after acquisition, and publishes no host port. One cell
runs at a time. Times below are hard ceilings enforced with `timeout`; a
breach stops the phase.

### Phase A — acquisition (effect)

1. Pull the four images above by digest only. Record each local image ID and
   `docker image inspect` config digest. A local image ID that is a platform
   manifest, not a config, must be linked to the listed config by manifest
   read. An alias is not accepted.
2. Bounds: 300 s total, network to Docker Hub and GHCR only.
3. Cleanup: none (images are retained for the run, then removed in phase F).

### Phase B — consumer root assembly (effect)

One held root at `$ROOT=/srv/sdk-php-cpq/consumer` (owner-created, mode 0700).

1. Copy the 22 sealed runner files from `$RUNNER_REF` into `$ROOT`
   (`git archive "$RUNNER_REF" -- <the 22 paths in CompletionPollSubject::RUNNER_FILES> | tar -x -C "$ROOT"`).
   Recompute the canonical runner manifest and require
   `$RUNNER_MANIFEST_SHA256` before PHP is started.
2. Write `$ROOT/composer.json`:
   `{"require":{"durable-workflow/sdk":"3.10.0"},"config":{"allow-plugins":false,"audit":{"block-insecure":true}}}`.
   No `repositories`, `scripts`, `minimum-stability` or platform overrides.
3. In container `sdk-php-cpq-b-install` (`$PHP_IMAGE`, 2 CPU, 2 GiB, 256 PIDs,
   network to `repo.packagist.org`, `api.github.com`, `codeload.github.com`
   only, owned `COMPOSER_HOME`, cache and `TMPDIR` under `/srv/sdk-php-cpq/composer`,
   no auth/proxy/token variables, `SSL_CERT_FILE` and `COMPOSER_CAFILE` set to
   the CA above): run the canonical verifier, then
   `php composer.phar install --no-dev --no-plugins --no-scripts --no-interaction --prefer-dist`
   (after `require`, with the verifier before each request). 240 s active, 30 s
   cleanup.
4. Record: lock SHA-256 (`$LOCK_SHA256`); the Packagist row and its
   dist reference/URL; the dist ZIP from the owned cache and its SHA-256 and
   size; the ZIP's package-relative inventory in the canonical form, which must
   equal `$SDK_INVENTORY_SHA256` computed from `vendor/durable-workflow/sdk`;
   and the canonical `vendor/` inventory (`$VENDOR_INVENTORY_SHA256`).
5. `chmod -R a-w "$ROOT"`. Every later container mounts it read-only at
   `/consumer`.

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
2. Run `sdk-php-cpq-d-<n>-runner` from `$PHP_IMAGE` with
   `--network container:sdk-php-cpq-d-<n>-server` (so `127.0.0.1:8080` is the
   cell's Server), `/consumer` read-only, the packet directory read-only, and
   `COMPLETION_TEST_CLIENT_TOKEN=$DW_OPERATOR_TOKEN`,
   `COMPLETION_TEST_WORKER_TOKEN=$DW_WORKER_TOKEN`:
   `php /consumer/scripts/conformance/qualify-completion-polls.php /consumer/vendor/autoload.php /packets/<n>.json <sha256>`.
   2 CPU, 2 GiB, 256 PIDs. Ceiling: deadline + 30 s cleanup.
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

Remove every `sdk-php-cpq-*` container, network and volume, `/srv/sdk-php-cpq`,
and the four images if they were not present before phase A. Prove absence with
`docker ps -a`, `docker network ls` and `docker volume ls` filtered by the
owner label, and record the final `grafana docker` sample.

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
| `$RUNNER_REF`, `$RUNNER_MANIFEST_SHA256` | Merge commit of this runner change on main and its canonical manifest |
| `$SDK_TAG_COMMIT`, Packagist row, dist ZIP SHA-256/size/inventory | General's 3.10.0 tag and release; normal Packagist availability |
| `$LOCK_SHA256`, `$VENDOR_INVENTORY_SHA256`, `$SDK_INVENTORY_SHA256` | Phase B output |
| Native 3.2.0 qualifier acceptance | Server's corpus run 42407065494 and the four Compose receipts, with independent acceptance |
| SQLite and PostgreSQL Server settings | Confirm `DB_CONNECTION=sqlite`/`pgsql` and their variables against the accepted Compose receipts. The published Compose file shows only the MySQL settings. |
| `default` namespace on 3.2.0 and 3.0.5 | Confirm it exists without bootstrap, from the same receipts |
| `$PHP_IMAGE` | A PHP 8.4 CLI image **with `pcntl`, `posix` and `curl`**, pinned by digest. The cached official `php` 8.4.27 image lacks `pcntl`, and the runner refuses to start without `pcntl_fork`. This needs a decision: an existing pinned image that has it, or a reviewed derived image (`docker-php-ext-install pcntl`) built as its own effect. |
| Per-phase selections | General's fresh selection with literal windows for phases A, B, D, E (×3) and F, after independent review |
