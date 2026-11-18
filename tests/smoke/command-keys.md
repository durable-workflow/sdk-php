# Installed PHP command-key adapter

`command-key-client.php` reads one JSON packet from stdin and performs one
fixture operation through the public PHP SDK API. The selected immutable SDK and Server tuple lives
in `command-key-supply.json`. Every result identifies the installed package
reference, runtime version and PHP version. The adapter bounds input to 64 KiB,
accepts only a numeric loopback origin, and restricts fixture workflow and
queue names to the `sdk-php-216-` prefix.

Before invocation, the owning acceptance runner verifies the complete registry
archive member digest, Composer lock/source/dist identities and installed SDK file bytes
against that selected supply. It owns the isolated runtime and endpoint,
synthetic credential, namespace provisioning, finite process timeout and exact
cleanup. Mount or capture the reviewed adapter, supply and autoloader inputs
read-only. The adapter's reflection checks reject a repository source fallback;
the runner supplies the independent archive/provenance proof.

`verify_command_key_supply.py <consumer-directory> <downloaded-archive>`
performs the package inspection before PHP loading. It checks the exact root
requirement, absence of repository overrides, lock/source/dist URLs and refs,
dist installation, the approved digest of every archive path/body/mode and every installed SDK file. It
accepts private umasks, normalizes GitHub's DOS nonexecutable entries and
rejects added permissions, missing/extra/changed files or aliases. Its output
is explicitly inspection-only. Exact commit and member identities are the
durable binding because GitHub can regenerate ZIP wrapper bytes; each download's
raw ZIP hash is still retained as an observation. Changed, added or missing
members and changed executable modes refuse. Run its inert controls with
`python3 -I -S -B tests/ci/test_command_key_supply.py`.

The controller also retains that approved archive member map in memory and
passes it into staging. Each SDK source descriptor is checked against the
approved path/body/mode policy before copying. Expected SDK bytes are never
derived from a later read of the mutable install tree. Staging assigns fixed
read-only modes from the approved modes, checks all members again after the
copy, and rejects missing or extra SDK files and directories. Every subsequent
root-controlled inventory rechecks the sealed SDK against the same digest from
the captured supply before container creation and around each invocation.
The fixture map comes independently from captured Git bytes and is checked
during copying and sealed-root checks too.

Set `COMMAND_KEY_RUNTIME_URL` to the owned numeric loopback URL and
`COMMAND_KEY_TOKEN` to its synthetic credential. Pass the registry consumer's
`vendor/autoload.php` as the first argument. Packet fields are:

- `operation`: `bootstrap`, `drive`, `signal`, `update`, `read`, `value`,
  `history` or `describe`.
- `workflow_id`, optional `run_id`, and optional `namespace` (`default` or an
  owned fixture prefix).
- `task_queue` for `bootstrap`/`drive`, and `mode` (`open`, `close`, `continue`)
  for bootstrap. The real Worker waits for `release`, completes with null, or
  continues once into an open run. Each processed workflow/update asks that
  Worker to shut down. The outer process timeout also bounds an empty queue.
- `name`, optional `request_id`, and either a list of `arguments` or an
  official Avro `arguments_envelope` for signal/update calls. `signal` and
  `other` are declared; `release` takes no arguments. Both update handlers
  return null. Omit a key to test the SDK's generated key and typed error.
- `update_id` and optional `wait_for` for retained reads. `read` returns the
  receipt; `value` preserves typed pending versus completed null.

Results select admission/read identities and flags rather than arbitrary
response fields, headers or payloads. A transport failure is an error with an
unknown admission outcome; it is never described as a refused admission.
History output contains event counts and selected signal/update admission
identities. It excludes argument values and arbitrary event payloads.

Use the accepted shared committed-response-loss fixture unchanged to prove
response loss. Use the accepted portable SQLite helper and its adjacent exact
Server manifest for synthetic legacy copies. The producer-owned physical seed
and upgrade profile stay separate and must never pass through the synthetic
transform. The modern controller supplies explicit four-way retries, changed
intent/name/run conflicts, pending/null reads, closed/continued retained reads,
Unicode character limits and generated-key recovery after a fully consumed
upstream response is deliberately lost. It compares history admissions for
these cases. Synthetic legacy, physical upgrade, namespace isolation and full
database snapshot proofs remain separate work. No complete shared216 result
follows from the modern controller alone.

`published-command-keys.yml` is a manual, main-only ordinary GitHub Actions
cell. It uses the qualified stock PHP helper, private Composer state and the
real Composer trust guard. Installation disables scripts, plugins and autoload
generation until archive/installed-file inspection passes. The runner captures
its exact committed source and the unchanged accepted shared loss fixture
before loading them. Source checks and independent review precede execution.

The proposed workflow has three phases. Staging inspects the installed package
and captures its complete consumer/vendor/fixture include tree without loading
PHP. A root-controlled directory receives the verified copy. Preparation
creates the selected linux/amd64 Server and an idle, unprivileged PHP consumer.
Both have read-only root filesystems, 1-CPU/1-GiB/256-PID caps and bounded tmpfs.
Creation explicitly selects linux/amd64. Classic image stores must report the
selected config digest. Multi-platform stores may report the exact selected
index instead, with its matching index descriptor; the created container must
then report the independently pinned linux/amd64 platform manifest before it
starts. A present manifest descriptor must always agree, even when the config
digest matches. Missing, conflicting or other-platform identities refuse.
The PHP consumer mounts the entire include tree read-only at `/closure`; only
its separate `/tmp` is writable. Cohort execution invokes the adapter inside
that consumer and checks the complete include inventory, mount configuration,
image identity and captured runtime endpoint before and after each operation.

Each phase loads the controller from exact Git bytes through an independently
fixed literal bootstrap. Preparation captures full IDs from Docker create
stdout, keeps an exclusive private record descriptor open, and publishes the
IDs to workflow outputs before executing package code. The separate literal
always-run cleanup uses those outputs, a fresh empty Docker configuration and
owner/role/nonce labels. It needs no checkout, controller import or state file.
If create loses its returned ID, cleanup reconciles the exact three labels with
a role-specific list limited to two candidates. Multiple candidates, malformed
IDs or conflicting inspection identity/labels refuse. Both roles are attempted
independently, and each role's labels must match no remaining container before
its absence is reported.
It attempts both removals, preserves foreign replacements, and accepts only an
exact missing-object response as proof of absence. Root-stage removal also
requires the original root-owned directory device/inode. Sanitized evidence
and the dependency lock are retained for seven days.

Run the inert controls with:

```sh
python3 -I -S -B tests/ci/test_command_key_cohort.py
python3 -I -S -B tests/ci/test_command_key_containers.py
python3 -I -S -B tests/ci/test_command_key_include_closure.py
```

They exercise synthetic inspection metadata, captured-ID substitution,
independent cleanup, include inventories and receipts without creating a
runtime or loading PHP.

`command_key_include_closure.py` inventories every
consumer/vendor/fixture leaf, rejects aliases, hardlinks, repository metadata
and unexpected root members, and copies verified bytes with read-only modes.
Its output includes a complete member/hash inventory. The workflow wiring is
proposed source, with installed execution on hold. Actual nested-file
overwrite/rename controls, a writable-mount countercontrol, full consumer
profile qualification and independent review are still required. File modes
and the inert inventory tests alone provide no isolation verdict. The proposed
official PHP image identities in the supply are raw-registry metadata bindings;
read-only base-profile observations on the PR do not qualify the SDK consumer.
Exact source and runtime qualification receipts belong on the owning PR.

`tests/ci/test_command_key_mount_boundary.py` proposes two opt-in controls and
an inert argument-factory control. The effectful tests use only generated
include/marker files. One exercises a root-owned sealed tree; the other uses
writable owned files with a read-only mount, then removes only that mount's
read-only qualifier for the negative primitive. The production guard must
refuse the writable mount. The primitive must then show successful marker
overwrite/rename, demonstrating that ordinary file permissions cannot explain
the read-only result. Only the root-owned sealed case includes a generated
constant PHP leaf; the mutable-tree controls execute the literal probe only.
No published SDK, application, repository or host control file is mounted.

The complete intended tree is constructed from the captured fixture constants
before the privilege transition, including every path, kind, mode, size and
body digest. Both staged and root-owned inventories must equal that retained
authority. A fresh inventory of the mutable staging path cannot redefine it.
The inert regression simulates the privileged copy under an owned temporary
directory, replaces a staged leaf after authority selection and proves refusal
before a mocked container creation. It invokes neither sudo nor Docker.

The opt-in tests share the proposed consumer argument factory and the exact
selected image/platform and captured-ID lifecycle checks. They create three
containers sequentially, never concurrently, with the existing 1-CPU/1-GiB/
256-PID limits and host-network setting but no network requests. Image pulling
is disabled. Container absence must be established before owned data or the
root-controlled staging inode can be removed; otherwise those paths are
preserved. Sanitized outcomes include the full IDs and removal results.

Execution remains review-gated. Set `SDK_PHP_COMMAND_KEY_MOUNT_IMAGE` only to
the exact cached `php@<php_index>` from the selected supply, and
`SDK_PHP_COMMAND_KEY_BOUNDARY_SOURCE` to the exact reviewed Git commit. Launch
the test from captured Git bytes through a literal bootstrap that verifies an
independently fixed test digest before compile/exec. The helpers and supply are
also captured from that commit. A direct mutable-path launch does not supply
that outer binding. Default CI runs only the inert factory control and skips
the two effectful tests. Preparing this source does not qualify the actual
mount, dependency profile, cleanup-after-state-refusal or installed cohort.

`tests/ci/test_command_key_cleanup_boundary.py` proposes a separate opt-in
cleanup control. It creates two containers from the selected cached PHP image,
using only the two fixture role labels. One deliberately loses create stdout
and stays unstarted; the other runs a fixed idle PHP command. No native Server, SDK, API or
Worker runs. Both containers have read-only roots, unprivileged users, no
mounts or network, and the existing resource caps. Ownership is flushed before
creation. The missing-ID role is recovered only through label reconciliation;
the test retains only a ground-truth ID hash for comparison, not as cleanup
input. The production guards then refuse an invalid source selection, nonprivate
disposable state and nonempty disposable Docker configuration.

The control then runs the reviewed literal workflow cleanup from captured Git
bytes with an independently fixed digest. Its environment supplies the held
IDs and labels while deliberately invalidating checkout, state, temporary and
ambient Docker configuration values. The cleanup must use its own fresh Docker
configuration and establish both exact absences. A separate observer checks
the full IDs and both role-label sets again before owned fixture data is removed.
An empty captured-ID set cannot establish cleanup. Cleanup refusal or an
incomplete observer preserves that data. Default CI checks the cleanup source
binding and lost/ambiguous create, duplicate/foreign candidate and independent
role cleanup controls, and skips the
container control. Any actual execution requires exact source/entry review,
`SDK_PHP_COMMAND_KEY_CLEANUP_IMAGE` and `SDK_PHP_COMMAND_KEY_CLEANUP_SOURCE`,
and a Git-blob bootstrap with an independently fixed outer test digest. This
control supplies no dependency, consumer profile or installed acceptance.

Run the offline input controls against an existing immutable PHP image:

```sh
SDK_PHP_COMMAND_KEY_IMAGE=sha256:<inspected-image-id> \
  python3 -I -S -B tests/ci/test_command_key_client.py
```

These controls use no network or SDK autoload. They verify malformed operations,
foreign fixture identities, non-loopback origins and oversized packets stop
before package loading. They establish no installed command admission result.
