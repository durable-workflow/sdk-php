# Installed PHP command-key adapter

`command-key-client.php` reads one JSON packet from stdin and performs one
fixture operation through the public PHP SDK API. The selected immutable SDK and Server tuple lives
in `command-key-supply.json`. Every result identifies the installed package
reference, runtime version and PHP version. The adapter bounds input to 64 KiB,
accepts only a numeric loopback origin, and restricts fixture workflow and
queue names to the `sdk-php-216-` prefix.

Before invocation, the owning acceptance runner verifies the registry archive
checksum, Composer lock/source/dist identities and installed SDK file bytes
against that selected supply. It owns the isolated runtime and endpoint,
synthetic credential, namespace provisioning, finite process timeout and exact
cleanup. Mount or capture the reviewed adapter, supply and autoloader inputs
read-only. The adapter's reflection checks reject a repository source fallback;
the runner supplies the independent archive/provenance proof.

`verify_command_key_supply.py <consumer-directory> <downloaded-archive>`
performs the package inspection before PHP loading. It checks the exact root
requirement, absence of repository overrides, lock/source/dist URLs and refs,
dist installation, approved archive digest and every installed SDK file. It
accepts private umasks, normalizes GitHub's DOS nonexecutable entries and
rejects added permissions, missing/extra/changed files or aliases. Its output
is explicitly inspection-only. Run its inert controls with
`python3 -I -S -B tests/ci/test_command_key_supply.py`.

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

The controller pulls the selected Server index for linux/amd64 and verifies
its image config/source. It creates one read-only, nonprivileged 1-CPU/1-GiB/
256-PID runtime with private bounded tmpfs and one numeric loopback binding.
Every operation checks the captured container ID/owner/endpoint and installed
SDK bytes again. A private CID file supports cleanup on exceptions and the
workflow's separate always-run cleanup. Removal uses that ID after checking
its label; foreign replacements are preserved. A missing-object response,
rather than a general engine error, establishes absence. Sanitized evidence
and the dependency lock are retained for seven days.

Run inert runner controls with
`python3 -I -S -B tests/ci/test_command_key_cohort.py`. They exercise synthetic
inspection metadata and receipts without creating a runtime or loading PHP.

The proposed include-tree capture is data-only preparation for the required
read-only consumer boundary. `command_key_include_closure.py` inventories every
consumer/vendor/fixture leaf, rejects aliases, hardlinks, repository metadata
and unexpected root members, and copies verified bytes with read-only modes.
Its output includes a complete member/hash inventory. Before any installed
execution, that complete tree still needs a root-controlled staging location,
a kernel-enforced read-only consumer mount, before/after source and mount
identity checks, independent container-ID cleanup and actual boundary controls.
File modes and the inert inventory tests alone provide no isolation verdict.
The proposed official PHP image identities in the supply are raw-registry
metadata bindings; this prototype has not pulled or qualified that consumer.

Run the offline input controls against an existing immutable PHP image:

```sh
SDK_PHP_COMMAND_KEY_IMAGE=sha256:<inspected-image-id> \
  python3 -I -S -B tests/ci/test_command_key_client.py
```

These controls use no network or SDK autoload. They verify malformed operations,
foreign fixture identities, non-loopback origins and oversized packets stop
before package loading. They establish no installed command admission result.
