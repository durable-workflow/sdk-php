# Installed PHP command-key adapter

`command-key-client.php` reads one JSON packet from stdin and performs one
public PHP SDK operation. The selected immutable SDK and Server tuple lives
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
History output contains event counts only.

Use the accepted shared committed-response-loss fixture unchanged to prove
response loss. Use the accepted portable SQLite helper and its adjacent exact
Server manifest for synthetic legacy copies. The producer-owned physical seed
and upgrade profile stay separate and must never pass through the synthetic
transform. This PHP adapter currently supplies modern direct/loss API calls;
installed execution, legacy/physical adapters and complete shared216 acceptance
require their own reviewed runner and sanitized receipts.

Run the offline input controls against an existing immutable PHP image:

```sh
SDK_PHP_COMMAND_KEY_IMAGE=sha256:<inspected-image-id> \
  python3 -I -S -B tests/ci/test_command_key_client.py
```

These controls use no network or SDK autoload. They verify malformed operations,
foreign fixture identities, non-loopback origins and oversized packets stop
before package loading. They establish no installed command admission result.
