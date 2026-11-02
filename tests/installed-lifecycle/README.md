# Installed-lifecycle qualification

This checks a **normally installed** PHP SDK release, from Packagist with no
path repository or source overlay, against an exact Server image pulled by
digest. Everything runs in disposable local containers; nothing touches Cloud.

```sh
SDK_VERSION=3.6.1 SERVER_IMAGE=ghcr.io/durable-workflow/server-native@sha256:<index> \
  tests/installed-lifecycle/run.sh <output-dir>
```

Scenarios (one JSON line each in `results.jsonl`, outcome `pass`, `product-fail` or `runner-blocked`):

- `lifecycle`: start, activity, query, update, signal, completion with an exact result
- `cancellation`: a cancelled waiting workflow surfaces `WorkflowCancelled`
- `typed_failure`: a non-retried activity exception surfaces `WorkflowFailed` carrying its message

The output directory also records the runner commit, the Server index and image
inspection, the installed SDK's `composer show` and dist sha256, start and end
UTC, and worker and server logs. The worker and client run on `php:8.4-cli`;
installation uses the digest-pinned Composer 2.10.3 image.
