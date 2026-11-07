---
layout: layout.njk
title: Upgrading
description: Upgrade PHP SDK workers with compatible condition histories, exact static credentials and a clear rollback boundary.
lead: Upgrade the complete task queue together, preserve durable replay identities, and restart workers with the matching source files.
previous:
  label: Deployment
  url: /operate/deployment/
next:
  label: Authentication
  url: /operate/authentication/
---
This guide covers PHP SDK {{ release.sdkVersion }} and its changes listed in the
[changelog](https://github.com/durable-workflow/sdk-php/blob/main/CHANGELOG.md).
The bare SDK retains PHP `^8.1`, and Symfony 6.4/7/8 support is unchanged.
The Laravel bridge follows Workflow 2.21.0: PHP `^8.2` and patched Laravel
`^12.69.0|^13.30.0`, with PHP 8.3 or newer for Laravel 13. SDK 3.10 removes
Laravel 10/11 bridge support. Its provider checks the installed framework and
PHP before registering application bindings, including during auto-discovery.
Upgrade the platform before adopting this bridge; existing SDK 3.9.x and
Workflow 2.20.x applications retain their own support contracts while upgrading.
Stable-v1 authority remains unchanged.

## Condition fingerprints and replay

The worker records a condition predicate's identity in durable history. The
new reader accepts both versions, with different comparisons:

| First recorded fingerprint | Replay and physical reopen |
| --- | --- |
| `sha256:` (v1) | The original source-line hash must match exactly. A reopen retains that same v1 value. |
| `dwphp2:sha256:` (v2) | The predicate's canonical tokens must match. A reopen retains that same v2 value. |
| No fingerprint | The existing absence is retained on reopen; the upgrade does not invent a fingerprint for that wait. |

For a new wait, tokenizable supported source records v2. Whitespace and
comments outside literals, source locations and neighboring call arguments
do not change its token identity. Literals and operator boundaries remain
significant. The key, timeout and authored call order have their own replay
checks; token tolerance does not permit changing those values.

If the callable cannot be isolated or PHP's tokenizer is unavailable, the
worker falls back to the existing v1 computation. If its source cannot be
read, that computation can remain absent. Every worker that replays v2
histories needs the tokenizer enabled and the corresponding PHP source files.
These are the implemented recording and read rules; there is no configuration
switch to disable strict legacy comparison or opt an existing wait into v2.

Existing v1 waits remain sensitive to reindentation, comments and neighboring
code on their original source lines. V2 formatting tolerance applies to waits
first opened with v2. Upgrading a worker does not convert older histories.
Indistinguishable same-line closures can share a combined candidate identity;
put predicates on separate lines before starting runs that need independent
identities. Use [version markers](/build/workflows-activities/#evolve-workflow-code-with-version-markers)
for a deliberate workflow behavior change.

## Upgrade the whole queue and preserve the rollback boundary

1. Inventory every worker that can receive work from the task queue, including
   standby and autoscaling pools. Prepare a consistent SDK, workflow source and
   tokenizer configuration for all of them.
2. Stop or drain the older workers before replacement workers begin recording
   v2 waits. A mixed queue is unsafe once any worker can write a fingerprint
   that another worker cannot replay.
3. Deploy the PHP sources atomically and restart all long-running workers,
   including workers using OPcache. Do not replace source files underneath
   processes that still execute their previous definitions.
4. Confirm that the replacement pool can replay representative existing v1
   histories and new v2 waits before restoring the full worker pool.

The fingerprint reads deployed files. Loaded code and files replaced in place
can disagree; `opcache.validate_timestamps=0` widens that window. Keep source
files with the worker deployment and restart processes when changing them.

After v2 histories exist, rolling back to an older SDK that cannot read v2
can fail replay, even when workflow behavior has not changed. Keep a rollback
build with the new reader and compatible workflow sources. Retain that reader
for as long as those histories may be replayed; switching a worker back to v1
recording would not remove already recorded v2 values.

## Validate all static credential settings

Review shared, client, worker and framework process-handoff values before
starting either role. The SDK validates every original nonempty static
credential before selection, including unused roles. Malformed UTF-8, Unicode
whitespace at either edge and embedded control characters fail instead of
being trimmed or ignored. Valid bytes and literal-empty optional fallback are
preserved. See [static credential validation](/operate/authentication/#static-credential-validation)
for exact boundaries and framework configuration guidance.

Keep runtime capability negotiation in place during the upgrade. Installing an
SDK does not add a producer capability to Server or Cloud, and the supported
Server release baseline remains separate from individual feature support.
