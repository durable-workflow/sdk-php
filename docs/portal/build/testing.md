---
layout: layout.njk
title: Testing
description: Test Durable Workflow PHP handlers, deterministic replay, client contracts, and end-to-end worker behavior.
lead: Keep fast logic tests close to handlers, then add replay fixtures and one real runtime smoke so protocol, auth, polling, and result handling are exercised together.
previous:
  label: Failures & retries
  url: /build/failures-retries/
next:
  label: Laravel
  url: /frameworks/laravel/
---
## Test activities as ordinary PHP

Move business logic behind dependencies and call the activity handler with fakes. Assert the downstream idempotency key and domain result, not SDK internals.

```php
$activity = new ReserveInventory($fakeInventory);
$result = $activity($activityContext, 'order-1001');

self::assertTrue($result['reserved']);
self::assertSame('order-1001', $fakeInventory->lastIdempotencyKey);
```

## Replay committed history

`Replayer` accepts a workflow callable, history events, input arguments, and a default task queue. A useful regression fixture proves both paths:

1. Empty history emits the expected durable command.
2. Recorded completion history resumes the Fiber at the context call and emits the final result without calling the activity.

Keep sanitized production histories when they reveal command-order bugs. Bind each fixture to the Apache Avro payload schema and event schema it actually exercises.

## Test clients with a fake transport

Inject a `Transport` implementation into `Client` to verify routes, authentication role, namespace, encoded request shape, and typed response handling without opening a socket.

Avoid snapshots of entire responses when the test only owns one field. Server envelopes grow additively; overbroad fixtures turn compatible additions into noise.

## Run a real boundary smoke

Your CI should also start the published qualified Server image, install the published SDK into an empty Composer project, and run the same worker/client pair from [your first workflow](/getting-started/first-workflow/).

Require these observations:

- readiness succeeds before the worker starts;
- the worker registers on the expected task queue;
- the client starts a unique workflow ID;
- the selected run reaches `completed`;
- the decoded result matches the activity output;
- `SIGTERM` stops the worker within the supervisor grace period.

This repository's protected published-service qualification performs that clean-artifact smoke after a release is available. A second published-artifact workflow creates clean Laravel and Symfony applications and exercises their first-party bridges. The portal renders the same standalone PHP files that ship under `examples/`, so source linting, package proof, and the copy button use the same bytes.

## Separate runtime and presentation checks

Documentation qualification should validate builds, internal links, canonical and social metadata, source examples, and rendered layout. It should not freeze headings or marketing sentences. Product behavior belongs in executable PHP and protocol tests; prose remains editable.

<div class="note"><strong>Testing pyramid</strong><p>Most tests should be ordinary PHP or replay fixtures. Keep a smaller end-to-end set for the network and worker lifecycle boundaries that fakes cannot prove.</p></div>
