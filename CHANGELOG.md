# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Class-oriented worker authoring with attributes for workflow, activity,
  query, signal, and update contracts; registration validates definitions and
  supports PSR-11 dependency resolution, PSR-3 logging, lifecycle diagnostics,
  and graceful deregistration.
- Framework-independent workflow client fakes and worker handler harnesses with
  assertions for workflow interactions and registered handler behavior.
- A source-free published-package smoke workflow for a real Server or
  Cloud-compatible endpoint.
- Optional first-party bridges for Laravel 12-13 and Symfony 6.4-8, including
  native container registration, console workers, configuration validation,
  framework logging and events, and test-client replacement helpers.

### Changed

- Advanced the PHP SDK to `2.0.0-rc.11` and qualified it with Server
  `2.0.0-rc.17`. SDK and Server prerelease counters remain independent; earlier
  prereleases retain historical release records but receive no compatibility
  shim.
- Replaced the prerelease JSON wrapper with the fixed recursive
  `durable_workflow.protocol.Value` schema and Avro single-object framing.
  `AvroBinaryValue` makes the bytes branch explicit while PHP strings remain
  UTF-8 text. `AvroMapValue` preserves empty maps and numeric-looking string
  keys that native PHP arrays cannot represent, and the reader resolves
  recursive named branches without warning suppression.
- Added golden cross-language fixtures and a repeatable production-path size
  and latency benchmark over the shared checked-in corpus, with an enforced
  regression budget calibrated with explicit headroom.

- Release-plan recovery now consumes immutable, exact-version release-note
  preparation authority before publishing a newly recorded plan.

### Fixed

- Laravel now resolves shared or role-scoped credentials when each client is
  constructed, keeping secrets out of cached configuration while preserving
  the control/worker privilege boundary.
- Laravel and Symfony now bind separate control-only and worker-only clients for
  scoped Cloud credentials. Missing role credentials fail before transport,
  while the shared self-hosted token remains valid for both roles.
- Graceful worker shutdown now uses the worker-plane registration lifecycle
  route; operator-initiated worker removal remains on the control plane.

- Attribute-discovered workflow entry points, queries, and updates now start
  from clean handler object state for every replay or invocation, without
  losing constructor-injected collaborators. Activity services and explicit
  low-level callables retain their application-owned lifetimes.

- The generated API reference omits its responsive header-menu control when
  the top navigation has no destinations, leaving search and sidebar
  navigation as the useful narrow-viewport interactions.

- The generated API reference constrains and, when needed, wraps its SDK title
  at narrow viewport widths so the shared header and consent controls remain
  usable without document-level horizontal overflow.

- Analytics consent withdrawal in the PHP API reference immediately disables
  the active GA4 property and updates every consent signal to denied before
  reloading. It also clears GA4 identifiers at the current host-only scope and
  legacy parent-domain scopes while retaining unrelated cookies; denied-state
  reloads remain fail-closed.

- Client construction rejects base URIs ending in the SDK-owned `/api` path
  segment before they can produce duplicated request paths.

- Explicit release recovery rejects terminally superseded plans before and
  after publication preflight while keeping completed-plan verification
  idempotent.

- Managed workers retry the server's fenced registration lock-pressure
  response with bounded backoff before entering the poll loop, while other
  registration failures remain terminal.

## [0.1.12] - 2026-07-19

### Fixed

- Managed workers recognize cancelled workflow tasks reported as no longer
  leased as terminal task races, while requiring the refusal to identify the
  task being acknowledged.

## [0.1.11] - 2026-07-18

### Changed

- Managed workers retry explicitly transient workflow, activity, and query
  poll refusals with observable capped backoff while preserving heartbeats and
  responsive shutdown; workflow-task execution also waits for a successful
  typed lease renewal after transient backend pressure. Unrelated server
  failures and invalid fencing outcomes remain fatal.

## [0.1.10] - 2026-07-17

### Changed

- Managed workers discard workflow, activity, and query tasks whose typed
  acknowledgements report that the leased task became terminal concurrently,
  then continue polling for unrelated work.
- Release recovery creates or verifies the exact planned source tag before
  publishing its GitHub Release, and public workflows qualify every supported
  target branch.

## [0.1.9] - 2026-07-16

### Added

- Automated recovery of the exact PHP SDK release selected by an immutable
  cross-repository release plan.

## [0.1.8] - 2026-07-16

### Added

- Replay-consumed workflow signal declarations and their parameter contracts
  in managed worker registration metadata.

## [0.1.7] - 2026-07-16

### Fixed

- Managed workers resolve their advertised SDK version from the installed
  Composer package instead of a source constant.

## [0.1.6] - 2026-07-15

### Added

- Reflected parameter contracts for declared workflow query and update
  handlers in managed worker registrations.

## [0.1.5] - 2026-07-14

### Added

- Per-workflow query and update command names in managed worker registrations
  so the server can address declared handlers on new runs.

## [0.1.4] - 2026-07-14

### Changed

- Managed workers adopt the server-advertised heartbeat cadence and refresh
  their registration between heartbeat-bounded workflow, activity, and query
  long polls.

## [0.1.3] - 2026-07-14

### Added

- Full workflow, activity, and query poll-response methods that preserve typed
  refusal and protocol metadata.

### Changed

- Managed workers stop polling after stale-registration, drain, and stop
  outcomes while ordinary empty polls remain idle.

## [0.1.2] - 2026-07-14

### Added

- Server-side schedule visibility filters and opaque continuation-token paging.

## [0.1.1] - 2026-07-14

### Added

- Immutable namespace selection and typed global workflow visibility pages.
- Search-attribute administration and namespace external-storage policy updates.
- Avro-backed service-operation start, execute, describe, and cancel APIs.
- Typed cluster discovery plus schedule page, continuation, and history route coverage.

## [0.1.0] - 2026-07-13

### Added

- Framework-neutral control-plane client with current-run and selected-run handles.
- Schedule and namespace management.
- PSR-18 transport and control/worker bearer-token authentication.
- Apache Avro generic-wrapper and typed-schema payload support.
- Remote PHP worker execution, heartbeats, history replay, query/update tasks,
  cancellation, and graceful shutdown.
- Generated API reference and supported PHP-version CI.

[Unreleased]: https://github.com/durable-workflow/sdk-php/compare/0.1.16...HEAD
[0.1.12]: https://github.com/durable-workflow/sdk-php/compare/0.1.11...0.1.12
[0.1.11]: https://github.com/durable-workflow/sdk-php/compare/0.1.10...0.1.11
[0.1.10]: https://github.com/durable-workflow/sdk-php/compare/0.1.9...0.1.10
[0.1.9]: https://github.com/durable-workflow/sdk-php/compare/0.1.8...0.1.9
[0.1.8]: https://github.com/durable-workflow/sdk-php/compare/0.1.7...0.1.8
[0.1.7]: https://github.com/durable-workflow/sdk-php/compare/0.1.6...0.1.7
[0.1.6]: https://github.com/durable-workflow/sdk-php/compare/0.1.5...0.1.6
[0.1.5]: https://github.com/durable-workflow/sdk-php/compare/0.1.4...0.1.5
[0.1.4]: https://github.com/durable-workflow/sdk-php/compare/0.1.3...0.1.4
[0.1.3]: https://github.com/durable-workflow/sdk-php/compare/0.1.2...0.1.3
[0.1.2]: https://github.com/durable-workflow/sdk-php/compare/0.1.1...0.1.2
[0.1.1]: https://github.com/durable-workflow/sdk-php/compare/0.1.0...0.1.1
[0.1.0]: https://github.com/durable-workflow/sdk-php/releases/tag/0.1.0
