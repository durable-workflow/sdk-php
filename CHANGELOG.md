# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed

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

[Unreleased]: https://github.com/durable-workflow/sdk-php/compare/0.1.12...HEAD
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
