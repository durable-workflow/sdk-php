# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Server-side schedule visibility filters and opaque continuation-token paging.
- Full workflow, activity, and query poll-response methods that preserve typed
  refusal and protocol metadata.

### Changed

- Managed workers stop polling after stale-registration, drain, and stop
  outcomes while ordinary empty polls remain idle.

## [0.1.1] - Unreleased

### Added

- Immutable namespace selection and typed global workflow visibility pages.
- Search-attribute administration and namespace external-storage policy updates.
- Avro-backed service-operation start, execute, describe, and cancel APIs.
- Typed cluster discovery plus schedule page, continuation, and history route coverage.

## [0.1.0] - Unreleased

### Added

- Framework-neutral control-plane client with current-run and selected-run handles.
- Schedule and namespace management.
- PSR-18 transport and control/worker bearer-token authentication.
- Apache Avro generic-wrapper and typed-schema payload support.
- Remote PHP worker execution, heartbeats, history replay, query/update tasks,
  cancellation, and graceful shutdown.
- Generated API reference and supported PHP-version CI.

[Unreleased]: https://github.com/durable-workflow/sdk-php/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/durable-workflow/sdk-php/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/durable-workflow/sdk-php/releases/tag/v0.1.0
