<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use RuntimeException;

/** @internal Failure metadata returned by an isolated activity callback. */
final class ActivityExecutionFailure extends RuntimeException
{
    /** @param array<string, mixed>|null $originalDiagnostics */
    public function __construct(
        string $message,
        public readonly string $originalType,
        public readonly bool $duringEncoding = false,
        public readonly bool $cancelled = false,
        public readonly bool $invalidReport = false,
        public readonly ?string $timeoutKind = null,
        public readonly bool $storageAdmissionFailure = false,
        public readonly ?string $originalStackTrace = null,
        public readonly ?array $originalDiagnostics = null,
    ) {
        parent::__construct($message);
    }
}
