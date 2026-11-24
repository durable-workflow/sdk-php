<?php

declare(strict_types=1);

namespace DurableWorkflow;

/** Protocol versions advertised to the standalone server. */
final class Version
{
    public const CONTROL_PLANE_PROTOCOL = '2';
    public const WORKER_PROTOCOL = '1.20';
    public const MINIMUM_WORKER_PROTOCOL = '1.19';
    public const MESSAGE_STREAMS_MINIMUM_WORKER_PROTOCOL = '1.15';
    public const DURABLE_SELECTION_MINIMUM_WORKER_PROTOCOL = '1.19';
    public const PORTABLE_WORKER_AFFINITY_MINIMUM_PROTOCOL = '1.18';
    public const COOPERATIVE_CANCELLATION_MINIMUM_WORKER_PROTOCOL = '1.20';
    public const CONTINUE_AS_NEW_TARGET_BUILD_PROTOCOL = '1.21';

    public static function supportsCooperativeCancellation(string $workerProtocol = self::WORKER_PROTOCOL): bool
    {
        return self::supportsWorkerProtocol($workerProtocol, self::COOPERATIVE_CANCELLATION_MINIMUM_WORKER_PROTOCOL);
    }

    /** @internal A newer minor keeps the existing worker envelope and operations. */
    public static function supportsWorkerProtocol(mixed $actual, string $minimum): bool
    {
        [$major, $minor] = explode('.', $minimum);

        return is_string($actual) && preg_match('/\A(\d+)\.(\d+)\z/', $actual, $parts) === 1
            && (int) $parts[1] === (int) $major && (int) $parts[2] >= (int) $minor;
    }

    public static function supportsMessageStreams(string $workerProtocol = self::WORKER_PROTOCOL): bool
    {
        return version_compare($workerProtocol, self::MESSAGE_STREAMS_MINIMUM_WORKER_PROTOCOL, '>=');
    }

    public static function supportsDurableSelection(string $workerProtocol = self::WORKER_PROTOCOL): bool
    {
        return version_compare($workerProtocol, self::DURABLE_SELECTION_MINIMUM_WORKER_PROTOCOL, '>=');
    }

    private function __construct()
    {
    }
}
