<?php

declare(strict_types=1);

namespace DurableWorkflow;

/** Protocol versions advertised to the standalone server. */
final class Version
{
    public const CONTROL_PLANE_PROTOCOL = '2';
    public const WORKER_PROTOCOL = '1.15';
    public const MESSAGE_STREAMS_MINIMUM_WORKER_PROTOCOL = '1.15';

    public static function supportsMessageStreams(string $workerProtocol = self::WORKER_PROTOCOL): bool
    {
        return version_compare($workerProtocol, self::MESSAGE_STREAMS_MINIMUM_WORKER_PROTOCOL, '>=');
    }

    private function __construct()
    {
    }
}
