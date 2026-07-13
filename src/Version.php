<?php

declare(strict_types=1);

namespace DurableWorkflow;

/** Protocol and package identity advertised to the standalone server. */
final class Version
{
    public const SDK = '0.1.0';
    public const CONTROL_PLANE_PROTOCOL = '2';
    public const WORKER_PROTOCOL = '1.13';

    private function __construct()
    {
    }
}
