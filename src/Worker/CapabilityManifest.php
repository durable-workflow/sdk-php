<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Version;

/** Truthful feature-by-feature manifest sent with every managed worker registration. */
final class CapabilityManifest
{
    /** @return array<string, array<string, bool|string>> */
    public static function portableWorkerAffinity(): array
    {
        return self::forCapabilities(['local_activities', 'worker_sessions', 'sticky_execution']);
    }

    /**
     * @param list<string> $capabilities
     * @return array<string, array<string, bool|string>>
     */
    public static function forCapabilities(array $capabilities): array
    {
        return [
            'local_activities' => self::entry(
                in_array('local_activities', $capabilities, true),
                'record_local_activity',
            ),
            'worker_sessions' => self::entry(
                in_array('worker_sessions', $capabilities, true),
                'create_heartbeat_close',
            ),
            'sticky_execution' => self::entry(
                in_array('sticky_execution', $capabilities, true),
                'bounded_exact_identity_cache',
            ),
        ];
    }

    /** @return array<string, bool|string> */
    private static function entry(bool $supported, string $implementation): array
    {
        return [
            'supported' => $supported,
            'minimum_protocol_version' => Version::PORTABLE_WORKER_AFFINITY_MINIMUM_PROTOCOL,
            $supported ? 'implementation' : 'reason' => $supported
                ? $implementation
                : 'not_enabled_for_this_worker_registration',
        ];
    }

    private function __construct()
    {
    }
}
