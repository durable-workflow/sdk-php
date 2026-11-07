<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Laravel;

use RuntimeException;

/** @internal Admission for the optional bridge, independent of the bare SDK's PHP floor. */
final class LaravelPlatform
{
    private const PLATFORMS = [
        12 => ['floor' => '12.69.0', 'php' => 80200],
        13 => ['floor' => '13.30.0', 'php' => 80300],
    ];

    public static function laravelConstraint(): string
    {
        $ranges = [];
        foreach (self::PLATFORMS as $platform) {
            $ranges[] = '^'.$platform['floor'];
        }

        return implode('|', $ranges);
    }

    public static function assertSupported(?string $laravelVersion, int $phpVersionId): void
    {
        $matches = [];
        $major = $laravelVersion !== null && strlen($laravelVersion) <= 32
            && preg_match('/\A(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:\.0)?\z/', $laravelVersion, $matches) === 1
                ? (int) $matches[1] : null;
        $platform = $major === null ? null : (self::PLATFORMS[$major] ?? null);

        if ($platform !== null
            && $phpVersionId >= $platform['php'] && $phpVersionId < 90000
            && version_compare($laravelVersion, $platform['floor'], '>=')) {
            return;
        }

        throw new RuntimeException(
            'The Durable Workflow Laravel bridge requires PHP ^8.2 and Laravel '.self::laravelConstraint()
            .'; Laravel 13 requires PHP ^8.3. Upgrade the application platform before activating the bridge'
            .' or retain the existing SDK 3.9.x release while upgrading.',
        );
    }
}
