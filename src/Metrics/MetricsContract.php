<?php

declare(strict_types=1);

namespace DurableWorkflow\Metrics;

/** @internal Loads the exact pinned shared registry, not a PHP-specific metric list. */
final class MetricsContract
{
    /** @var array<string, array{counter: string, histogram: string}>|null */
    private static ?array $pairs = null;

    /** @return array{counter: string, histogram: string} */
    public static function pair(string $scope): array
    {
        if (self::$pairs === null) {
            $document = file_get_contents(dirname(__DIR__, 2).'/resources/protocol/sdk-metrics-v1.json');
            if ($document === false) {
                throw new \RuntimeException('The SDK metrics registry is unavailable.');
            }
            $contract = json_decode($document, true, flags: JSON_THROW_ON_ERROR);
            if (!is_array($contract) || !is_array($contract['metrics'] ?? null)) {
                throw new \RuntimeException('The SDK metrics registry has an invalid shape.');
            }
            /** @var array<string, array{counter?: string, histogram?: string}> $pairs */
            $pairs = [];
            foreach ($contract['metrics'] as $metric) {
                if (!is_array($metric) || !is_string($metric['name'] ?? null)
                    || !is_string($metric['scope'] ?? null)) {
                    throw new \RuntimeException('The SDK metrics registry has an invalid metric.');
                }
                $kind = match ($metric['type'] ?? null) {
                    'counter' => 'counter',
                    'histogram' => 'histogram',
                    default => throw new \RuntimeException('Unknown SDK metric instrument.'),
                };
                $pairs[$metric['scope']][$kind] = $metric['name'];
            }
            $complete = [];
            foreach ($pairs as $metricScope => $pair) {
                if (!isset($pair['counter'], $pair['histogram'])) {
                    throw new \RuntimeException('The SDK metrics registry has an incomplete scope.');
                }
                $complete[$metricScope] = ['counter' => $pair['counter'], 'histogram' => $pair['histogram']];
            }
            self::$pairs = $complete;
        }

        return self::$pairs[$scope] ?? throw new \InvalidArgumentException('Unknown SDK metrics scope.');
    }
}
