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
        self::$pairs ??= self::load();

        return self::$pairs[$scope] ?? throw new \InvalidArgumentException('Unknown SDK metrics scope.');
    }

    /** @return array<string, array{counter: string, histogram: string}> */
    private static function load(): array
    {
        $document = file_get_contents(dirname(__DIR__, 2).'/resources/protocol/sdk-metrics-v1.json');
        if ($document === false) {
            throw new InvalidMetricsRegistry('The SDK metrics registry is unavailable.');
        }
        $contract = json_decode($document, true, flags: JSON_THROW_ON_ERROR);
        if (!is_array($contract) || !is_array($contract['metrics'] ?? null)) {
            throw new InvalidMetricsRegistry('The SDK metrics registry has an invalid shape.');
        }
        /** @var array<string, array{counter?: string, histogram?: string}> $pairs */
        $pairs = [];
        foreach ($contract['metrics'] as $metric) {
            [$scope, $kind, $name] = self::metric($metric);
            $pairs[$scope][$kind] = $name;
        }
        $complete = [];
        foreach ($pairs as $scope => $pair) {
            if (!isset($pair['counter'], $pair['histogram'])) {
                throw new InvalidMetricsRegistry('The SDK metrics registry has an incomplete scope.');
            }
            $complete[$scope] = ['counter' => $pair['counter'], 'histogram' => $pair['histogram']];
        }

        return $complete;
    }

    /** @return array{0: string, 1: 'counter'|'histogram', 2: string} */
    private static function metric(mixed $metric): array
    {
        if (!is_array($metric) || !is_string($metric['name'] ?? null) || !is_string($metric['scope'] ?? null)) {
            throw new InvalidMetricsRegistry('The SDK metrics registry has an invalid metric.');
        }
        $kind = match ($metric['type'] ?? null) {
            'counter' => 'counter',
            'histogram' => 'histogram',
            default => throw new InvalidMetricsRegistry('Unknown SDK metric instrument.'),
        };

        return [$metric['scope'], $kind, $metric['name']];
    }
}
