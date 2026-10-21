<?php

declare(strict_types=1);

namespace DurableWorkflow\Telemetry;

use OpenTelemetry\API\Baggage\Baggage;
use OpenTelemetry\API\Baggage\Propagation\BaggagePropagator;
use OpenTelemetry\API\Globals;
use OpenTelemetry\API\Trace\Propagation\TraceContextPropagator;
use OpenTelemetry\API\Trace\Span;
use OpenTelemetry\Context\Context;
use Throwable;

/** Optional observations. A validated carrier never grants a receipt or lease. */
final class TraceCarrier
{
    private const SCHEMA = 'durable-workflow.trace-context';
    private const LIMITS = ['traceparent' => 256, 'tracestate' => 512, 'baggage' => 2048];
    private const CAPABILITY = [
        'supported' => true, 'schema' => self::SCHEMA, 'version' => 1,
        'dispatch_schema' => 'durable-workflow.queue-trace', 'links_schema' => 'durable-workflow.trace-links',
        'max_context_bytes' => 4096, 'max_links' => 32, 'max_links_bytes' => 4096,
    ];

    /** @param array<string, mixed> $clusterInfo */
    public static function supported(array $clusterInfo): bool
    {
        $protocol = $clusterInfo['worker_protocol'] ?? null;
        $capabilities = is_array($protocol) ? ($protocol['server_capabilities'] ?? null) : null;
        $value = is_array($capabilities) ? ($capabilities['observability_context'] ?? null) : null;
        if (!is_array($value)) {
            return false;
        }
        foreach (self::CAPABILITY as $key => $expected) {
            if (($value[$key] ?? null) !== $expected) {
                return false;
            }
        }

        return true;
    }

    public static function identity(mixed $value): bool
    {
        return is_string($value) && $value !== '' && strlen($value) <= 512
            && preg_match('//u', $value) === 1 && preg_match('/\p{Cc}/u', $value) === 0;
    }

    /** @param list<string> $allowlist
     *  @return array<string, mixed>|null
     */
    public static function capture(array $allowlist = [], ?string &$diagnostic = null): ?array
    {
        $diagnostic = null;
        if (!class_exists(Globals::class)) {
            return null;
        }
        try {
            $headers = [];
            Globals::propagator()->inject($headers);
            $headers = array_intersect_key($headers, self::LIMITS);
            if ($headers === []) {
                return null;
            }

            return self::normalize(['schema' => self::SCHEMA, 'version' => 1, 'carrier' => $headers], $allowlist, $diagnostic);
        } catch (Throwable) {
            $diagnostic = 'discarded';

            return null;
        }
    }

    /** @param list<string> $allowlist
     *  @return array<string, mixed>|null
     */
    public static function normalize(mixed $value, array $allowlist = [], ?string &$diagnostic = null): ?array
    {
        $diagnostic = $value === null ? null : 'discarded';
        if (!is_array($value) || count($value) !== 3 || ($value['schema'] ?? null) !== self::SCHEMA || ($value['version'] ?? null) !== 1
            || array_diff(array_keys($value), ['schema', 'version', 'carrier']) !== []
            || !is_array($value['carrier'] ?? null) || !class_exists(TraceContextPropagator::class)) {
            return null;
        }
        $headers = $value['carrier'];
        if (count($headers) > 3 || !isset($headers['traceparent']) || array_diff(array_keys($headers), array_keys(self::LIMITS)) !== []) {
            return null;
        }
        if (!self::validHeaderValues($headers)) {
            return null;
        }
        try {
            if (strlen(json_encode($value, JSON_THROW_ON_ERROR)) > 4096) {
                return null;
            }
            $normalized = self::normalizeHeaders($headers, $allowlist);
            if ($normalized === null) {
                return null;
            }
            $result = ['schema' => self::SCHEMA, 'version' => 1, 'carrier' => $normalized];
            if (strlen($normalized['baggage'] ?? '') > 2048 || strlen(json_encode($result, JSON_THROW_ON_ERROR)) > 4096) {
                return null;
            }
            $diagnostic = $normalized === $headers ? null : 'filtered';

            return $result;
        } catch (Throwable) {
            return null;
        }
    }

    /** @param array<string, mixed> $headers
     *  @phpstan-assert-if-true array<string, string> $headers
     */
    private static function validHeaderValues(array $headers): bool
    {
        foreach ($headers as $key => $header) {
            if (!is_string($header) || strlen($header) > self::LIMITS[$key]) {
                return false;
            }
        }
        return true;
    }

    /** @param array<string, string> $headers
     *  @param list<string> $allowlist
     *  @return array<string, string>|null
     */
    private static function normalizeHeaders(array $headers, array $allowlist): ?array
    {
        $context = TraceContextPropagator::getInstance()->extract($headers, context: Context::getRoot());
        if (!Span::fromContext($context)->getContext()->isValid()) {
            return null;
        }
        $normalized = [];
        TraceContextPropagator::getInstance()->inject($normalized, context: $context);
        if ($allowlist !== [] && isset($headers['baggage'])) {
            self::allowlistedBaggage($headers, $allowlist, $normalized);
        }
        return $normalized;
    }

    /** @param array<string, string> $headers
     *  @param list<string> $allowlist
     *  @param array<string, string> $normalized
     */
    private static function allowlistedBaggage(array $headers, array $allowlist, array &$normalized): void
    {
        $baggage = BaggagePropagator::getInstance()->extract($headers, context: Context::getRoot());
        $builder = Baggage::getBuilder();
        $count = 0;
        foreach (Baggage::fromContext($baggage)->getAll() as $key => $entry) {
            if (!in_array($key, $allowlist, true)) {
                continue;
            }
            if (++$count > 32) {
                throw new \LengthException('Baggage entry limit exceeded.');
            }
            $builder->set($key, $entry->getValue(), $entry->getMetadata());
        }
        BaggagePropagator::getInstance()->inject($normalized, context: $builder->build()->storeInContext(Context::getRoot()));
    }

    /** @return array<string, mixed>|null */
    public static function dispatch(mixed $value, string $taskId, ?string &$diagnostic = null): ?array
    {
        $diagnostic = $value === null ? null : 'discarded';
        if (!self::identity($taskId) || !is_array($value) || count($value) !== 4 || ($value['schema'] ?? null) !== 'durable-workflow.queue-trace'
            || ($value['version'] ?? null) !== 1 || ($value['task_id'] ?? null) !== $taskId
            || array_diff(array_keys($value), ['schema', 'version', 'task_id', 'context']) !== []
            || !is_array($value['context'] ?? null) || !is_array($value['context']['carrier'] ?? null) || count($value['context']['carrier']) > 2
            || array_diff(array_keys($value['context']['carrier']), ['traceparent', 'tracestate']) !== []) {
            return null;
        }
        $context = self::normalize($value['context'], diagnostic: $contextDiagnostic);
        if ($context === null) {
            return null;
        }
        try {
            if (strlen(json_encode($value, JSON_THROW_ON_ERROR)) > 4096) {
                return null;
            }
            $diagnostic = $contextDiagnostic;

            return $context;
        } catch (Throwable) {
            return null;
        }
    }
}
