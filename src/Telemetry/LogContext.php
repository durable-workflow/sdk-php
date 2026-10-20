<?php

declare(strict_types=1);

namespace DurableWorkflow\Telemetry;

use Monolog\LogRecord;
use Throwable;

/** Optional Monolog 2/3 processor. Attach once to the application's logger. */
final class LogContext
{
    /** @param array<string, mixed>|LogRecord $record
     *  @return array<string, mixed>|LogRecord
     */
    public function __invoke(array|LogRecord $record): array|LogRecord
    {
        try {
            $fields = ExecutionTelemetry::current();
            if ($fields === []) {
                return $record;
            }
            $context = $record instanceof LogRecord ? $record->context : ($record['context'] ?? []);
            if (!is_array($context)) {
                return $record;
            }
            if (array_key_exists('request_id', $context)) {
                unset($fields['request_id']);
            }
            $context = $fields + $context;
            if ($record instanceof LogRecord) {
                return $record->with(context: $context);
            }
            $record['context'] = $context;
        } catch (Throwable) {
            // Preserve the application's logging path if optional correlation fails.
        }

        return $record;
    }
}
