<?php

declare(strict_types=1);

// Deliberately omit Composer so the optional API and Monolog are unavailable.
spl_autoload_register(static function (string $class): void {
    $prefix = 'DurableWorkflow\\';
    if (str_starts_with($class, $prefix)) {
        $path = dirname(__DIR__, 2).'/src/'.str_replace('\\', '/', substr($class, strlen($prefix))).'.php';
        if (is_file($path)) {
            require $path;
        }
    }
});
$calls = 0;
$result = (new \DurableWorkflow\Telemetry\ExecutionTelemetry())->task('activity', [], 'default', 'orders',
    static function () use (&$calls): string { ++$calls; return 'business-result'; });
$record = ['message' => 'application message', 'context' => ['request_id' => 'application-request']];
if ($result !== 'business-result' || $calls !== 1
    || \DurableWorkflow\Telemetry\ExecutionTelemetry::current() !== []
    || (new \DurableWorkflow\Telemetry\LogContext())($record) !== $record) {
    throw new \RuntimeException('Optional API absence changed execution or logging.');
}
echo "optional-api-absent-pass\n";
