<?php

declare(strict_types=1);

use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Client;
use DurableWorkflow\Telemetry\ExecutionTelemetry;
use DurableWorkflow\Transport\Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;

final class FrameworkTelemetryHandler
{
    /** @var list<array<string, string|int>> */
    private static array $seen = [];
    public static ?\Psr\Log\LoggerInterface $logger = null;

    public static function reset(): void { self::$seen = []; }

    /** @return list<array<string, string|int>> */
    public static function observed(): array { return self::$seen; }

    #[Activity('telemetry.framework')]
    public function activity(ActivityContext $context): string
    {
        self::$seen[] = ExecutionTelemetry::current();
        self::$logger?->info('framework callback', ['request_id' => 'application-request']);

        return 'framework-result';
    }
}

/** @param Closure(Client, ?ExecutionTelemetry): Worker $create
 *  @param (Closure(\Monolog\Logger): void)|null $configureLogger
 */
function verifyFrameworkExecutionTelemetry(Closure $create, ?Closure $configureLogger = null): void
{
    if (getenv('DURABLE_WORKFLOW_TEST_TRACING') === '1'
        && (!class_exists(\OpenTelemetry\API\Globals::class) || !class_exists(\Monolog\Logger::class))) {
        throw new RuntimeException('The optional tracing qualification requires the API and Monolog.');
    }
    foreach ([null, new ExecutionTelemetry()] as $telemetry) {
        FrameworkTelemetryHandler::reset();
        $logs = null;
        if (class_exists(\Monolog\Logger::class)) {
            $logs = new \Monolog\Handler\TestHandler();
            $logger = new \Monolog\Logger('framework', [$logs]);
            if ($configureLogger !== null) {
                $configureLogger($logger);
            } else {
                $logger->pushProcessor(new \DurableWorkflow\Telemetry\LogContext());
            }
            FrameworkTelemetryHandler::$logger = $logger;
        }
        $transport = new class implements Transport {
            /** @var list<array<string, mixed>> */
            private array $outcomes = [];
            /** @return list<array<string, mixed>> */
            public function outcomes(): array { return $this->outcomes; }
            /** @param array<string, string> $headers
             *  @param array<string, mixed>|null $body
             *  @return array<string, mixed>
             */
            public function send(string $method, string $uri, array $headers, ?array $body = null): array
            {
                if (str_ends_with($uri, '/workflow-tasks/poll') || str_ends_with($uri, '/query-tasks/poll')) {
                    return ['task' => null, 'poll_status' => 'empty'];
                }
                if (str_ends_with($uri, '/activity-tasks/poll')) {
                    return ['task' => ['task_id' => 'framework-task', 'workflow_id' => 'framework-workflow',
                        'run_id' => 'framework-run', 'activity_execution_id' => 'framework-activity',
                        'activity_attempt_id' => 'framework-attempt', 'attempt_number' => 1,
                        'activity_type' => 'telemetry.framework', 'lease_owner' => 'framework-worker',
                        'payload_codec' => 'avro'], 'poll_status' => 'leased'];
                }
                if (str_ends_with($uri, '/framework-task/complete')) {
                    if ($body === null) {
                        throw new RuntimeException('Framework completion omitted its body.');
                    }
                    $this->outcomes[] = $body;

                    return ['recorded' => true];
                }
                throw new RuntimeException('Unexpected framework telemetry request '.$method.' '.$uri);
            }
        };
        $client = new Client('https://framework.invalid', namespace: 'framework-namespace', transport: $transport);
        $worker = $create($client, $telemetry);
        $worked = $worker->tick(0);
        $seen = FrameworkTelemetryHandler::observed();
        $outcomes = $transport->outcomes();
        if (!$worked || count($seen) !== 1 || count($outcomes) !== 1
            || ($outcomes[0]['activity_attempt_id'] ?? null) !== 'framework-attempt') {
            throw new RuntimeException('Framework telemetry changed the callback or durable outcome.');
        }
        $fields = $seen[0];
        $expected = $telemetry !== null && class_exists(\OpenTelemetry\API\Globals::class);
        if ($expected) {
            foreach (['workflow_run_id' => 'framework-run', 'activity_id' => 'framework-activity',
                'activity_attempt_id' => 'framework-attempt', 'namespace' => 'framework-namespace'] as $key => $value) {
                if (($fields[$key] ?? null) !== $value) {
                    throw new RuntimeException('Framework worker omitted its scoped '.$key.'.');
                }
            }
        } elseif ($fields !== []) {
            throw new RuntimeException('Framework worker enabled unrequested correlation.');
        }
        if ($logs !== null) {
            $records = $logs->getRecords();
            if (count($records) !== 1) {
                throw new RuntimeException('Framework callback did not reach the application logger.');
            }
            $record = $records[0];
            $context = ((array) $record)['context'];
            if (($context['request_id'] ?? null) !== 'application-request'
                || ($expected && ($context['activity_attempt_id'] ?? null) !== 'framework-attempt')
                || (!$expected && array_key_exists('workflow_run_id', $context))) {
                throw new RuntimeException('Framework logging lost its request ID or execution scope.');
            }
        }
        FrameworkTelemetryHandler::$logger = null;
        if (ExecutionTelemetry::current() !== []) {
            throw new RuntimeException('Framework worker leaked callback context.');
        }
    }
}
