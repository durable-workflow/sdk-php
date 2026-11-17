<?php

declare(strict_types=1);

namespace DurableWorkflow\Metrics;

/**
 * @internal Reviewed operation templates; unmatched paths never become raw metric labels.
 *
 * The shared list and its order match the Python SDK's route vocabulary
 * (durable-workflow/.github#233), so equal operations get equal labels across
 * SDKs. Routes only the PHP client sends follow, after the shared list.
 */
final class MetricsRoutes
{
    public const UNKNOWN = 'unknown';

    /** @var list<array{string, list<string>}>|null */
    private static ?array $templates = null;

    public static function template(string $path): string
    {
        $path = explode('#', explode('?', $path, 2)[0], 2)[0];
        $parts = explode('/', trim($path, '/'));
        foreach (self::$templates ??= self::compile() as [$route, $template]) {
            if (count($parts) !== count($template)) {
                continue;
            }
            foreach ($template as $index => $expected) {
                $actual = $parts[$index];
                if ($expected !== $actual && !(str_starts_with($expected, '{') && $actual !== '')) {
                    continue 2;
                }
            }

            return $route;
        }

        return self::UNKNOWN;
    }

    /** @return list<string> */
    public static function routes(): array
    {
        return array_column(self::$templates ??= self::compile(), 0);
    }

    /** @return list<array{string, list<string>}> */
    private static function compile(): array
    {
        $templates = [];
        foreach ([...self::sharedRoutes(), ...self::phpRoutes()] as $route) {
            $templates[] = [$route, explode('/', trim($route, '/'))];
        }

        return $templates;
    }

    /** @return list<string> Same entries and order as the Python SDK. */
    private static function sharedRoutes(): array
    {
        $routes = ['/', '/health', '/cluster/info', '/activities', '/activities/{activity_id}',
            '/namespaces', '/namespaces/{name}', '/namespaces/{name}/external-storage',
            '/search-attributes', '/search-attributes/{name}', '/storage/test',
            '/task-queues', '/task-queues/{task_queue}', '/task-queues/{task_queue}/build-ids'];
        foreach (['drain', 'promote', 'resume'] as $action) {
            $routes[] = "/task-queues/{task_queue}/build-ids/{$action}";
        }
        foreach (['activity-timeouts', 'repair', 'retention'] as $operation) {
            foreach (['', '/pass'] as $suffix) {
                $routes[] = "/system/{$operation}{$suffix}";
            }
        }
        array_push($routes, '/workers', '/workers/{worker_id}',
            '/worker/register', '/worker/heartbeat', '/worker/registrations/{worker_id}',
            '/worker/registrations/{worker_id}/deregister',
            '/worker/sessions', '/worker/sessions/{session_id}', '/worker/sessions/{session_id}/heartbeat');
        array_push($routes, ...self::sharedTaskRoutes());
        array_push($routes, '/workflows', '/workflows/{workflow_id}');
        foreach (['archive', 'cancel', 'repair', 'terminate', 'request-cancellation', 'runs'] as $action) {
            $routes[] = "/workflows/{workflow_id}/{$action}";
        }
        foreach (['signal', 'query', 'update'] as $action) {
            $routes[] = "/workflows/{workflow_id}/{$action}/{name}";
        }
        array_push($routes, '/workflows/{workflow_id}/message-streams/{name}/messages',
            '/workflows/{workflow_id}/runs/{run_id}');
        foreach (['history', 'history/export', 'redrive', 'request-cancellation', 'streams'] as $action) {
            $routes[] = "/workflows/{workflow_id}/runs/{run_id}/{$action}";
        }
        $routes[] = '/workflows/{workflow_id}/runs/{run_id}/streams/{name}';
        foreach (['items', 'close'] as $action) {
            $routes[] = "/workflows/{workflow_id}/runs/{run_id}/streams/{name}/{$action}";
        }
        $routes[] = '/schedules';
        $routes[] = '/schedules/{schedule_id}';
        foreach (['backfill', 'pause', 'resume', 'trigger', 'history'] as $action) {
            $routes[] = "/schedules/{schedule_id}/{$action}";
        }
        array_push($routes, '/bridge-adapters/webhook/{adapter}',
            '/service-endpoints/{endpoint_name}/services/{service_name}/operations/{operation_name}/execute');

        return $routes;
    }

    /** @return list<string> Shared worker task routes, in the Python SDK's order. */
    private static function sharedTaskRoutes(): array
    {
        $routes = [];
        foreach (['workflow', 'activity', 'query', 'update-validation'] as $kind) {
            $routes[] = "/worker/{$kind}-tasks/poll";
        }
        foreach ([
            'workflow' => ['complete', 'fail', 'heartbeat', 'history', 'deliver-cancellation'],
            'activity' => ['complete', 'fail', 'heartbeat', 'status', 'renew-lease', 'acknowledge-cancellation'],
            'query' => ['complete', 'fail', 'status'],
            'update-validation' => ['approve', 'reject'],
        ] as $kind => $actions) {
            foreach ($actions as $action) {
                $routes[] = "/worker/{$kind}-tasks/{task_id}/{$action}";
            }
        }
        foreach (['open', 'prepare', 'deliver'] as $action) {
            $routes[] = "/worker/workflow-tasks/{task_id}/cancellation-scopes/{$action}";
        }
        foreach (['checkpoint', 'checkpoint-group', 'prepare', 'recover'] as $action) {
            $routes[] = "/worker/workflow-tasks/{task_id}/local-activities/{$action}";
        }
        foreach (['control', 'heartbeat', 'outcome', 'acknowledge-cancellation'] as $action) {
            $routes[] = "/worker/workflow-tasks/{task_id}/local-activities/{activity_attempt_id}/{$action}";
        }

        return $routes;
    }

    /** @return list<string> Reviewed operations only the PHP client currently sends. */
    private static function phpRoutes(): array
    {
        $routes = ['/workflow-version-usage', '/activities/{activity_id}/result', '/system/health', '/system/operator-dashboard',
            '/system/operator-dashboard/bounded', '/system/operator-dashboard/bounded/workflow-types',
            '/system/operator-metrics', '/task-queues/{task_queue}/activity-dispatch',
            '/task-queues/{task_queue}/activity-dispatch/pause', '/task-queues/{task_queue}/activity-dispatch/resume'];
        foreach (['debug', 'observation', 'redrive'] as $action) {
            $routes[] = "/workflows/{workflow_id}/{$action}";
        }
        foreach (['archive', 'cancel', 'repair', 'terminate', 'debug', 'observation'] as $action) {
            $routes[] = "/workflows/{workflow_id}/runs/{run_id}/{$action}";
        }
        foreach (['signal', 'query', 'update'] as $action) {
            $routes[] = "/workflows/{workflow_id}/runs/{run_id}/{$action}/{name}";
        }
        $routes[] = '/service-endpoints/{endpoint_name}/services/{service_name}/operations/{operation_name}/service-calls/{service_call_id}';
        $routes[] = '/service-endpoints/{endpoint_name}/services/{service_name}/operations/{operation_name}/service-calls/{service_call_id}/cancel';

        return $routes;
    }
}
