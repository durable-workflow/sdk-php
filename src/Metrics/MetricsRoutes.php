<?php

declare(strict_types=1);

namespace DurableWorkflow\Metrics;

/** @internal Reviewed templates; unmatched paths never become raw metric labels. */
final class MetricsRoutes
{
    private const TEMPLATES = [
        '/health', '/cluster/info', '/namespaces', '/workflows', '/schedules', '/workers',
        '/worker/register', '/worker/heartbeat', '/worker/workflow-tasks/poll',
        '/worker/activity-tasks/poll', '/worker/query-tasks/poll',
        '/workflows/{workflow_id}/runs/{run_id}/history',
        '/workflows/{workflow_id}/runs/{run_id}', '/workflows/{workflow_id}/history',
        '/workflows/{workflow_id}', '/workers/{worker_id}', '/schedules/{schedule_id}',
        '/worker/registrations/{worker_id}/deregister',
    ];

    /** @var array<string, string>|null */
    private static ?array $patterns = null;

    public static function template(string $path): string
    {
        $path = explode('?', $path, 2)[0];
        if (self::$patterns === null) {
            self::$patterns = [];
            foreach (self::TEMPLATES as $template) {
                $parts = preg_split('/(\{[a-z_]+\})/', $template, flags: PREG_SPLIT_DELIM_CAPTURE);
                $pattern = '';
                foreach ($parts === false ? [] : $parts as $part) {
                    $pattern .= str_starts_with($part, '{') ? '[^/]+' : preg_quote($part, '~');
                }
                self::$patterns[$template] = '~^'.$pattern.'$~D';
            }
        }
        foreach (self::$patterns as $template => $pattern) {
            if (preg_match($pattern, $path) === 1) {
                return $template;
            }
        }

        return 'unknown';
    }
}
