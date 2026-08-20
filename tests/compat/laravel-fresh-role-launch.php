<?php

declare(strict_types=1);

use Illuminate\Support\Env;

final class LaravelFreshRoleProbe
{
    public static function record(string $stage): void
    {
        $log = getenv('DURABLE_WORKFLOW_ROLE_PROBE_LOG');
        if (!is_string($log) || $log === '') {
            throw new RuntimeException('Fresh Laravel role probe log is not configured.');
        }
        $presence = [];
        foreach ([
            'DURABLE_WORKFLOW_TOKEN',
            'DURABLE_WORKFLOW_CLIENT_TOKEN',
            'DURABLE_WORKFLOW_WORKER_TOKEN',
            'DURABLE_WORKFLOW_PROCESS_ROLE',
            'DURABLE_WORKFLOW_PROCESS_TOKEN',
        ] as $name) {
            $presence[$name] = [
                'getenv' => getenv($name) !== false,
                '_ENV' => array_key_exists($name, $_ENV),
                '_SERVER' => array_key_exists($name, $_SERVER),
                'laravel_env' => class_exists(Env::class)
                    ? Env::get($name) !== null
                    : null,
            ];
        }
        $role = getenv('DURABLE_WORKFLOW_PROCESS_ROLE');
        file_put_contents($log, json_encode([
            'stage' => $stage,
            'role_is_client' => $role === 'client',
            'role_is_worker' => $role === 'worker',
            'presence' => $presence,
        ], JSON_THROW_ON_ERROR).PHP_EOL, FILE_APPEND | LOCK_EX);
    }
}

$basePath = $argv[1] ?? null;
if (!is_string($basePath) || !is_file($basePath.'/artisan')) {
    throw new InvalidArgumentException('Pass the path to a fresh Laravel application.');
}

LaravelFreshRoleProbe::record('shell-entry');
require $basePath.'/vendor/autoload.php';
LaravelFreshRoleProbe::record('before-bootstrap');
$argv = array_merge([$basePath.'/artisan'], array_slice($argv, 2));
$_SERVER['argv'] = $argv;
$_SERVER['argc'] = count($argv);
require $basePath.'/artisan';
