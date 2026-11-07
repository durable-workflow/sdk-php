<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use RuntimeException;

final class CompletionPipelineScenario
{
    /** @var (callable(string, ?string): void)|null */
    private static $launchGate = null;

    /** A held qualification runner refuses every router launch the gate does not admit. */
    public static function gate(callable $gate): void
    {
        if (self::$launchGate !== null) {
            throw new RuntimeException('The completion fixture launch gate is already held.');
        }
        self::$launchGate = $gate;
    }

    /**
     * @template T
     * @param callable(string, string): T $scenario
     * @return T
     */
    public static function run(callable $scenario, ?string $router = null, bool $concurrent = false): mixed
    {
        if (!extension_loaded('curl') || !function_exists('proc_open')) {
            throw new RuntimeException('The completion pipeline socket fixture requires cURL and proc_open.');
        }
        if ($concurrent && (!function_exists('posix_setsid') || !function_exists('posix_kill') || !function_exists('pcntl_exec'))) {
            throw new RuntimeException('The concurrent Worker socket fixture requires Unix process groups.');
        }
        $router ??= __DIR__.'/completion-pipeline-http.php';
        $server = $concurrent ? __DIR__.'/completion-pipeline-server.php' : null;
        if (self::$launchGate !== null) {
            (self::$launchGate)($router, $server);
        }
        $directory = sys_get_temp_dir().'/sdk-php-completion-'.bin2hex(random_bytes(8));
        mkdir($directory, 0700);
        $state = $directory.'/state.json';
        file_put_contents($state, '{"events":[]}');
        $process = null;
        try {
            $socket = stream_socket_server('tcp://127.0.0.1:0', $code, $message);
            if (!is_resource($socket)) throw new RuntimeException('The completion fixture could not reserve a loopback port.');
            $address = stream_socket_get_name($socket, false);
            fclose($socket);
            $command = $server !== null ? [PHP_BINARY, $server, $address, $router]
                : [PHP_BINARY, '-S', $address, $router];
            $environment = ['PIPELINE_TEST_STATE' => $state];
            if ($concurrent) $environment['PHP_CLI_SERVER_WORKERS'] = '2';
            $process = proc_open($command,
                [0 => ['file', '/dev/null', 'r'], 1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']],
                $pipes, env_vars: $environment);
            if (!is_resource($process)) throw new RuntimeException('The completion fixture could not start.');
            $deadline = microtime(true) + 2;
            do {
                $ready = @stream_socket_client('tcp://'.$address, $code, $message, 0.05);
                if (is_resource($ready)) {
                    fclose($ready);
                    return $scenario('http://'.$address, $state);
                }
                usleep(10000);
            } while (microtime(true) < $deadline);
            throw new RuntimeException('The completion fixture did not become ready.');
        } finally {
            if (is_resource($process)) {
                $pid = proc_get_status($process)['pid'];
                if ($concurrent) {
                    posix_kill(-$pid, SIGTERM);
                } else {
                    proc_terminate($process);
                }
                proc_close($process);
                if ($concurrent) {
                    $deadline = microtime(true) + 1;
                    while (posix_kill(-$pid, 0) && microtime(true) < $deadline) usleep(1000);
                    if (posix_kill(-$pid, 0)) posix_kill(-$pid, SIGKILL);
                }
            }
            if (is_file($state)) unlink($state);
            rmdir($directory);
        }
    }

    /** @return list<array<string, mixed>> */
    public static function events(string $path): array
    {
        $handle = fopen($path, 'r');
        flock($handle, LOCK_SH);
        $value = json_decode(stream_get_contents($handle), true, flags: JSON_THROW_ON_ERROR);
        flock($handle, LOCK_UN);
        fclose($handle);
        return $value['events'];
    }

    public static function allowAnswer(string $path, bool $allowed = true): void
    {
        $handle = fopen($path, 'c+');
        flock($handle, LOCK_EX);
        $value = json_decode(stream_get_contents($handle), true, flags: JSON_THROW_ON_ERROR);
        $value['release'] = $allowed;
        rewind($handle);
        ftruncate($handle, 0);
        fwrite($handle, json_encode($value, JSON_THROW_ON_ERROR));
        flock($handle, LOCK_UN);
        fclose($handle);
    }
}
