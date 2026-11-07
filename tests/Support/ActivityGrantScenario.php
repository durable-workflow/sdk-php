<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use Composer\Autoload\ClassLoader;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use ReflectionClass;
use ReflectionMethod;
use ReflectionProperty;
use RuntimeException;

/** An inert HTTP delivery delay exercised through the public Worker. */
final class ActivityGrantScenario
{
    /** @return array{callbacks: int, commands: list<array<string, mixed>>} */
    public static function run(string $uri, string $statePath): array
    {
        file_put_contents($statePath, json_encode(['activity_count' => 2, 'lease_seconds' => 4,
            'delay_poll_delivery' => 2, 'poll_reply_delay_micros' => 4_250_000], JSON_THROW_ON_ERROR));
        $worker = new Worker(new Client($uri, namespace: 'tenant', workerToken: 'fixture-worker'),
            'queue', workerId: 'owner');
        (new ReflectionProperty($worker, 'registered'))->setValue($worker, true);
        (new ReflectionProperty($worker, 'lastHeartbeatAt'))->setValue($worker, microtime(true));
        (new ReflectionMethod($worker, 'captureRegistrationFence'))->invoke($worker,
            ['server_capabilities' => ['activity_lease_renewal' => true]]);
        $calls = 0;
        $worker->registerActivity('slow', static function (ActivityContext $context) use (&$calls): int { return ++$calls; });
        try {
            if (!$worker->tick(0)) {
                throw new RuntimeException('The original activity fixture task was not executed.');
            }
            try {
                $worker->tick(0);
            } catch (ServerException $error) {
                if ($error->status !== 409) {
                    throw $error;
                }
            }
            $state = json_decode((string) file_get_contents($statePath), true, flags: JSON_THROW_ON_ERROR);
            $codec = new AvroPayloadCodec();
            $commands = array_map(static fn (array $body): array => ['type' => 'complete_activity',
                'activity_attempt_id' => $body['activity_attempt_id'], 'result' => $codec->decodeEnvelope($body['result'])],
                $state['outcomes'] ?? []);

            return ['callbacks' => $calls, 'commands' => $commands];
        } finally {
            $worker->requestShutdown();
        }
    }

    /** Parent and helper must load the selected immutable SDK, not a cached vendor's SDK.
     * @return array{callbacks: int, commands: list<array<string, mixed>>}
     */
    public static function runIsolated(): array
    {
        return self::runWithRouter(self::run(...), __DIR__.'/activity-lease-http.php');
    }

    /**
     * @template T
     * @param callable(string, string): T $scenario
     * @return T
     */
    public static function runWithRouter(callable $scenario, string $router): mixed
    {
        if (!function_exists('proc_open') || !extension_loaded('curl')) {
            throw new RuntimeException('The activity grant fixture requires CLI proc_open and cURL.');
        }
        $directory = sys_get_temp_dir().'/activity-grant-'.bin2hex(random_bytes(8));
        mkdir($directory, 0700);
        mkdir($directory.'/vendor', 0700);
        $vendor = dirname((new ReflectionClass(ClassLoader::class))->getFileName(), 2);
        $source = dirname((new ReflectionClass(Worker::class))->getFileName(), 2);
        $bootstrap = "<?php\nrequire ".var_export($vendor.'/autoload.php', true).";\n"
            .'$loader = new \Composer\Autoload\ClassLoader(__DIR__);'."\n"
            .'$source = '.var_export($source.'/src', true).";\n"
            .'$loader->setPsr4("DurableWorkflow\\\\", $source); $loader->register(true);'."\n"
            .'foreach (["Client", "Worker", "Worker/ActivityLeaseProcess", "Worker/ActivityLeaseMaintainer"] as $name) {'."\n"
            .'    $file = (new \ReflectionClass("DurableWorkflow\\\\".str_replace("/", "\\\\", $name)))->getFileName();'."\n"
            .'    if (realpath($file) !== realpath($source."/".$name.".php")) {'."\n"
            .'        throw new \RuntimeException("Activity fixture loaded a different SDK source.");'."\n"
            ."    }\n}\nreturn \$loader;\n";
        file_put_contents($directory.'/vendor/autoload.php', $bootstrap);
        $loader = null;
        $server = null;
        try {
            $loader = require $directory.'/vendor/autoload.php';
            $socket = stream_socket_server('tcp://127.0.0.1:0');
            if (!is_resource($socket)) {
                throw new RuntimeException('The activity fixture cannot reserve a loopback address.');
            }
            $address = stream_socket_get_name($socket, false);
            fclose($socket);
            $server = proc_open([PHP_BINARY, '-S', $address, $router],
                [0 => ['file', '/dev/null', 'r'], 1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']],
                $pipes, env_vars: ['LEASE_TEST_STATE' => $directory.'/state.json']);
            if (!is_resource($server)) {
                throw new RuntimeException('The activity fixture HTTP process did not start.');
            }
            $until = microtime(true) + 5;
            do {
                $ready = @stream_socket_client('tcp://'.$address, $errno, $message, 0.1);
                if (is_resource($ready)) {
                    fclose($ready);

                    return $scenario('http://'.$address, $directory.'/state.json');
                }
                usleep(10000);
            } while (microtime(true) < $until);
            throw new RuntimeException('The activity fixture HTTP process was not ready.');
        } finally {
            if (is_resource($server)) {
                proc_terminate($server);
                proc_close($server);
            }
            if ($loader instanceof ClassLoader) {
                $loader->unregister();
            }
            foreach ([$directory.'/state.json', $directory.'/vendor/autoload.php'] as $path) {
                if (is_file($path)) {
                    unlink($path);
                }
            }
            rmdir($directory.'/vendor');
            rmdir($directory);
        }
    }
}
