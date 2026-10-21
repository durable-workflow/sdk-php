<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Transport\Psr18Transport;
use GuzzleHttp\Client as GuzzleClient;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkerActivityLeaseMaintenanceTest extends TestCase
{
    /** @var resource|null */
    private $server;
    /** @var resource|null */
    private $tlsServer = null;
    private string $directory;
    private string $uri;

    protected function setUp(): void
    {
        if (!function_exists('proc_open') || !extension_loaded('curl')) {
            self::markTestSkipped('Real PHP activity supervision needs proc_open and cURL.');
        }
        $this->directory = sys_get_temp_dir().'/dw-lease-'.bin2hex(random_bytes(8));
        mkdir($this->directory, 0700);
        file_put_contents($this->directory.'/state.json', '{}');
        $socket = stream_socket_server('tcp://127.0.0.1:0');
        self::assertIsResource($socket);
        $address = stream_socket_get_name($socket, false);
        fclose($socket);
        $this->uri = 'http://'.$address;
        $this->server = proc_open([PHP_BINARY, '-S', $address, __DIR__.'/Support/activity-lease-http.php'],
            [0 => ['file', '/dev/null', 'r'], 1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']],
            $pipes, env_vars: ['LEASE_TEST_STATE' => $this->directory.'/state.json']);
        self::assertIsResource($this->server);
        $until = microtime(true) + 5;
        do {
            $ready = @stream_socket_client('tcp://'.$address, $errno, $message, 0.1);
            if (is_resource($ready)) {
                fclose($ready);
                return;
            }
            usleep(10000);
        } while (microtime(true) < $until);
        self::fail('The isolated HTTP fixture did not start.');
    }

    protected function tearDown(): void
    {
        if (is_resource($this->tlsServer)) {
            proc_terminate($this->tlsServer);
            proc_close($this->tlsServer);
        }
        if (is_resource($this->server)) {
            proc_terminate($this->server);
            proc_close($this->server);
        }
        if (isset($this->directory)) {
            foreach (glob($this->directory.'/*') ?: [] as $path) {
                unlink($path);
            }
            rmdir($this->directory);
        }
    }

    public function testBlockingCallbackStaysInTheOriginalProcessAndOutlastsTheRuntimeLease(): void
    {
        $worker = $this->worker();
        $captured = 0;
        $parent = getmypid();
        $worker->registerActivity('slow', static function (ActivityContext $context) use (&$captured, $parent): int {
            self::assertSame($parent, getmypid());
            usleep(4200000);
            return ++$captured;
        });
        self::assertTrue($worker->tick(0));
        self::assertSame(1, $captured, 'Inline captured state must remain visible.');
        $state = $this->state();
        self::assertCount(1, $state['outcomes'] ?? [], 'A blocking callback must have one retained completion.');
        self::assertGreaterThanOrEqual(3, $state['renewals'] ?? 0);
        self::assertSame(0, $state['heartbeats'] ?? 0, 'Runtime pings must not fabricate progress.');
        self::assertCount(1, $state['outcomes']);
        $presence = array_values(array_filter($state['requests'],
            static fn (array $request): bool => $request['path'] === '/api/worker/heartbeat'));
        self::assertNotEmpty($presence);
        self::assertSame(['workflow_available' => 0, 'activity_available' => 0, 'session_available' => 0],
            $presence[0]['body']['task_slots']);
        self::assertSame($parent, $presence[0]['body']['process_metrics']['process_id']);
        foreach ($state['requests'] as $request) {
            self::assertSame('Bearer fixture-worker', $request['authorization']);
            self::assertSame('tenant', $request['namespace']);
        }
        $renewals = $state['renewals'];
        usleep(150000);
        self::assertSame($renewals, $this->state()['renewals'], 'Joined maintenance must stop after execution.');
    }

    #[DataProvider('lateOutcomes')]
    public function testExpiredBusinessAuthorityFencesBothReturnAndFailure(bool $throw): void
    {
        $worker = $this->worker(['business_seconds' => 2]);
        $calls = 0;
        $worker->registerActivity('slow', static function (ActivityContext $context) use ($throw, &$calls): string {
            ++$calls;
            usleep(2300000);
            if ($throw) {
                throw new \RuntimeException('Application error after authority expired.');
            }
            return 'late';
        });
        self::assertTrue($worker->tick(0));
        self::assertSame(1, $calls);
        self::assertSame([], $this->state()['outcomes'] ?? []);
        self::assertSame([], array_values(array_filter($this->state()['requests'],
            static fn (array $request): bool => str_ends_with($request['path'], '/complete') || str_ends_with($request['path'], '/fail'))));
    }

    public static function lateOutcomes(): array
    {
        return [[false], [true]];
    }

    public function testRuntimePingsCannotReplaceRequiredApplicationHeartbeats(): void
    {
        $worker = $this->worker(['heartbeat_seconds' => 2]);
        $worker->registerActivity('slow', static function (ActivityContext $context): string {
            usleep(2300000);
            return 'late';
        });
        self::assertTrue($worker->tick(0));
        self::assertSame(0, $this->state()['heartbeats'] ?? 0);
        self::assertSame([], $this->state()['outcomes'] ?? []);
    }

    public function testRealApplicationProgressIsRelayedWhileRuntimeMaintenanceContinues(): void
    {
        $worker = $this->worker(['heartbeat_seconds' => 3]);
        $worker->registerActivity('slow', static function (ActivityContext $context): string {
            for ($step = 0; $step < 4; ++$step) {
                usleep(800000);
                $context->heartbeat(['step' => $step]);
            }
            return 'done';
        });
        self::assertTrue($worker->tick(0));
        self::assertSame(4, $this->state()['heartbeats']);
        self::assertCount(1, $this->state()['outcomes']);
    }

    public function testChangedAttemptProofFencesOutcomeAfterCallbackReturns(): void
    {
        $worker = $this->worker(['corrupt_after' => 2]);
        $calls = 0;
        $worker->registerActivity('slow', static function (ActivityContext $context) use (&$calls): string {
            ++$calls;
            usleep(2300000);
            return 'must not publish';
        });
        self::assertTrue($worker->tick(0));
        self::assertSame(1, $calls);
        self::assertGreaterThanOrEqual(2, $this->state()['renewals']);
        self::assertSame([], $this->state()['outcomes'] ?? []);
    }

    public function testCaughtHeartbeatHttpRefusalCannotRestoreCompletionAuthority(): void
    {
        $worker = $this->worker(['deny_heartbeat' => true]);
        $caught = false;
        $worker->registerActivity('slow', static function (ActivityContext $context) use (&$caught): string {
            try {
                $context->heartbeat();
            } catch (\Throwable) {
                $caught = true;
            }
            return 'must not publish';
        });
        self::assertTrue($worker->tick(0));
        self::assertTrue($caught);
        self::assertSame([], $this->state()['outcomes'] ?? []);
        self::assertSame([], array_values(array_filter($this->state()['requests'],
            static fn (array $request): bool => str_ends_with($request['path'], '/complete') || str_ends_with($request['path'], '/fail'))));
    }

    public function testCustomHttpConfigurationMustNotSilentlyBecomeDefaultTransport(): void
    {
        $client = new Client($this->uri, transport: new Psr18Transport(new GuzzleClient(['verify' => '/private-ca.pem'])),
            namespace: 'tenant', workerToken: 'fixture-worker');
        $this->expectException(\InvalidArgumentException::class);
        $client->activityLeaseProcessConfiguration();
    }

    public function testMaintenanceStopsWhenTheOriginalOwnerIsKilled(): void
    {
        if (!function_exists('posix_kill')) {
            self::markTestSkipped('This owner-death control uses POSIX process observation.');
        }
        $autoload = dirname(__DIR__).'/vendor/autoload.php';
        $owner = proc_open([PHP_BINARY, __DIR__.'/Support/activity-lease-owner.php', $autoload, $this->uri],
            [0 => ['file', '/dev/null', 'r'], 1 => ['pipe', 'w'], 2 => ['file', '/dev/null', 'w']], $pipes);
        self::assertIsResource($owner);
        $helper = null;
        try {
            stream_set_timeout($pipes[1], 6);
            $ready = json_decode(fgets($pipes[1]) ?: '{}', true, 32, JSON_THROW_ON_ERROR);
            self::assertTrue($ready['ready'] ?? false);
            $helper = $ready['helper_pid'];
            self::assertTrue(posix_kill($helper, 0));
            proc_terminate($owner, 9);
            proc_close($owner);
            $owner = null;
            $until = microtime(true) + 3;
            while (posix_kill($helper, 0) && microtime(true) < $until) {
                usleep(10000);
            }
            self::assertFalse(posix_kill($helper, 0), 'Maintenance must not survive the original callback owner.');
            $count = $this->state()['renewals'];
            usleep(1100000);
            self::assertSame($count, $this->state()['renewals']);
            self::assertSame([], $this->state()['outcomes'] ?? []);
        } finally {
            if (is_resource($pipes[1])) {
                fclose($pipes[1]);
            }
            if (is_resource($owner)) {
                proc_terminate($owner, 9);
                proc_close($owner);
            }
            if (is_int($helper) && posix_kill($helper, 0)) {
                posix_kill($helper, 9);
            }
        }
    }

    public function testCustomBootstrapRecreatesItsHttpConfigurationInTheHelper(): void
    {
        $bootstrap = $this->directory.'/bootstrap.php';
        file_put_contents($bootstrap, '<?php return new \\DurableWorkflow\\Client('.var_export($this->uri, true).', '
            .'namespace: "tenant", workerToken: "fixture-worker", transport: new \\DurableWorkflow\\Transport\\Psr18Transport('
            .'new \\GuzzleHttp\\Client(["headers" => ["X-Maintenance-Bootstrap" => "retained"], "verify" => true])));');
        $client = (new Client($this->uri, namespace: 'tenant', workerToken: 'fixture-worker',
            transport: new Psr18Transport(new GuzzleClient(['verify' => true]))))->withActivityLeaseBootstrap($bootstrap);
        $worker = $this->worker(client: $client);
        $worker->registerActivity('slow', static fn (ActivityContext $context): int => 1);
        self::assertTrue($worker->tick(0));
        $renewals = array_values(array_filter($this->state()['requests'],
            static fn (array $request): bool => str_ends_with($request['path'], '/renew-lease')));
        self::assertNotEmpty($renewals);
        self::assertSame('retained', $renewals[0]['bootstrap']);
        self::assertCount(1, $this->state()['outcomes']);
    }

    public function testPrivateCaAndRotatingAuthenticationSurviveTheIndependentBootstrap(): void
    {
        if (!extension_loaded('openssl')) {
            self::markTestSkipped('The actual private-CA control requires OpenSSL.');
        }
        $httpUri = $this->uri;
        $this->uri = $this->privateTlsEndpoint($httpUri);
        try {
            (new Client($this->uri, workerToken: 'fixture-worker'))->workerClusterInfo();
            self::fail('The generated private certificate must not be trusted by default.');
        } catch (\DurableWorkflow\Exception\ServerException $error) {
            self::assertInstanceOf(\DurableWorkflow\Exception\TransportException::class, $error->getPrevious());
            self::assertStringContainsString('certificate', strtolower($error->getMessage()));
        }
        self::assertSame([], $this->state()['requests'] ?? [], 'Untrusted TLS must not reach the HTTP fixture.');

        $token = $this->directory.'/worker-token';
        file_put_contents($token, 'fixture-worker');
        $bootstrap = $this->directory.'/tls-bootstrap.php';
        file_put_contents($bootstrap, '<?php return new \\DurableWorkflow\\Client('.var_export($this->uri, true).', '
            .'authentication: new class implements \\DurableWorkflow\\Auth\\Authentication {'
            .'public function headers(bool $workerRequest): array { return ["Authorization" => "Bearer ".trim(file_get_contents('
            .var_export($token, true).'))]; } }, namespace: "tenant", '
            .'transport: new \\DurableWorkflow\\Transport\\Psr18Transport(new \\GuzzleHttp\\Client(["verify" => '
            .var_export($this->directory.'/certificate.pem', true).'])));');
        $client = (require $bootstrap)->withActivityLeaseBootstrap($bootstrap);
        $worker = $this->worker(['authentication_file' => $token], $client);
        $worker->registerActivity('slow', static function (ActivityContext $context) use ($token): int {
            file_put_contents($token, 'rotated-fixture-worker');
            usleep(4200000);
            return 17;
        });
        self::assertTrue($worker->tick(0));
        self::assertCount(1, $this->state()['outcomes'] ?? []);
        $renewals = array_values(array_filter($this->state()['requests'],
            static fn (array $request): bool => str_ends_with($request['path'], '/renew-lease')));
        self::assertGreaterThanOrEqual(3, count($renewals));
        self::assertSame('Bearer fixture-worker', $renewals[0]['authorization']);
        self::assertSame('Bearer rotated-fixture-worker', $renewals[count($renewals) - 1]['authorization']);
    }

    private function privateTlsEndpoint(string $upstream): string
    {
        $config = $this->directory.'/openssl.cnf';
        file_put_contents($config, "[req]\ndistinguished_name=dn\n[dn]\n[v3]\nsubjectAltName=IP:127.0.0.1\nbasicConstraints=critical,CA:TRUE\nextendedKeyUsage=serverAuth\n");
        $key = openssl_pkey_new(['private_key_bits' => 2048]);
        self::assertNotFalse($key);
        $request = openssl_csr_new(['commonName' => '127.0.0.1'], $key, ['config' => $config, 'digest_alg' => 'sha256']);
        self::assertNotFalse($request);
        $certificate = openssl_csr_sign($request, null, $key, 1,
            ['config' => $config, 'x509_extensions' => 'v3', 'digest_alg' => 'sha256']);
        self::assertNotFalse($certificate);
        self::assertTrue(openssl_x509_export_to_file($certificate, $this->directory.'/certificate.pem'));
        self::assertTrue(openssl_pkey_export_to_file($key, $this->directory.'/key.pem'));
        $ready = $this->directory.'/tls-ready';
        $this->tlsServer = proc_open([PHP_BINARY, __DIR__.'/Support/activity-lease-tls.php',
            $this->directory.'/certificate.pem', $this->directory.'/key.pem',
            str_replace('http://', 'tcp://', $upstream), $ready],
            [0 => ['file', '/dev/null', 'r'], 1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']], $pipes);
        self::assertIsResource($this->tlsServer);
        $until = microtime(true) + 5;
        do {
            if (is_file($ready)) {
                return 'https://'.file_get_contents($ready);
            }
            usleep(10000);
        } while (microtime(true) < $until);
        self::fail('The isolated TLS fixture did not start.');
    }

    public function testCallbackCannotPublishThroughItsCapturedClonedClientAfterExpiry(): void
    {
        $client = new Client($this->uri, namespace: 'tenant', workerToken: 'fixture-worker');
        $worker = $this->worker(['business_seconds' => 2], $client);
        $refused = false;
        $worker->registerActivity('slow', static function (ActivityContext $context) use ($client, &$refused): string {
            usleep(2300000);
            try {
                $client->withNamespace('tenant')->completeActivityTask('task', 'attempt', 'owner', 'late');
            } catch (\DurableWorkflow\Worker\WorkflowClaimRevoked) {
                $refused = true;
            }
            return 'also late';
        });
        self::assertTrue($worker->tick(0));
        self::assertTrue($refused);
        self::assertSame([], $this->state()['outcomes'] ?? []);
        self::assertSame([], array_values(array_filter($this->state()['requests'],
            static fn (array $request): bool => str_ends_with($request['path'], '/complete'))));
    }

    private function worker(array $configuration = [], ?Client $client = null): Worker
    {
        file_put_contents($this->directory.'/state.json', json_encode($configuration, JSON_THROW_ON_ERROR));
        $worker = new Worker($client ?? new Client($this->uri, namespace: 'tenant',
            controlToken: 'must-not-reach-maintenance', workerToken: 'fixture-worker'), 'queue', workerId: 'owner');
        // tick() is the real polling/execution path; only registration setup is
        // supplied directly so this fixture remains independent of rollout APIs.
        (new \ReflectionProperty($worker, 'registered'))->setValue($worker, true);
        (new \ReflectionProperty($worker, 'lastHeartbeatAt'))->setValue($worker, microtime(true));
        (new \ReflectionMethod($worker, 'captureRegistrationFence'))->invoke($worker,
            ['server_capabilities' => ['activity_lease_renewal' => true]]);

        return $worker;
    }

    private function state(): array
    {
        $file = fopen($this->directory.'/state.json', 'r');
        flock($file, LOCK_SH);
        $state = json_decode(stream_get_contents($file), true, 32, JSON_THROW_ON_ERROR);
        flock($file, LOCK_UN);
        fclose($file);

        return $state;
    }
}
