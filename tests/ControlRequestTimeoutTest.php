<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\Psr18Transport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ControlRequestTimeoutTest extends TestCase
{
    #[DataProvider('stalledRequests')]
    public function testRealControlDeadlineIncludesHeadersAndBodyWithoutRepeatingTheMutation(string $mode, string $operation, bool $native): void
    {
        $this->withPeer($mode, static function (string $url, string $directory) use ($operation, $native): void {
            $command = [PHP_BINARY];
            if ($native) {
                array_push($command, '-d', 'disable_functions=curl_exec,curl_multi_exec');
            }
            array_push($command, __DIR__.'/Support/control-timeout-client.php', getenv('SDK_CONTROL_ACCEPTANCE_AUTOLOAD') ?: dirname(__DIR__).'/vendor/autoload.php', $url, $operation, '0.2');
            $process = proc_open($command, [0 => ['file', '/dev/null', 'r'],
                1 => ['file', $directory.'/stdout', 'w'], 2 => ['file', $directory.'/stderr', 'w']], $pipes);
            self::assertIsResource($process);
            try {
                $until = microtime(true) + 2;
                while (proc_get_status($process)['running'] && microtime(true) < $until) {
                    usleep(10000);
                }
                self::assertFalse(proc_get_status($process)['running'], 'The configured total deadline was not enforced.');
                $result = json_decode(file_get_contents($directory.'/stdout'), true, flags: JSON_THROW_ON_ERROR);
                self::assertSame('error', $result['outcome']);
                self::assertSame(0, $result['status']);
                self::assertSame('request_timeout', $result['reason']);
                self::assertGreaterThanOrEqual(0.15, $result['elapsed']);
                self::assertLessThan(1.0, $result['elapsed']);
                self::assertSame('', file_get_contents($directory.'/stderr'));
                $requests = array_map(static fn (string $line): array => json_decode($line, true, flags: JSON_THROW_ON_ERROR),
                    file($directory.'/requests', FILE_IGNORE_NEW_LINES));
                $actual = array_values(array_filter($requests, static fn (array $r): bool => !str_starts_with($r['path'], '/api/cluster/info')));
                self::assertCount(1, $actual, 'A timeout cannot authorize resubmission of an ambiguous mutation.');
            } finally {
                proc_terminate($process, 9);
                proc_close($process);
            }
        });
    }

    public static function stalledRequests(): iterable
    {
        foreach ([false, true] as $native) {
            foreach (['stall', 'slow_headers', 'slow_body'] as $mode) {
                foreach (['list', 'start', 'signal', 'update'] as $operation) {
                    yield $mode.' '.$operation.' native='.(int) $native => [$mode, $operation, $native];
                }
            }
        }
    }

    #[DataProvider('invalidTimeouts')]
    public function testInvalidTimeoutCannotDisableTheControlBudget(float $seconds): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new Client('https://server.example', requestTimeoutSeconds: $seconds);
    }

    public static function invalidTimeouts(): iterable
    {
        foreach ([0.0, -1.0, 0.0001, INF, NAN, (float) PHP_INT_MAX] as $seconds) {
            yield [$seconds];
        }
    }

    public function testInjectedTransportCannotBeSilentlyRebuiltToApplyAnSdkTimeout(): void
    {
        $transport = new FakeTransport();
        $this->expectException(\InvalidArgumentException::class);
        try {
            new Client('https://server.example', transport: $transport, requestTimeoutSeconds: 1);
        } finally {
            self::assertSame([], $transport->requests);
        }
    }

    public function testDefaultAndConfiguredBudgetsSurviveNamespaceAndMaintenanceConfiguration(): void
    {
        $default = new Client('https://server.example');
        self::assertSame(60.0, $default->activityLeaseProcessConfiguration()['request_timeout_seconds']);
        $configured = (new Client('https://server.example', requestTimeoutSeconds: 0.25))->withNamespace('billing');
        self::assertSame(0.25, $configured->activityLeaseProcessConfiguration()['request_timeout_seconds']);
    }

    #[DataProvider('httpHandlers')]
    public function testDynamicCredentialsAndNamespaceAreEvaluatedForEachRealRequest(bool $native): void
    {
        $this->withPeer('echo', function (string $url, string $directory) use ($native): void {
            $result = $this->runClient($directory, $url, $native, 'dynamic');
            self::assertSame('response', $result['outcome']);
            $requests = array_map(static fn (string $line): array => json_decode($line, true, flags: JSON_THROW_ON_ERROR),
                file($directory.'/requests', FILE_IGNORE_NEW_LINES));
            self::assertCount(2, $requests);
            self::assertSame('Bearer rotated-1', $requests[0]['headers']['authorization']);
            self::assertSame('Bearer rotated-2', $requests[1]['headers']['authorization']);
            self::assertSame('billing', $requests[1]['headers']['x-namespace']);
        });
    }

    #[DataProvider('httpHandlers')]
    public function testRedirectDoesNotResubmitCredentialsOrMutation(bool $native): void
    {
        $this->withPeer('redirect', function (string $url, string $directory) use ($native): void {
            $result = $this->runClient($directory, $url, $native);
            self::assertSame('error', $result['outcome']);
            self::assertSame(302, $result['status']);
            self::assertSame(1, count(file($directory.'/requests')));
        });
    }

    #[DataProvider('httpHandlers')]
    public function testTlsTrustAndHostnameVerificationRemainEnabled(bool $native): void
    {
        $this->withPeer('echo', function (string $url, string $directory) use ($native): void {
            $hostUrl = str_replace('127.0.0.1', 'localhost', $url);
            $untrusted = $this->runClient($directory, $hostUrl, $native);
            self::assertSame('error', $untrusted['outcome']);
            self::assertSame(0, $untrusted['status']);
            self::assertNull($untrusted['reason']);
            $trusted = $this->runClient($directory, $hostUrl, $native, caFile: $directory.'/cert.pem');
            self::assertSame('response', $trusted['outcome'], json_encode($trusted));
            $wrongHost = $this->runClient($directory, $url, $native, caFile: $directory.'/cert.pem');
            self::assertSame('error', $wrongHost['outcome']);
            self::assertSame(0, $wrongHost['status']);
            self::assertNull($wrongHost['reason']);
            $injected = $this->runClient($directory, $hostUrl, $native, 'injected', $directory.'/cert.pem');
            self::assertSame('response', $injected['outcome'], json_encode($injected));
        }, tls: true);
    }

    #[DataProvider('httpHandlers')]
    public function testTlsSlowBodyUsesTheTotalDeadline(bool $native): void
    {
        $this->withPeer('slow_body', function (string $url, string $directory) use ($native): void {
            $result = $this->runClient($directory, str_replace('127.0.0.1', 'localhost', $url), $native,
                caFile: $directory.'/cert.pem');
            self::assertSame('error', $result['outcome']);
            self::assertSame('request_timeout', $result['reason'], json_encode($result));
            self::assertGreaterThanOrEqual(0.4, $result['elapsed']);
            self::assertLessThan(1.0, $result['elapsed']);
        }, tls: true);
    }

    public function testFrameworkConfigurationPreservesControlBudgetAndSeparateWorkerBudget(): void
    {
        $configuration = \DurableWorkflow\Bridge\ServiceConfiguration::fromArray([
            'endpoint' => 'https://server.example', 'task_queue' => 'orders', 'request_timeout_seconds' => '0.25',
        ]);
        self::assertSame(0.25, $configuration->controlClient()->activityLeaseProcessConfiguration()['request_timeout_seconds']);
        self::assertSame(60.0, $configuration->workerClient()->activityLeaseProcessConfiguration()['request_timeout_seconds']);
    }

    public function testInjectedHttpClientCannotBeSilentlyRebuiltToApplyAnSdkTimeout(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new Psr18Transport(new \GuzzleHttp\Client(['verify' => '/private/ca.pem']), requestTimeoutSeconds: 1);
    }

    #[DataProvider('httpHandlers')]
    public function testControlDeadlineDoesNotShortenWorkerOrPayloadRequests(bool $native): void
    {
        $this->withPeer('delay', function (string $url, string $directory) use ($native): void {
            foreach (['worker', 'payload'] as $operation) {
                $result = $this->runClient($directory, $url, $native, operation: $operation, seconds: '0.05');
                self::assertSame('response', $result['outcome'], json_encode($result));
                self::assertGreaterThanOrEqual(0.2, $result['elapsed']);
            }
        });
    }

    #[DataProvider('proxyHandlers')]
    public function testEnvironmentProxyKeepsOriginAndProxyCredentialsSeparate(bool $native, bool $tlsProxy, bool $tlsOrigin): void
    {
        $this->withPeer('echo', function (string $url, string $directory) use ($native, $tlsProxy, $tlsOrigin): void {
            $caFile = $tlsOrigin ? $directory.'/cert.pem' : '';
            $command = [PHP_BINARY, __DIR__.'/Support/control-timeout-proxy.php', $directory];
            if ($tlsProxy) {
                array_push($command, $directory.'/cert.pem', $directory.'/key.pem');
            }
            $proxy = proc_open($command, [0 => ['file', '/dev/null', 'r'],
                1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']], $pipes);
            self::assertIsResource($proxy);
            try {
                $until = microtime(true) + 2;
                while (!is_file($directory.'/proxy-address') && microtime(true) < $until) {
                    usleep(10000);
                }
                self::assertFileExists($directory.'/proxy-address');
                $address = str_replace('127.0.0.1', 'localhost', trim(file_get_contents($directory.'/proxy-address')));
                $proxyUrl = ($tlsProxy ? 'https://' : 'http://').'proxy-user:proxy-secret@'.$address;
                $url = str_replace('127.0.0.1', 'localhost', $url);
                $result = $this->runClient($directory, $url, $native, caFile: $caFile,
                    environment: ['HTTP_PROXY' => $proxyUrl, 'HTTPS_PROXY' => $proxyUrl, 'NO_PROXY' => '']);
                if ($tlsProxy && !$native) {
                    // Preserve libcurl's separate proxy trust policy. A private
                    // proxy CA is not implicitly trusted by this origin CA.
                    self::assertSame('error', $result['outcome']);
                    self::assertNull($result['reason']);
                    self::assertFileDoesNotExist($directory.'/requests');
                    $bypassed = $this->runClient($directory, $url, false, caFile: $caFile,
                        environment: ['HTTP_PROXY' => 'http://127.0.0.1:1', 'HTTPS_PROXY' => 'http://127.0.0.1:1', 'NO_PROXY' => 'localhost']);
                    self::assertSame('response', $bypassed['outcome'], json_encode($bypassed));

                    return;
                }
                self::assertSame('response', $result['outcome'], json_encode($result));
                $requests = file($directory.'/requests', FILE_IGNORE_NEW_LINES);
                self::assertCount(1, $requests);
                $origin = json_decode($requests[0], true, flags: JSON_THROW_ON_ERROR);
                self::assertSame('Bearer rotated-1', $origin['headers']['authorization']);
                self::assertArrayNotHasKey('proxy-authorization', $origin['headers']);
                $rawProxy = file_get_contents($directory.'/proxy-requests');
                self::assertStringContainsString('Basic '.base64_encode('proxy-user:proxy-secret'), $rawProxy);
                if ($tlsOrigin || $native) {
                    self::assertStringNotContainsString('rotated-1', $rawProxy);
                }

                $bypassed = $this->runClient($directory, $url, $native, caFile: $caFile,
                    environment: ['HTTP_PROXY' => 'http://127.0.0.1:1', 'HTTPS_PROXY' => 'http://127.0.0.1:1', 'NO_PROXY' => 'localhost']);
                self::assertSame('response', $bypassed['outcome'], json_encode($bypassed));
            } finally {
                proc_terminate($proxy, 9);
                proc_close($proxy);
            }
        }, tls: $tlsOrigin);
    }

    public static function proxyHandlers(): iterable
    {
        foreach ([false, true] as $native) {
            yield 'plain origin native='.(int) $native => [$native, false, false];
            foreach ([false, true] as $tlsProxy) {
                yield 'native='.(int) $native.' tlsProxy='.(int) $tlsProxy => [$native, $tlsProxy, true];
            }
        }
    }

    public static function httpHandlers(): iterable
    {
        yield 'cURL' => [false];
        yield 'without cURL' => [true];
    }

    private function runClient(string $directory, string $url, bool $native, string $profile = 'default', string $caFile = '', string $operation = 'list', string $seconds = '0.5', ?array $environment = null): array
    {
        $command = [PHP_BINARY];
        if ($native) {
            array_push($command, '-d', 'disable_functions=curl_exec,curl_multi_exec');
        }
        if ($caFile !== '' && $profile !== 'injected') {
            array_push($command, '-d', 'openssl.cafile='.$caFile);
        }
        array_push($command, __DIR__.'/Support/control-timeout-client.php', getenv('SDK_CONTROL_ACCEPTANCE_AUTOLOAD') ?: dirname(__DIR__).'/vendor/autoload.php',
            $url, $operation, $seconds, $profile, $caFile);
        $process = proc_open($command, [0 => ['file', '/dev/null', 'r'],
            1 => ['file', $directory.'/stdout', 'w'], 2 => ['file', $directory.'/stderr', 'w']], $pipes, env_vars: $environment);
        self::assertIsResource($process);
        try {
            $until = microtime(true) + 2;
            while (proc_get_status($process)['running'] && microtime(true) < $until) {
                usleep(10000);
            }
            self::assertFalse(proc_get_status($process)['running']);
            self::assertSame('', file_get_contents($directory.'/stderr'));

            return json_decode(file_get_contents($directory.'/stdout'), true, flags: JSON_THROW_ON_ERROR);
        } finally {
            proc_terminate($process, 9);
            proc_close($process);
        }
    }

    private function withPeer(string $mode, \Closure $test, bool $tls = false): void
    {
        if (!function_exists('proc_open')) {
            self::markTestSkipped('The real deadline fixture requires proc_open.');
        }
        $directory = sys_get_temp_dir().'/dw-control-timeout-'.bin2hex(random_bytes(8));
        mkdir($directory, 0700);
        $command = [PHP_BINARY, __DIR__.'/Support/control-timeout-peer.php', $directory, $mode];
        if ($tls) {
            file_put_contents($directory.'/openssl.cnf', "[req]\ndistinguished_name=dn\n[dn]\n[v3]\nsubjectAltName=DNS:localhost\nbasicConstraints=CA:TRUE\n");
            $key = openssl_pkey_new(['private_key_bits' => 2048, 'private_key_type' => OPENSSL_KEYTYPE_RSA]);
            $options = ['config' => $directory.'/openssl.cnf', 'x509_extensions' => 'v3', 'digest_alg' => 'sha256'];
            $csr = openssl_csr_new(['commonName' => 'localhost'], $key, $options);
            $cert = openssl_csr_sign($csr, null, $key, 1, $options);
            openssl_x509_export_to_file($cert, $directory.'/cert.pem');
            openssl_pkey_export_to_file($key, $directory.'/key.pem');
            array_push($command, $directory.'/cert.pem', $directory.'/key.pem');
        }
        $process = proc_open($command,
            [0 => ['file', '/dev/null', 'r'], 1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']], $pipes);
        self::assertIsResource($process);
        try {
            $until = microtime(true) + 3;
            while (!is_file($directory.'/address') && microtime(true) < $until) {
                usleep(10000);
            }
            self::assertFileExists($directory.'/address');
            $test(($tls ? 'https://' : 'http://').trim(file_get_contents($directory.'/address')), $directory);
        } finally {
            proc_terminate($process, 9);
            proc_close($process);
            foreach (glob($directory.'/*') as $file) {
                unlink($file);
            }
            rmdir($directory);
        }
    }
}
