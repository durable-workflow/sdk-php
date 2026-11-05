<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use DurableWorkflow\Client;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Transport\Transport;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Promise\PromiseInterface;
use Psr\Http\Message\RequestInterface;
use ReflectionProperty;

require_once __DIR__.'/StaticCredentialProbeFailure.php';

/** Counts and refuses attempts without retaining URLs, bodies or credential headers. */
final class StaticCredentialTransportSpy implements Transport
{
    public int $attempts = 0;

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        $this->refuse();
    }

    /** @param array<string, mixed> $options */
    public function __invoke(RequestInterface $request, array $options): PromiseInterface
    {
        $this->refuse();
    }

    /** Instrument actual framework-created clients without replacing their authentication or factories. */
    public function attach(Client $client): void
    {
        $transport = (new ReflectionProperty(Client::class, 'transport'))->getValue($client);
        if ($transport === $this) {
            return;
        }
        if (!$transport instanceof Psr18Transport) {
            throw new StaticCredentialProbeFailure('Unsupported framework transport.');
        }
        $http = (new ReflectionProperty(Psr18Transport::class, 'client'))->getValue($transport);
        if (!$http instanceof GuzzleClient) {
            throw new StaticCredentialProbeFailure('Unsupported framework HTTP client.');
        }
        $handler = $http->getConfig('handler');
        if (!$handler instanceof HandlerStack) {
            throw new StaticCredentialProbeFailure('Unsupported framework HTTP handler.');
        }
        $handler->setHandler($this);
        // Bounded control requests use a distinct, lazily-created Guzzle client.
        (new ReflectionProperty(Psr18Transport::class, 'nativeControlClient'))->setValue($transport,
            new GuzzleClient(['http_errors' => false, 'handler' => HandlerStack::create($this)]));
    }

    private function refuse(): never
    {
        ++$this->attempts;
        throw new StaticCredentialProbeFailure('A credential fixture attempted transport.');
    }
}
