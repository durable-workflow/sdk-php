<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

use DurableWorkflow\Exception\TransportException;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Psr7\HttpFactory;
use GuzzleHttp\TransferStats;
use JsonException;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Client\NetworkExceptionInterface;
use Psr\Http\Message\RequestFactoryInterface;
use Psr\Http\Message\StreamFactoryInterface;
use Throwable;

/** Default PSR-18 JSON transport. */
final class Psr18Transport implements Transport
{
    private readonly ClientInterface $client;
    private readonly RequestFactoryInterface $requestFactory;
    private readonly StreamFactoryInterface $streamFactory;

    public function __construct(
        ?ClientInterface $client = null,
        ?RequestFactoryInterface $requestFactory = null,
        ?StreamFactoryInterface $streamFactory = null,
    ) {
        $factory = new HttpFactory();
        $this->client = $client ?? new GuzzleClient(['http_errors' => false]);
        $this->requestFactory = $requestFactory ?? $factory;
        $this->streamFactory = $streamFactory ?? $factory;
    }

    /**
     * @param array<string, string> $headers
     * @param array<string, mixed>|null $body
     * @return array<string, mixed>|list<mixed>|null
     */
    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        $connectionFailure = false;
        try {
            $request = $this->requestFactory->createRequest(strtoupper($method), $uri);
            foreach ($headers as $name => $value) {
                $request = $request->withHeader($name, $value);
            }
            if ($body !== null) {
                $json = json_encode($body, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
                $request = $request->withBody($this->streamFactory->createStream($json));
            }
            if ($this->client instanceof GuzzleClient) {
                $onStats = $this->client->getConfig('on_stats');
                $response = $this->client->send($request, [
                    'synchronous' => true,
                    'http_errors' => false,
                    'allow_redirects' => false,
                    'on_stats' => static function (TransferStats $stats) use (&$connectionFailure, $onStats): void {
                        // Guzzle 8 no longer attaches cURL errno to exceptions.
                        // Accept only DNS/connect/timeout/closed-connection errors,
                        // never TLS, invalid options, or an unclassified PSR error.
                        $connectionFailure = !$stats->hasResponse()
                            && in_array($stats->getHandlerErrorData(), [5, 6, 7, 28, 52, 55, 56], true);
                        if (is_callable($onStats)) {
                            $onStats($stats);
                        }
                    },
                ]);
            } else {
                $response = $this->client->sendRequest($request);
            }
            $rawBody = (string) $response->getBody();
            $decoded = $rawBody === '' ? null : json_decode($rawBody, true, 512, JSON_THROW_ON_ERROR);
            if ($decoded !== null && !is_array($decoded)) {
                throw new TransportException('The server returned a JSON value instead of an object or array.');
            }

            if ($response->getStatusCode() < 200 || $response->getStatusCode() >= 300) {
                throw TransportException::fromResponse($response->getStatusCode(), $decoded, $rawBody);
            }

            return $decoded;
        } catch (TransportException $exception) {
            throw $exception;
        } catch (JsonException $exception) {
            throw new TransportException('The server returned invalid JSON: '.$exception->getMessage(), previous: $exception);
        } catch (Throwable $exception) {
            throw new TransportException(
                'HTTP request failed: '.$exception->getMessage(),
                previous: $exception,
                transientConnectionFailure: $connectionFailure && $exception instanceof NetworkExceptionInterface,
            );
        }
    }
}
