<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

use DurableWorkflow\Exception\ExternalPayloadException;
use DurableWorkflow\Exception\TransportException;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Psr7\HttpFactory;
use GuzzleHttp\TransferStats;
use JsonException;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Client\NetworkExceptionInterface;
use Psr\Http\Message\RequestFactoryInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\StreamFactoryInterface;
use Throwable;

/** Default PSR-18 JSON and bounded runtime-payload transport. */
final class Psr18Transport implements PayloadTransport
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
        try {
            $request = $this->requestFactory->createRequest(strtoupper($method), $uri);
            foreach ($headers as $name => $value) {
                $request = $request->withHeader($name, $value);
            }
            if ($body !== null) {
                $json = json_encode($body, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
                $request = $request->withBody($this->streamFactory->createStream($json));
            }
            $response = $this->sendRequest($request);
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
            throw new TransportException('HTTP request failed: '.$exception->getMessage(), previous: $exception);
        }
    }

    public function fetchPayload(string $uri, array $headers, int $maxBytes): string
    {
        if ($maxBytes < 0) {
            throw new \InvalidArgumentException('Payload read bound cannot be negative.');
        }
        $request = $this->requestFactory->createRequest('GET', $uri);
        foreach ($headers as $name => $value) {
            $request = $request->withHeader($name, $value);
        }
        $response = $this->sendRequest($request, true);
        $stream = $response->getBody();
        try {
            $status = $response->getStatusCode();
            if ($status < 200 || $status >= 300) {
                $raw = $stream->read(65536);
                $details = json_decode($raw, true);
                $reason = is_array($details) && is_string($details['reason'] ?? null)
                    ? $details['reason'] : match ($status) {
                        401, 403 => 'external_payload_unauthorized',
                        404 => 'external_payload_not_found',
                        410 => 'external_payload_expired',
                        413 => 'external_payload_oversized',
                        default => 'external_payload_unavailable',
                    };
                throw new ExternalPayloadException('Runtime external payload fetch failed.', $status, $reason,
                    is_array($details) ? $details : null);
            }
            foreach (['Codec', 'Size', 'SHA256'] as $field) {
                $name = 'X-Durable-Workflow-Payload-'.$field;
                if ($response->getHeaderLine($name) !== ($headers[$name] ?? null)) {
                    throw new ExternalPayloadException('Runtime payload response metadata differs from its reference.', 422, 'external_payload_integrity_mismatch');
                }
            }
            if (strtolower(trim(explode(';', $response->getHeaderLine('Content-Type'))[0])) !== 'application/octet-stream') {
                throw new ExternalPayloadException('Runtime payload response has an unsupported media type.', 415, 'external_payload_unsupported');
            }
            $body = '';
            while (!$stream->eof()) {
                $chunk = $stream->read(min(8192, $maxBytes - strlen($body) + 1));
                // read() advances EOF; PSR's stubs do not mark that mutation.
                if ($chunk === '' && !$stream->eof()) { // @phpstan-ignore booleanNot.alwaysTrue
                    throw new ExternalPayloadException('Runtime payload stream stopped before completion.', 503, 'external_payload_unavailable');
                }
                $body .= $chunk;
                if (strlen($body) > $maxBytes) {
                    throw new ExternalPayloadException('Runtime payload response exceeds its declared size.', 422, 'external_payload_integrity_mismatch');
                }
            }

            return $body;
        } catch (ExternalPayloadException $exception) {
            throw $exception;
        } catch (Throwable $exception) {
            throw new ExternalPayloadException('Runtime payload stream could not be read.', 503, 'external_payload_unavailable', previous: $exception);
        } finally {
            $stream->close();
        }
    }

    private function sendRequest(RequestInterface $request, bool $stream = false): ResponseInterface
    {
        $connectionFailure = false;
        try {
            if ($this->client instanceof GuzzleClient) {
                $onStats = $this->client->getConfig('on_stats');

                return $this->client->send($request, [
                    'synchronous' => true,
                    'http_errors' => false,
                    'allow_redirects' => false,
                    ...($stream ? ['stream' => true, 'timeout' => 30, 'read_timeout' => 30] : []),
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
            }

            return $this->client->sendRequest($request);
        } catch (Throwable $exception) {
            throw new TransportException(
                'HTTP request failed: '.$exception->getMessage(),
                previous: $exception,
                transientConnectionFailure: $connectionFailure && $exception instanceof NetworkExceptionInterface,
            );
        }
    }
}
