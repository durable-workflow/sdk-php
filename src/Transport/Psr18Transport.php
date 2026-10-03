<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

use DurableWorkflow\Exception\ExternalPayloadException;
use DurableWorkflow\Exception\TransportException;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Psr7\FnStream;
use GuzzleHttp\Psr7\HttpFactory;
use GuzzleHttp\Psr7\Utils;
use GuzzleHttp\TransferStats;
use JsonException;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Client\NetworkExceptionInterface;
use Psr\Http\Message\RequestFactoryInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\StreamFactoryInterface;
use Psr\Http\Message\StreamInterface;
use Throwable;

/** Default PSR-18 JSON and bounded runtime-payload transport. */
final class Psr18Transport implements BoundedPayloadTransport, BoundedPayloadUploadTransport, BoundedTransport
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
        return $this->sendJson($method, $uri, $headers, $body);
    }

    public function supportsBoundedRequests(): bool
    {
        return $this->client instanceof GuzzleClient && extension_loaded('curl');
    }

    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        if (!$this->supportsBoundedRequests() || $timeoutSeconds < 1 || $timeoutSeconds > 65) {
            throw new \InvalidArgumentException('Bounded requests require Guzzle with cURL and a timeout from 1 through 65 seconds.');
        }

        return $this->sendJson($method, $uri, $headers, $body, $timeoutSeconds);
    }

    /**
     * @param array<string, string> $headers
     * @param array<string, mixed>|null $body
     * @return array<string, mixed>|list<mixed>|null
     */
    private function sendJson(string $method, string $uri, array $headers, ?array $body, ?int $timeoutSeconds = null): ?array
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
            $response = $this->sendRequest($request, timeoutSeconds: $timeoutSeconds ?? 30, bounded: $timeoutSeconds !== null);
            $rawBody = (string) $response->getBody();
            if ($response->getStatusCode() < 200 || $response->getStatusCode() >= 300) {
                // An upstream proxy can return HTML or an empty body. Preserve
                // its status without interpreting it as a successful protocol reply.
                $decoded = json_decode($rawBody, true);
                throw TransportException::fromResponse($response->getStatusCode(), is_array($decoded) ? $decoded : null, $rawBody);
            }

            $decoded = $rawBody === '' ? null : json_decode($rawBody, true, 512, JSON_THROW_ON_ERROR);
            if ($decoded !== null && !is_array($decoded)) {
                throw new TransportException('The server returned a JSON value instead of an object or array.');
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
        return $this->fetch($uri, $headers, $maxBytes);
    }

    public function fetchPayloadBounded(string $uri, array $headers, int $maxBytes, int $timeoutSeconds): string
    {
        return $this->fetch($uri, $headers, $maxBytes, $timeoutSeconds);
    }

    /** @param array<string, string> $headers */
    private function fetch(string $uri, array $headers, int $maxBytes, ?int $timeoutSeconds = null): string
    {
        if ($maxBytes < 0) {
            throw new \InvalidArgumentException('Payload read bound cannot be negative.');
        }
        $request = $this->requestFactory->createRequest('GET', $uri);
        foreach ($headers as $name => $value) {
            $request = $request->withHeader($name, $value);
        }
        $response = $timeoutSeconds === null
            ? $this->sendRequest($request, true)
            : $this->sendPayloadBounded($request, max($maxBytes, 65536), $timeoutSeconds);
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

    public function uploadPayload(string $uri, array $headers, string $blob, int $timeoutSeconds): array
    {
        return $this->upload($uri, $headers, $blob, $timeoutSeconds, false);
    }

    public function uploadPayloadBounded(string $uri, array $headers, string $blob, int $timeoutSeconds): array
    {
        return $this->upload($uri, $headers, $blob, $timeoutSeconds, true);
    }

    /** @param array<string, string> $headers
     * @return array<string, mixed>
     */
    private function upload(string $uri, array $headers, string $blob, int $timeoutSeconds, bool $bounded): array
    {
        $request = $this->requestFactory->createRequest('POST', $uri);
        foreach ($headers as $name => $value) {
            $request = $request->withHeader($name, $value);
        }
        $request = $request->withBody($this->streamFactory->createStream($blob));
        $response = $bounded
            ? $this->sendPayloadBounded($request, 65536, $timeoutSeconds)
            : $this->sendRequest($request, true, $timeoutSeconds);
        $stream = $response->getBody();
        try {
            $raw = '';
            while (!$stream->eof()) {
                $chunk = $stream->read(min(8192, 65537 - strlen($raw)));
                if ($chunk === '' && !$stream->eof()) { // @phpstan-ignore booleanNot.alwaysTrue
                    throw new ExternalPayloadException('Runtime upload response stopped before completion.', 503, 'external_payload_unavailable');
                }
                $raw .= $chunk;
                if (strlen($raw) > 65536) {
                    throw new ExternalPayloadException('Runtime upload response exceeds 64 KiB.', 422, 'external_payload_unsupported');
                }
            }
            $decoded = json_decode($raw, true);
            if ($response->getStatusCode() < 200 || $response->getStatusCode() >= 300) {
                throw TransportException::fromResponse($response->getStatusCode(), is_array($decoded) ? $decoded : null, $raw);
            }
            if (!is_array($decoded) || array_is_list($decoded)) {
                throw new ExternalPayloadException('Runtime upload response must be a JSON object.', 422, 'external_payload_unsupported');
            }

            return $decoded;
        } catch (TransportException|ExternalPayloadException $exception) {
            throw $exception;
        } catch (Throwable $exception) {
            throw new ExternalPayloadException('Runtime upload response could not be read.', 503, 'external_payload_unavailable', previous: $exception);
        } finally {
            $stream->close();
        }
    }

    private function sendPayloadBounded(RequestInterface $request, int $maxBytes, int $timeoutSeconds): ResponseInterface
    {
        if (!$this->supportsBoundedRequests() || $timeoutSeconds < 1 || $timeoutSeconds > 65) {
            throw new \InvalidArgumentException('Bounded payload requests require Guzzle with cURL and a timeout from 1 through 65 seconds.');
        }
        // A streaming response only bounds each read. Let cURL finish the entire
        // transfer within its deadline, using a finite sink that spills to disk.
        $target = Utils::streamFor(Utils::tryFopen('php://temp', 'w+'));
        $written = 0;
        $overflow = false;
        $sink = FnStream::decorate($target, [
            'write' => static function (string $bytes) use ($target, $maxBytes, &$written, &$overflow): int {
                if (strlen($bytes) > $maxBytes - $written) {
                    $overflow = true;
                    throw new ExternalPayloadException('Runtime payload response exceeds its read bound.', 422, 'external_payload_integrity_mismatch');
                }
                $count = $target->write($bytes);
                $written += $count;

                return $count;
            },
        ]);
        try {
            return $this->sendRequest($request, timeoutSeconds: $timeoutSeconds, bounded: true, sink: $sink);
        } catch (Throwable $exception) {
            $sink->close();
            if ($overflow) {
                throw new ExternalPayloadException('Runtime payload response exceeds its read bound.', 422, 'external_payload_integrity_mismatch', previous: $exception);
            }

            throw $exception;
        }
    }

    private function sendRequest(RequestInterface $request, bool $stream = false, int $timeoutSeconds = 30, bool $bounded = false, ?StreamInterface $sink = null): ResponseInterface
    {
        $connectionFailure = false;
        try {
            if ($this->client instanceof GuzzleClient) {
                $onStats = $this->client->getConfig('on_stats');

                return $this->client->send($request, [
                    'synchronous' => true,
                    'http_errors' => false,
                    'allow_redirects' => false,
                    ...($bounded ? ['stream' => false] : ($stream ? ['stream' => true] : [])),
                    ...($sink !== null ? ['stream' => false, 'sink' => $sink] : []),
                    ...($stream || $bounded ? ['timeout' => $timeoutSeconds, 'read_timeout' => $timeoutSeconds,
                        'connect_timeout' => $timeoutSeconds] : []),
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
