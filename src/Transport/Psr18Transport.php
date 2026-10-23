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
use GuzzleHttp\HandlerStack;
use Throwable;

/** Default PSR-18 JSON and bounded runtime-payload transport. */
final class Psr18Transport implements BoundedPayloadTransport, BoundedPayloadUploadTransport, BoundedTransport, BoundedProgressPayloadTransport
{
    public const DEFAULT_REQUEST_TIMEOUT_SECONDS = 60.0;

    private readonly ClientInterface $client;
    private readonly RequestFactoryInterface $requestFactory;
    private readonly StreamFactoryInterface $streamFactory;
    private readonly bool $defaultProcessConfiguration;
    private readonly ?float $requestTimeoutSeconds;
    private ?GuzzleClient $nativeControlClient = null;

    public function __construct(
        ?ClientInterface $client = null,
        ?RequestFactoryInterface $requestFactory = null,
        ?StreamFactoryInterface $streamFactory = null,
        private readonly int $payloadTransferTimeoutSeconds = 30,
        ?float $requestTimeoutSeconds = null,
    ) {
        if ($payloadTransferTimeoutSeconds < 1 || $payloadTransferTimeoutSeconds > 900) {
            throw new \InvalidArgumentException('Supervised payload transfers require a timeout from 1 through 900 seconds.');
        }
        if ($client !== null && $requestTimeoutSeconds !== null) {
            throw new \InvalidArgumentException('Configure timeouts on the injected HTTP client instead of passing requestTimeoutSeconds.');
        }
        if ($requestTimeoutSeconds !== null && (!is_finite($requestTimeoutSeconds)
            || $requestTimeoutSeconds < 0.001 || $requestTimeoutSeconds > intdiv(PHP_INT_MAX, 1000))) {
            throw new \InvalidArgumentException('requestTimeoutSeconds must be a finite positive duration with millisecond precision.');
        }
        $this->requestTimeoutSeconds = $client === null ? ($requestTimeoutSeconds ?? self::DEFAULT_REQUEST_TIMEOUT_SECONDS) : null;
        $factory = new HttpFactory();
        $this->defaultProcessConfiguration = $client === null && $requestFactory === null && $streamFactory === null;
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

    /** @internal Custom HTTP clients require an application bootstrap in a fresh process. */
    public function supportsDefaultActivityLeaseProcess(): bool
    {
        return $this->defaultProcessConfiguration && $this->supportsBoundedRequests();
    }

    /** @internal Preserve the owned HTTP policy when recreating a maintenance Client. */
    public function defaultRequestTimeoutSeconds(): ?float
    {
        return $this->requestTimeoutSeconds;
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
        $controlTimeout = $timeoutSeconds === null && $this->isControlRequest($headers) ? $this->requestTimeoutSeconds : null;
        try {
            $request = $this->createJsonRequest($method, $uri, $headers, $body);
            $response = $this->sendRequest($request, timeoutSeconds: $timeoutSeconds ?? $controlTimeout ?? 30,
                bounded: $timeoutSeconds !== null || $controlTimeout !== null, controlTimeout: $controlTimeout);

            return $this->decodeJsonResponse($response);
        } catch (TransportException $exception) {
            throw $exception;
        } catch (JsonException $exception) {
            throw new TransportException('The server returned invalid JSON: '.$exception->getMessage(), previous: $exception);
        } catch (Throwable $exception) {
            if ($controlTimeout !== null && $this->isTimeout($exception)) {
                throw $this->controlTimeoutFailure($exception, $controlTimeout);
            }
            throw new TransportException('HTTP request failed: '.$exception->getMessage(), previous: $exception);
        }
    }

    /** @return array<string, mixed>|list<mixed>|null */
    private function decodeJsonResponse(ResponseInterface $response): ?array
    {
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
    }

    /**
     * @param array<string, string> $headers
     * @param array<string, mixed>|null $body
     */
    private function createJsonRequest(string $method, string $uri, array $headers, ?array $body): RequestInterface
    {
        $request = $this->requestFactory->createRequest(strtoupper($method), $uri);
        foreach ($headers as $name => $value) {
            $request = $request->withHeader($name, $value);
        }
        if ($body !== null) {
            $json = json_encode($body, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
            $request = $request->withBody($this->streamFactory->createStream($json));
        }

        return $request;
    }

    /** @param array<string, string> $headers */
    private function isControlRequest(array $headers): bool
    {
        $names = array_change_key_case($headers, CASE_LOWER);

        return isset($names['x-durable-workflow-control-plane-version'])
            || !isset($names['x-durable-workflow-protocol-version']);
    }

    private function isTimeout(Throwable $exception): bool
    {
        do {
            if ($exception instanceof \HTTP_Request2_Exception && $exception->getCode() === \HTTP_Request2_Exception::TIMEOUT) {
                return true;
            }
            $exception = $exception->getPrevious();
        } while ($exception !== null);

        return false;
    }

    private function controlTimeoutFailure(Throwable $exception, float $seconds): TransportException
    {
        return new TransportException('The control-plane HTTP request timed out.',
            response: ['reason' => 'request_timeout', 'timeout_seconds' => $seconds],
            previous: $exception, transientConnectionFailure: true);
    }

    public function fetchPayload(string $uri, array $headers, int $maxBytes): string
    {
        return $this->fetch($uri, $headers, $maxBytes);
    }

    public function fetchPayloadBounded(string $uri, array $headers, int $maxBytes, int $timeoutSeconds): string
    {
        return $this->fetch($uri, $headers, $maxBytes, $timeoutSeconds);
    }

    public function fetchPayloadWithProgress(string $uri, array $headers, int $maxBytes, \Closure $progress): string
    {
        return $this->fetch($uri, $headers, $maxBytes, progress: $progress);
    }

    public function fetchPayloadWithBoundedProgress(string $uri, array $headers, int $maxBytes, \Closure $progress, ?int $timeoutSeconds = null): string
    {
        return $this->fetch($uri, $headers, $maxBytes, $timeoutSeconds ?? $this->payloadTransferTimeoutSeconds, $progress);
    }

    /** @param array<string, string> $headers */
    private function fetch(string $uri, array $headers, int $maxBytes, ?int $timeoutSeconds = null, ?\Closure $progress = null): string
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
            : $this->sendPayloadBounded($request, max($maxBytes, 65536), $timeoutSeconds, $progress);
        $stream = $response->getBody();
        $progressFailure = null;
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
                try {
                    $progress?->__invoke();
                } catch (Throwable $exception) {
                    $progressFailure = $exception;
                    throw $exception;
                }
            }

            return $body;
        } catch (ExternalPayloadException $exception) {
            throw $exception;
        } catch (Throwable $exception) {
            if ($progressFailure !== null) {
                throw $progressFailure;
            }
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

    private function sendPayloadBounded(RequestInterface $request, int $maxBytes, int $timeoutSeconds, ?\Closure $progress = null): ResponseInterface
    {
        $maximumTimeout = $progress === null ? 65 : 900;
        if (!$this->supportsBoundedRequests() || $timeoutSeconds < 1 || $timeoutSeconds > $maximumTimeout) {
            throw new \InvalidArgumentException('Bounded payload requests require Guzzle with cURL and a timeout from 1 through '.$maximumTimeout.' seconds.');
        }
        // A streaming response only bounds each read. Let cURL finish the entire
        // transfer within its deadline, using a finite sink that spills to disk.
        $target = Utils::streamFor(Utils::tryFopen('php://temp', 'w+'));
        $written = 0;
        $overflow = false;
        $progressFailure = null;
        $supervise = $progress === null ? null : static function () use ($progress, &$progressFailure): void {
            try {
                $progress();
            } catch (Throwable $error) {
                $progressFailure = $error;
                throw $error;
            }
        };
        $sink = FnStream::decorate($target, [
            'write' => static function (string $bytes) use ($target, $maxBytes, &$written, &$overflow, $supervise): int {
                if (strlen($bytes) > $maxBytes - $written) {
                    $overflow = true;
                    throw new ExternalPayloadException('Runtime payload response exceeds its read bound.', 422, 'external_payload_integrity_mismatch');
                }
                $supervise?->__invoke();
                $count = $target->write($bytes);
                $written += $count;

                return $count;
            },
        ]);
        try {
            return $this->sendRequest($request, timeoutSeconds: $timeoutSeconds, bounded: true, sink: $sink, progress: $supervise);
        } catch (Throwable $exception) {
            $sink->close();
            // cURL invokes the callback while sendRequest() is running.
            /** @var Throwable|null $progressFailure */
            if ($progressFailure !== null) {
                throw $progressFailure;
            }
            if ($overflow) {
                throw new ExternalPayloadException('Runtime payload response exceeds its read bound.', 422, 'external_payload_integrity_mismatch', previous: $exception);
            }

            throw $exception;
        }
    }

    private function nativeControlHttpClient(): GuzzleClient
    {
        if (!$this->client instanceof GuzzleClient) {
            throw new \LogicException('Only the owned Guzzle client can select its native control handler.');
        }

        return $this->nativeControlClient ??= new GuzzleClient([
            'handler' => HandlerStack::create(new ControlSocketHandler()),
        ] + $this->client->getConfig());
    }

    private function sendRequest(RequestInterface $request, bool $stream = false, int|float $timeoutSeconds = 30, bool $bounded = false, ?StreamInterface $sink = null, ?\Closure $progress = null, ?float $controlTimeout = null): ResponseInterface
    {
        $connectionFailure = false;
        $timedOut = false;
        try {
            $http = $controlTimeout !== null && !function_exists('curl_exec') && !function_exists('curl_multi_exec')
                ? $this->nativeControlHttpClient() : $this->client;
            if ($http instanceof GuzzleClient) {
                $onStats = $http->getConfig('on_stats');

                $options = $this->guzzleRequestOptions($stream, $timeoutSeconds, $bounded, $sink, $progress);
                $options['on_stats'] = static function (TransferStats $stats) use (&$connectionFailure, &$timedOut, $onStats): void {
                    // Guzzle 8 no longer attaches cURL errno to exceptions.
                    // Accept only DNS/connect/timeout/closed-connection errors,
                    // never TLS, invalid options, or an unclassified PSR error.
                    $connectionFailure = !$stats->hasResponse()
                        && in_array($stats->getHandlerErrorData(), [5, 6, 7, 28, 52, 55, 56], true);
                    $timedOut = $stats->getHandlerErrorData() === 28;
                    if (is_callable($onStats)) {
                        $onStats($stats);
                    }
                };

                return $http->send($request, $options);
            }

            return $this->client->sendRequest($request);
        } catch (Throwable $exception) {
            throw $this->requestFailure($exception, $controlTimeout, $timedOut, $connectionFailure);
        }
    }

    /** @return array<string, mixed> */
    private function guzzleRequestOptions(bool $stream, int|float $timeoutSeconds, bool $bounded, ?StreamInterface $sink, ?\Closure $progress): array
    {
        $options = ['synchronous' => true, 'http_errors' => false, 'allow_redirects' => false];
        if ($bounded || $sink !== null) {
            $options['stream'] = false;
        } elseif ($stream) {
            $options['stream'] = true;
        }
        if ($sink !== null) {
            $options['sink'] = $sink;
        }
        if ($progress !== null) {
            $options['progress'] = $progress;
        }
        if ($stream || $bounded) {
            $options += ['timeout' => $timeoutSeconds, 'read_timeout' => $timeoutSeconds, 'connect_timeout' => $timeoutSeconds];
        }

        return $options;
    }

    private function requestFailure(Throwable $exception, ?float $controlTimeout, bool $timedOut, bool $connectionFailure): TransportException
    {
        if ($controlTimeout !== null) {
            if ($exception instanceof TransportException) {
                return $exception;
            }
            if ($timedOut || $this->isTimeout($exception)) {
                return $this->controlTimeoutFailure($exception, $controlTimeout);
            }
        }

        return new TransportException('HTTP request failed: '.$exception->getMessage(), previous: $exception,
            transientConnectionFailure: $connectionFailure && $exception instanceof NetworkExceptionInterface);
    }
}
