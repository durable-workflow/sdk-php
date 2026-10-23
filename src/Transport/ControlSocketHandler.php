<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

use Clue\React\HttpProxy\ProxyConnector;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Promise\Promise;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Psr7\Message;
use GuzzleHttp\Psr7\Response;
use GuzzleHttp\Psr7\Uri;
use GuzzleHttp\TransferStats;
use Psr\Http\Message\RequestInterface;
use React\EventLoop\StreamSelectLoop;
use React\Socket\ConnectionInterface;
use React\Socket\Connector;
use React\Socket\SecureConnector;
use Throwable;
use WpOrg\Requests\Requests;
use function React\Promise\Stream\buffer;
use function React\Promise\Timer\timeout;

/** @internal Guzzle adapter for owned control requests without cURL. No HTTP parser lives here. */
final class ControlSocketHandler
{
    /** @param array<string, mixed> $options */
    public function __invoke(RequestInterface $request, array $options): PromiseInterface
    {
        $loop = new StreamSelectLoop();
        $connection = null;
        $started = microtime(true);
        $seconds = (float) $options['timeout'];
        $connector = new Connector(['tls' => false, 'timeout' => false], $loop);
        $uri = $request->getUri();
        $host = $uri->getHost();
        if (!in_array($uri->getScheme(), ['http', 'https'], true) || $host === '') {
            throw new \InvalidArgumentException('Control requests require an absolute HTTP or HTTPS URI.');
        }
        $port = $uri->getPort() ?? ($uri->getScheme() === 'https' ? 443 : 80);
        $address = $host.':'.$port;
        $proxy = $this->proxy($request, $options['proxy'] ?? null);
        $proxyAuthorization = $request->getHeader('Proxy-Authorization');
        $wireRequest = $request->withoutHeader('Proxy-Authorization')->withHeader('Connection', 'close');
        if ($proxy !== null && $proxy !== '') {
            $proxyUri = $this->proxyUri($proxy);
            if (count($proxyAuthorization) > 1) {
                throw new \InvalidArgumentException('A proxy request accepts one Proxy-Authorization value.');
            }
            $authorization = $proxyAuthorization[0] ?? ($proxyUri->getUserInfo() !== ''
                ? 'Basic '.base64_encode(rawurldecode($proxyUri->getUserInfo())) : null);
            if ($uri->getScheme() === 'https') {
                $connector = new ProxyConnector((string) $proxyUri->withUserInfo(''), $connector,
                    $authorization !== null ? ['Proxy-Authorization' => $authorization] : []);
            } else {
                $address = $proxyUri->getHost().':'.($proxyUri->getPort() ?? 80);
                $wireRequest = $wireRequest->withRequestTarget((string) $uri->withUserInfo('')->withFragment(''));
                if ($authorization !== null) {
                    $wireRequest = $wireRequest->withHeader('Proxy-Authorization', $authorization);
                }
            }
        }
        if ($uri->getScheme() === 'https') {
            $connector = new SecureConnector($connector, $loop, [
                'verify_peer' => true, 'verify_peer_name' => true, 'allow_self_signed' => false,
                'crypto_method' => STREAM_CRYPTO_METHOD_TLSv1_2_CLIENT | STREAM_CRYPTO_METHOD_TLSv1_3_CLIENT,
            ]);
        }
        $connecting = $connector->connect($address);
        $reading = $connecting->then(static function (ConnectionInterface $stream) use (&$connection, $wireRequest) {
            $connection = $stream;
            $response = buffer($stream);
            $stream->write(Message::toString($wireRequest));

            return $response;
        });
        $bounded = timeout($reading, $seconds, $loop);
        $promise = new Promise(static fn () => $loop->run(), static function () use ($bounded, &$connection, $loop): void {
            $bounded->cancel();
            $connection?->close();
            $loop->stop();
        });
        $finish = static function (?Response $response, ?Throwable $error) use ($options, $request, $started, &$connection, $loop): void {
            $connection?->close();
            $loop->stop();
            if (isset($options['on_stats'])) {
                $options['on_stats'](new TransferStats($request, $response, microtime(true) - $started, $error));
            }
        };
        $bounded->then(static function (string $raw) use ($request, $promise, $finish): void {
            $response = null;
            $error = null;
            try {
                // Requests owns HTTP framing, chunk decoding and content coding.
                // This transport supplies buffered bytes and performs no I/O.
                $decoded = Requests::request((string) $request->getUri()->withUserInfo(''), [], [], $request->getMethod(), [
                    'transport' => new BufferedHttpResponse($raw), 'follow_redirects' => false,
                ]);
                $response = new Response($decoded->status_code, $decoded->headers->getAll(), $decoded->body);
                $promise->resolve($response);
            } catch (Throwable $exception) {
                $error = $exception;
                $promise->reject($exception);
            } finally {
                $finish($response, $error);
            }
        }, static function (Throwable $error) use ($request, $promise, $finish): void {
            $promise->reject(new ConnectException('The control HTTP connection failed: '.$error->getMessage(), $request, $error));
            $finish(null, $error);
        });

        return $promise;
    }

    private function proxyUri(#[\SensitiveParameter] string $proxy): Uri
    {
        try {
            $uri = new Uri(str_contains($proxy, '://') ? $proxy : 'http://'.$proxy);
        } catch (\InvalidArgumentException) {
            throw new \InvalidArgumentException('The HTTP proxy URL is invalid.');
        }
        if ($uri->getScheme() !== 'http' || $uri->getHost() === '') {
            throw new \InvalidArgumentException('HTTPS-scheme proxies are not supported by the native HTTP handler.');
        }

        return $uri;
    }

    private function proxy(RequestInterface $request, mixed $proxy): ?string
    {
        if (class_exists(\GuzzleHttp\ProxyOptions::class)) {
            return \GuzzleHttp\ProxyOptions::resolve($request->getUri(), $proxy)->getProxy();
        }
        if (is_array($proxy)) {
            $noProxy = $proxy['no'] ?? [];
            /** @var class-string $legacyUtility */
            $legacyUtility = \GuzzleHttp\Utils::class;
            $matchUri = [$legacyUtility, 'isUriInNoProxy'];
            $matchHost = [$legacyUtility, 'isHostInNoProxy'];
            if (!is_callable($matchHost)) {
                throw new \LogicException('The installed Guzzle lacks its legacy proxy matcher.');
            }
            $bypass = is_callable($matchUri)
                ? $matchUri($request->getUri(), $noProxy)
                : $matchHost($request->getUri()->getHost(), $noProxy);
            $proxy = $bypass ? null : ($proxy[$request->getUri()->getScheme()] ?? null);
        }
        if ($proxy !== null && !is_string($proxy)) {
            throw new \InvalidArgumentException('The HTTP proxy must be a string or a scheme-specific array.');
        }

        return $proxy;
    }
}
