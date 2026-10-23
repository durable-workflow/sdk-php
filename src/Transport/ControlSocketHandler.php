<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

use GuzzleHttp\Promise\Create;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Psr7\Response;
use GuzzleHttp\Psr7\Uri;
use GuzzleHttp\TransferStats;
use Psr\Http\Message\RequestInterface;
use Throwable;

/** @internal Adapts Guzzle's prepared control request to a mature total-deadline socket client. */
final class ControlSocketHandler
{
    /** @param array<string, mixed> $options */
    public function __invoke(RequestInterface $request, array $options): PromiseInterface
    {
        $started = microtime(true);
        $seconds = (float) $options['timeout'];
        $response = null;
        $error = null;
        try {
            $proxy = $this->proxy($request, $options['proxy'] ?? null);
            $native = new ControlHttpRequest((string) $request->getUri()->withUserInfo(''), $request->getMethod(), [
                'adapter' => new ControlSocketAdapter($started + $seconds, $seconds),
                'timeout' => $seconds, 'connect_timeout' => $seconds, 'follow_redirects' => false,
                'ssl_verify_peer' => true, 'ssl_verify_host' => true,
            ]);
            foreach ($request->withoutHeader('Proxy-Authorization')->withoutHeader('Expect')->getHeaders() as $name => $values) {
                $native->setHeader($name, implode(', ', $values));
            }
            // Match Guzzle's native no-Expect behavior; the library's separate
            // 100-Continue wait must not create another timeout allowance.
            $native->setHeader('Expect', '');
            $native->setHeader('Connection', 'close');
            $native->setBody((string) $request->getBody());
            if (!$request->hasHeader('Authorization') && $request->getUri()->getUserInfo() !== '') {
                [$user, $password] = array_pad(explode(':', $request->getUri()->getUserInfo(), 2), 2, '');
                $native->setAuth(rawurldecode($user), rawurldecode($password), \HTTP_Request2::AUTH_BASIC);
            }
            if ($proxy !== null && $proxy !== '') {
                $proxyUri = $this->proxyUri($proxy);
                $native->setConfig(['proxy_type' => 'http', 'proxy_host' => $proxyUri->getHost(),
                    'proxy_port' => $proxyUri->getPort() ?? 80]);
                $authorization = $request->getHeader('Proxy-Authorization');
                if (count($authorization) > 1) {
                    throw new \InvalidArgumentException('A proxy request accepts one Proxy-Authorization value.');
                }
                $value = $authorization[0] ?? ($proxyUri->getUserInfo() !== ''
                    ? $this->basicProxyAuthorization($proxyUri) : null);
                if ($value !== null) {
                    $native->setHeader('Proxy-Authorization', $value);
                }
            }
            $reply = $native->send();
            $response = new Response($reply->getStatus(), $reply->getHeader(), $reply->getBody(),
                $reply->getVersion(), $reply->getReasonPhrase());

            return Create::promiseFor($response);
        } catch (Throwable $exception) {
            $error = $exception;

            if (microtime(true) - $started >= $seconds) {
                $exception = new \DurableWorkflow\Exception\TransportException('The control-plane HTTP request timed out.',
                    response: ['reason' => 'request_timeout', 'timeout_seconds' => $seconds],
                    previous: $exception, transientConnectionFailure: true);
            }

            return Create::rejectionFor($exception);
        } finally {
            if (isset($options['on_stats'])) {
                $options['on_stats'](new TransferStats($request, $response, microtime(true) - $started, $error));
            }
        }
    }

    private function basicProxyAuthorization(Uri $proxy): string
    {
        [$user, $password] = array_pad(explode(':', $proxy->getUserInfo(), 2), 2, '');

        return 'Basic '.base64_encode(rawurldecode($user).':'.rawurldecode($password));
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
