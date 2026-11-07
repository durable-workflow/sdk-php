<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

use Fiber;
use GuzzleHttp\Handler\CurlMultiHandler;
use GuzzleHttp\Promise\Promise;
use GuzzleHttp\Promise\PromiseInterface;
use Psr\Http\Message\RequestInterface;
use Throwable;

/** @internal Own the cURL transfer while its sending Fiber yields to one leased task. */
final class CompletionRequestHandler
{
    private readonly CurlMultiHandler $multi;

    public function __construct()
    {
        // Drive one cURL tick before yielding; do not wait for the response here.
        $this->multi = new CurlMultiHandler(['select_timeout' => 0.01]);
    }

    /**
     * @param array<string, mixed> $options
     * @param Fiber<mixed, mixed, mixed, mixed> $owner
     */
    public function __invoke(RequestInterface $request, array $options, Fiber $owner): PromiseInterface
    {
        $bytes = $request->getBody()->getSize();
        if ($bytes === null || $bytes < 1 || isset($options['progress'])) {
            return ($this->multi)($request, $options);
        }
        $sent = false;
        $options['progress'] = static function (float $downloadTotal, float $downloaded, float $uploadTotal, float $uploaded) use ($bytes, &$sent): void {
            $sent = $sent || ($uploadTotal >= $bytes && $uploaded >= $bytes);
        };
        $transfer = ($this->multi)($request, $options);
        $result = null;
        $result = new Promise(function () use ($transfer, $owner, &$sent, &$result): void {
            $yielded = false;
            try {
                while ($transfer->getState() === PromiseInterface::PENDING) {
                    $pending = $this->tickPending($transfer);
                    if ($sent && !$yielded && $pending
                        && Fiber::getCurrent() === $owner) {
                        $yielded = true;
                        // Suspend outside cURL's progress callback and its native call frame.
                        Fiber::suspend(new RequestBodySent());
                    }
                }
                $result->resolve($transfer->wait());
            } catch (Throwable $error) {
                $result->reject($error);
            } finally {
                if ($transfer->getState() === PromiseInterface::PENDING) {
                    $transfer->cancel();
                }
            }
        }, static function () use ($transfer): void {
            $transfer->cancel();
        });

        return $result;
    }

    private function tickPending(PromiseInterface $transfer): bool
    {
        $this->multi->tick();

        return $transfer->getState() === PromiseInterface::PENDING;
    }
}
