<?php

declare(strict_types=1);

namespace DurableWorkflow\Metrics;

use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Transport\ResponseStatusTransport;
use DurableWorkflow\Transport\Transport;
use Throwable;

/** @internal Per-request state, shared only by that request's internal retries. */
final class ClientRequestObservation
{
    public ?int $status = null;
    private ?Throwable $failure = null;
    private readonly int $started;

    public function __construct(
        private readonly MetricsObserver $observer,
        private readonly string $method,
        private readonly string $route,
        private readonly string $plane,
    ) {
        $this->started = hrtime(true);
    }

    public function captureStatus(Transport $transport): void
    {
        try {
            $this->status = $transport instanceof ResponseStatusTransport ? $transport->lastResponseStatus() : null;
        } catch (Throwable) {
            // An optional status provider cannot replace the request's result.
        }
    }

    public function failed(Throwable $error): void
    {
        $this->failure = $error;
        if ($error instanceof ServerException && $error->status > 0) {
            $this->status = $error->status;
        }
    }

    public function finish(): void
    {
        $outcome = ($this->status ?? 0) >= 400 ? 'http_error'
            : ($this->failure === null ? 'ok' : $this->failure::class);
        $this->observer->observe('client_request', (hrtime(true) - $this->started) / 1e9, [
            'method' => strtoupper($this->method), 'route' => $this->route, 'plane' => $this->plane,
            'status_code' => $this->status === null ? 'none' : (string) $this->status, 'outcome' => $outcome,
        ]);
    }
}
