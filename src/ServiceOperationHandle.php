<?php

declare(strict_types=1);

namespace DurableWorkflow;

use DurableWorkflow\Model\ServiceOperationDescription;

/** Operations bound to one durable service call. */
final class ServiceOperationHandle
{
    public function __construct(
        private readonly Client $client,
        public readonly string $endpointName,
        public readonly string $serviceName,
        public readonly string $operationName,
        public readonly string $serviceCallId,
        public readonly ServiceOperationDescription $started,
    ) {
    }

    public function describe(): ServiceOperationDescription
    {
        return $this->client->describeServiceOperation(
            $this->endpointName,
            $this->serviceName,
            $this->operationName,
            $this->serviceCallId,
        );
    }

    public function cancel(?string $reason = null): ServiceOperationDescription
    {
        return $this->client->cancelServiceOperation(
            $this->endpointName,
            $this->serviceName,
            $this->operationName,
            $this->serviceCallId,
            $reason,
        );
    }
}
