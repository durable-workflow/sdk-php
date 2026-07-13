<?php

declare(strict_types=1);

namespace DurableWorkflow\Auth;

/** Supplies request headers without coupling the SDK to an auth provider. */
interface Authentication
{
    /** @return array<string, string> */
    public function headers(bool $workerRequest): array;
}
