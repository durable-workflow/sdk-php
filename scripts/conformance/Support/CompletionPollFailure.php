<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use RuntimeException;
use Throwable;

/** Unknown execution failures cannot become a product verdict. */
final class CompletionPollFailure extends RuntimeException
{
    public static function product(string $code): self
    {
        return new self($code);
    }

    /** @return array{outcome: string, code: string, class: string} */
    public static function diagnostic(Throwable $error): array
    {
        $message = $error->getMessage();
        return ['outcome' => $error instanceof self ? 'product-fail' : 'runner-blocked',
            'code' => preg_match('/\Acompletion_[a-z_]+\z/', $message) === 1 ? $message : 'completion_consumer_failed',
            'class' => $error::class];
    }
}
