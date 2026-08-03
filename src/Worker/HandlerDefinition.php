<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Closure;

/** @internal Retains a handler contract separately from its invocation lifetime. */
final class HandlerDefinition
{
    /** @var Closure(): Closure */
    private readonly Closure $resolver;

    /** @param Closure(): Closure $resolver */
    private function __construct(
        private readonly Closure $contract,
        Closure $resolver,
    ) {
        $this->resolver = $resolver;
    }

    public static function shared(callable $handler): self
    {
        $contract = Closure::fromCallable($handler);

        return new self($contract, static fn (): Closure => $contract);
    }

    public static function replaySafe(object $prototype, string $method): self
    {
        $contract = Closure::fromCallable([$prototype, $method]);

        return new self(
            $contract,
            static function () use ($prototype, $method): Closure {
                $handler = clone $prototype;

                return Closure::fromCallable([$handler, $method]);
            },
        );
    }

    public function contract(): Closure
    {
        return $this->contract;
    }

    private function resolve(): Closure
    {
        return ($this->resolver)();
    }

    public function __invoke(mixed ...$arguments): mixed
    {
        return ($this->resolve())(...$arguments);
    }
}
