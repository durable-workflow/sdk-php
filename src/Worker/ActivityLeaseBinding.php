<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Closure;
use Fiber;
use WeakMap;

/** @internal Cloned Clients share a binding; concurrent Fibers do not share claims. */
final class ActivityLeaseBinding
{
    /** @var WeakMap<object, ActivityLeaseProcess> Only Fiber objects are used as keys. */
    private WeakMap $fibers;
    private ?ActivityLeaseProcess $main = null;

    public function __construct()
    {
        $this->fibers = new WeakMap();
    }

    public function current(): ?ActivityLeaseProcess
    {
        $fiber = Fiber::getCurrent();

        return $fiber === null ? $this->main : ($this->fibers[$fiber] ?? null);
    }

    public function enter(ActivityLeaseProcess $process): Closure
    {
        if ($this->current() !== null) {
            throw new \LogicException('An activity execution context already owns a lease supervisor.');
        }
        $fiber = Fiber::getCurrent();
        if ($fiber === null) {
            $this->main = $process;
        } else {
            $this->fibers[$fiber] = $process;
        }

        return function () use ($fiber, $process): void {
            if ($fiber === null && $this->main === $process) {
                $this->main = null;
            } elseif ($fiber !== null && ($this->fibers[$fiber] ?? null) === $process) {
                unset($this->fibers[$fiber]);
            }
        };
    }
}
