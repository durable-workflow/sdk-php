<?php

declare(strict_types=1);

if (posix_setsid() < 0) {
    throw new RuntimeException('The concurrent fixture could not own its process group.');
}
pcntl_exec(PHP_BINARY, ['-S', $argv[1], $argv[2]]);
throw new RuntimeException('The concurrent fixture could not execute its HTTP server.');
