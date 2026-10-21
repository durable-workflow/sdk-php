<?php

declare(strict_types=1);

// Launched directly by PHP. Credentials and original metadata arrive only over stdin.
if (PHP_SAPI !== 'cli' || !isset($argv[1]) || !is_file($argv[1])) {
    exit(1);
}
require_once $argv[1];
DurableWorkflow\Worker\ActivityLeaseMaintainer::run();
