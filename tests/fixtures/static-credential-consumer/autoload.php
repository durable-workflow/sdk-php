<?php

declare(strict_types=1);

// Negative sentinel only. It is not an SDK/registry substitute and must never load.
echo "UNEXPECTED_CONSUMER_AUTOLOAD\n";
throw new LogicException('An unclosed consumer autoload was invoked.');
