<?php

declare(strict_types=1);

final class PhpLintUnavailable extends RuntimeException {}

// One container invocation; each file is linted by this runtime's PHP binary.
$failed = false;
foreach (['src', 'tests', 'examples'] as $directory) {
    $files = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($directory, FilesystemIterator::SKIP_DOTS));
    foreach ($files as $file) {
        if ($file->getExtension() !== 'php') {
            continue;
        }
        $process = proc_open([PHP_BINARY, '-l', $file->getPathname()], [STDIN, STDOUT, STDERR], $pipes);
        if (!is_resource($process)) {
            throw new PhpLintUnavailable('Cannot start PHP lint.');
        }
        $failed = proc_close($process) !== 0 || $failed;
    }
}
exit($failed ? 1 : 0);
