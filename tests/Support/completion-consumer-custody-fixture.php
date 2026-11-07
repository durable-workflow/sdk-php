<?php

declare(strict_types=1);

// Exercise the actual subject reader in a fresh process, with a deterministic
// pathname replacement on close. No autoload, package, network or runtime runs.
require $argv[1];

use DurableWorkflow\Qualification\CompletionPollSubject;

final class CompletionCustodyFiles
{
    public mixed $context;
    public static string $target;
    public static string $replacement;
    public static int $reads = 0;
    private mixed $stream;
    private string $path;

    private static function native(callable $operation): mixed
    {
        stream_wrapper_restore('file');
        try {
            return $operation();
        } finally {
            stream_wrapper_unregister('file');
            stream_wrapper_register('file', self::class);
        }
    }

    public function stream_open(string $path, string $mode, int $options, ?string &$openedPath): bool
    {
        $this->path = $path;
        $this->stream = self::native(static fn () => fopen($path, $mode));
        if ($path === self::$target) ++self::$reads;
        return is_resource($this->stream);
    }

    public function stream_read(int $length): string { return fread($this->stream, $length); }
    public function stream_eof(): bool { return feof($this->stream); }
    public function stream_stat(): array|false { return fstat($this->stream); }
    public function stream_seek(int $offset, int $whence): bool { return fseek($this->stream, $offset, $whence) === 0; }
    public function stream_tell(): int { return ftell($this->stream); }
    public function url_stat(string $path, int $flags): array|false
    {
        return self::native(static fn () => ($flags & STREAM_URL_STAT_LINK) !== 0 ? @lstat($path) : @stat($path));
    }

    public function stream_close(): void
    {
        fclose($this->stream);
        if ($this->path === self::$target) {
            self::native(static fn () => file_put_contents(self::$target, self::$replacement));
        }
    }
}

$root = sys_get_temp_dir().'/completion-custody-'.bin2hex(random_bytes(8));
mkdir($root.'/vendor', 0700, true);
file_put_contents($root.'/vendor/autoload.php', '<?php throw new RuntimeException("Consumer autoload must not execute.");');
$reference = str_repeat('a', 40);
$lock = ['packages' => [['name' => 'durable-workflow/sdk', 'version' => '9.0.0',
    'dist' => ['type' => 'zip', 'reference' => $reference,
        'url' => 'https://api.github.com/repos/durable-workflow/sdk-php/zipball/'.$reference]]]];
file_put_contents($root.'/composer.lock', json_encode($lock, JSON_THROW_ON_ERROR));
file_put_contents($root.'/composer.json', json_encode(['require' => ['durable-workflow/sdk' => '9.0.0'],
    'config' => ['allow-plugins' => false]], JSON_THROW_ON_ERROR));
$subject = ['schema' => CompletionPollSubject::SCHEMA, 'runner_reference' => str_repeat('b', 40),
    'sdk_version' => '9.0.0', 'sdk_reference' => $reference,
    'lock_sha256' => hash_file('sha256', $root.'/composer.lock'), 'server_version' => '9.0.0',
    'server_image' => 'ghcr.io/durable-workflow/server@sha256:'.str_repeat('c', 64),
    'endpoint' => 'http://127.0.0.1:12345', 'namespace' => 'fixture', 'scenario' => 'carriers', 'deadline_seconds' => 30];
$bytes = json_encode($subject, JSON_THROW_ON_ERROR);
file_put_contents($root.'/subject.json', $bytes);
if (($argv[2] ?? null) === 'subject') {
    CompletionCustodyFiles::$target = $root.'/subject.json';
    CompletionCustodyFiles::$replacement = json_encode(array_replace($subject, ['endpoint' => 'http://127.0.0.1:54321']), JSON_THROW_ON_ERROR);
} elseif (($argv[2] ?? null) === 'lock') {
    CompletionCustodyFiles::$target = $root.'/composer.lock';
    $lock['packages'][0]['dist']['reference'] = str_repeat('d', 40);
    CompletionCustodyFiles::$replacement = json_encode($lock, JSON_THROW_ON_ERROR);
} else {
    throw new RuntimeException('Select one custody control.');
}
stream_wrapper_unregister('file');
stream_wrapper_register('file', CompletionCustodyFiles::class);
try {
    $selected = CompletionPollSubject::read($root.'/vendor/autoload.php', $root.'/subject.json', hash('sha256', $bytes));
    $result = ['selected' => $selected->data, 'reads' => CompletionCustodyFiles::$reads];
} catch (Throwable $error) {
    $result = ['error' => $error->getMessage(), 'reads' => CompletionCustodyFiles::$reads];
} finally {
    stream_wrapper_restore('file');
    foreach (['subject.json', 'composer.json', 'composer.lock', 'vendor/autoload.php'] as $name) unlink($root.'/'.$name);
    rmdir($root.'/vendor');
    rmdir($root);
}
echo json_encode($result, JSON_THROW_ON_ERROR).PHP_EOL;
