<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Qualification\CompletionPollSubject;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;

require_once __DIR__.'/../scripts/conformance/Support/CompletionPollSubject.php';

final class CompletionPollConsumerSubjectTest extends TestCase
{
    private string $directory;
    private array $data;

    protected function setUp(): void
    {
        $this->directory = sys_get_temp_dir().'/completion-subject-'.bin2hex(random_bytes(6));
        mkdir($this->directory.'/vendor', 0700, true);
        file_put_contents($this->directory.'/vendor/autoload.php', '<?php throw new RuntimeException("Autoload must not execute in preflight.");');
        $project = ['require' => ['durable-workflow/sdk' => '9.0.0'], 'config' => ['allow-plugins' => false]];
        file_put_contents($this->directory.'/composer.json', json_encode($project, JSON_THROW_ON_ERROR));
        $lock = ['packages' => [['name' => 'durable-workflow/sdk', 'version' => '9.0.0',
            'dist' => ['type' => 'zip', 'reference' => str_repeat('a', 40),
                'url' => 'https://api.github.com/repos/durable-workflow/sdk-php/zipball/'.str_repeat('a', 40)]]]];
        file_put_contents($this->directory.'/composer.lock', json_encode($lock, JSON_THROW_ON_ERROR));
        $this->data = ['schema' => CompletionPollSubject::SCHEMA, 'runner_reference' => str_repeat('b', 40),
            'sdk_version' => '9.0.0', 'sdk_reference' => str_repeat('a', 40),
            'lock_sha256' => hash_file('sha256', $this->directory.'/composer.lock'),
            'server_version' => '9.0.0', 'server_image' => 'ghcr.io/durable-workflow/server@sha256:'.str_repeat('c', 64),
            'endpoint' => 'http://127.0.0.1:12345', 'namespace' => 'fixture', 'scenario' => 'carriers', 'deadline_seconds' => 30];
    }

    protected function tearDown(): void
    {
        foreach (['vendor/autoload.php', 'composer.json', 'composer.lock', 'subject.json'] as $name) {
            if (is_file($this->directory.'/'.$name)) unlink($this->directory.'/'.$name);
        }
        rmdir($this->directory.'/vendor');
        rmdir($this->directory);
    }

    public function testPreflightReadsTheCallerPacketWithoutLoadingConsumerCode(): void
    {
        $subject = $this->read();
        self::assertSame($this->data, $subject->data);
        self::assertSame($this->directory.'/vendor', $subject->vendor);
        self::assertSame(hash_file('sha256', $this->directory.'/subject.json'), $subject->sha256);
    }

    public function testLoadSelectionRequiresClosedCallerDatabaseAndConditionsBindings(): void
    {
        $this->data['scenario'] = 'load-remote-on';
        $this->data['load'] = ['backend' => 'postgresql', 'database_identity' => 'fresh-source-fixture',
            'repetition' => 1, 'conditions_sha256' => str_repeat('a', 64)];
        self::assertSame($this->data, $this->read()->data);
        $this->data['load']['reset_database'] = true;
        $this->expectExceptionMessage('completion_subject_load');
        $this->read();
    }

    #[DataProvider('replacedDocuments')]
    public function testPathReplacementCannotChangeTheAuthenticatedDocument(string $document): void
    {
        $process = proc_open([PHP_BINARY, __DIR__.'/Support/completion-consumer-custody-fixture.php',
            __DIR__.'/../scripts/conformance/Support/CompletionPollSubject.php', $document],
            [0 => ['file', '/dev/null', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
        self::assertIsResource($process);
        $output = stream_get_contents($pipes[1]);
        $errors = stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);
        self::assertSame(0, proc_close($process), $errors);
        self::assertSame('', $errors);
        $result = json_decode($output, true, flags: JSON_THROW_ON_ERROR);
        self::assertArrayNotHasKey('error', $result);
        self::assertSame(1, $result['reads']);
        self::assertSame('http://127.0.0.1:12345', $result['selected']['endpoint']);
    }

    public static function replacedDocuments(): iterable
    {
        yield 'subject pathname replaced after the authenticated read' => ['subject'];
        yield 'lock pathname replaced after the authenticated read' => ['lock'];
    }

    public function testAnOversizedSubjectRefusesEvenWhenItsDigestMatches(): void
    {
        $path = $this->directory.'/subject.json';
        file_put_contents($path, str_repeat(' ', 16385));
        $this->expectExceptionMessage('completion_subject_identity');
        CompletionPollSubject::read($this->directory.'/vendor/autoload.php', $path, hash_file('sha256', $path));
    }

    public function testAnOversizedLockRefusesEvenWhenItsDigestMatches(): void
    {
        $path = $this->directory.'/composer.lock';
        file_put_contents($path, str_repeat(' ', 4194305));
        $this->data['lock_sha256'] = hash_file('sha256', $path);
        $this->expectExceptionMessage('completion_subject_lock');
        $this->read();
    }

    #[DataProvider('invalidFields')]
    public function testUnclosedInputsRefuseBeforeConsumerAutoload(string $field, mixed $value, string $reason): void
    {
        $this->data[$field] = $value;
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage($reason);
        $this->read();
    }

    public static function invalidFields(): iterable
    {
        yield 'missing source reference' => ['sdk_reference', '', 'completion_subject_reference'];
        yield 'moving runner branch' => ['runner_reference', 'main', 'completion_subject_reference'];
        yield 'unselected version' => ['sdk_version', '^9.0', 'completion_subject_version'];
        yield 'mutable image' => ['server_image', 'durableworkflow/server:latest', 'completion_subject_runtime'];
        yield 'unknown scenario' => ['scenario', 'benchmark', 'completion_subject_runtime'];
        yield 'unbounded runtime' => ['deadline_seconds', 121, 'completion_subject_runtime'];
        yield 'external runtime' => ['endpoint', 'http://server.example:8080', 'completion_subject_loopback'];
        yield 'DNS host' => ['endpoint', 'http://localhost:8080', 'completion_subject_loopback'];
        yield 'credentials in URL' => ['endpoint', 'http://fixture:fixture@127.0.0.1:8080', 'completion_subject_loopback'];
        yield 'path override' => ['endpoint', 'http://127.0.0.1:8080/api', 'completion_subject_loopback'];
        yield 'unknown field' => ['execute', true, 'completion_subject_shape'];
        yield 'wrong lock seal' => ['lock_sha256', str_repeat('d', 64), 'completion_subject_lock'];
    }

    public function testASubjectMutationDoesNotPassItsPreviouslySelectedHash(): void
    {
        $path = $this->directory.'/subject.json';
        file_put_contents($path, json_encode($this->data, JSON_THROW_ON_ERROR));
        $selected = hash_file('sha256', $path);
        file_put_contents($path, '{}');
        $this->expectExceptionMessage('completion_subject_identity');
        CompletionPollSubject::read($this->directory.'/vendor/autoload.php', $path, $selected);
    }

    public function testAPathRepositoryCannotBeRelabeledAsARegistryConsumer(): void
    {
        $path = $this->directory.'/composer.json';
        $project = json_decode(file_get_contents($path), true, flags: JSON_THROW_ON_ERROR);
        $project['repositories'] = [['type' => 'path', 'url' => 'fixture-source']];
        file_put_contents($path, json_encode($project, JSON_THROW_ON_ERROR));
        $this->expectExceptionMessage('completion_subject_project');
        $this->read();
    }

    public function testTheCurrentSourceVendorCannotQualifyAsInstalledPackageIdentity(): void
    {
        $subject = $this->read();
        $this->expectExceptionMessage('completion_subject_installed_identity');
        $subject->installed();
    }

    private function read(): CompletionPollSubject
    {
        $path = $this->directory.'/subject.json';
        file_put_contents($path, json_encode($this->data, JSON_THROW_ON_ERROR));
        return CompletionPollSubject::read($this->directory.'/vendor/autoload.php', $path, hash_file('sha256', $path));
    }
}
