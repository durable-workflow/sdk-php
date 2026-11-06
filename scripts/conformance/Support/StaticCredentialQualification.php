<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use InvalidArgumentException;
use Throwable;

require_once __DIR__.'/StaticCredentialCases.php';

/** Reports only bounded fixture identity and setting names, never native diagnostic/value text. */
final class StaticCredentialQualification
{
    /**
     * @param array<string, mixed> $case
     * @return array<string, mixed>
     */
    public static function check(array $case): array
    {
        $spy = new StaticCredentialTransportSpy();
        $output = null;
        $error = null;
        try {
            $output = match (true) {
                $case['target'] === 'ServiceConfigurationConstructor' => StaticCredentialProbe::constructor($case['options']),
                $case['target'] === 'ProcessRole' => ['credential_role' => StaticCredentialProbe::workerRole($case['options'], $case['process'])],
                isset(StaticCredentialFrameworkProbe::targets()[$case['target']]) =>
                    StaticCredentialFrameworkProbe::headers($case['target'], $case['options'], $case['plane'], $spy, $case['process']),
                default => StaticCredentialProbe::headers($case['target'], $case['options'], $case['plane'], $spy, $case['process']),
            };
        } catch (Throwable $caught) {
            $error = $caught;
        }
        return self::verdict($case, $output, $error, $spy->attempts);
    }

    /**
     * @param array<string, mixed> $case
     * @param array<string, ?string>|null $output
     * @return array<string, mixed>
     */
    public static function verdict(array $case, ?array $output, ?Throwable $error, int $attempts): array
    {
        $setting = self::setting($error);
        $pass = $attempts === 0 && match ($case['expected']) {
            'preserve' => $error === null && $output === $case['output'],
            'reject' => $error instanceof InvalidArgumentException && $setting !== null && $setting === $case['setting'],
            'required-token' => $error instanceof InvalidArgumentException
                && $error->getMessage() === 'At least one non-empty authentication token is required.',
            'role-refusal' => self::roleRefusal($case, $error),
            'mode-refusal' => self::modeRefusal($case, $error),
            default => false,
        };
        $failure = $pass ? null : 'fixture_mismatch';
        if ($attempts !== 0) {
            $failure = 'transport_attempted';
        }
        return ['id' => $case['id'], 'target' => $case['target'], 'role' => $case['role'], 'plane' => $case['plane'],
            'verdict' => $pass ? 'PASS' : 'FAIL', 'setting' => $setting, 'transport_attempts' => $attempts,
            'failure' => $failure];
    }

    private static function setting(?Throwable $error): ?string
    {
        if (!$error instanceof InvalidArgumentException
            || preg_match('/\AInvalid static credential setting ([A-Za-z][A-Za-z0-9_.]{0,127})\.\z/', $error->getMessage(), $matches) !== 1) {
            return null;
        }
        $allowed = ['token', 'controlToken', 'workerToken', 'credentials.token', 'credentials.control_token',
            'credentials.worker_token', ...array_values(StaticCredentialProbe::ENVIRONMENT), 'DURABLE_WORKFLOW_PROCESS_TOKEN'];
        return in_array($matches[1], $allowed, true) ? $matches[1] : null;
    }

    /** @param array<string, mixed> $case */
    private static function roleRefusal(array $case, ?Throwable $error): bool
    {
        $laravel = $case['target'] === 'ProcessCredentialResolver'
            || (StaticCredentialFrameworkProbe::targets()[$case['target']]['framework'] ?? null) === 'laravel';
        $role = $case['plane'];
        $message = match (true) {
            $laravel && $role === 'control' => 'A client credential is required for the Laravel application client. Configure the self-hosted shared token or scoped client token.',
            $laravel => 'A worker credential is required for the Laravel worker client. Configure the self-hosted shared token or scoped worker token.',
            in_array($case['target'], ['Client', 'TokenAuthentication'], true) =>
                'A '.$role.' credential is required for this request. Configure a shared token or the scoped '.$role.' token.',
            $role === 'control' => 'A control credential is required for the application client. Configure the shared token or scoped control token.',
            default => 'A worker credential is required for the worker client. Configure the shared token or scoped worker token.',
        };
        return $error instanceof InvalidArgumentException && $error->getMessage() === $message;
    }

    /** @param array<string, mixed> $case */
    private static function modeRefusal(array $case, ?Throwable $error): bool
    {
        $laravel = $case['target'] === 'ProcessCredentialResolver'
            || (StaticCredentialFrameworkProbe::targets()[$case['target']]['framework'] ?? null) === 'laravel';
        $message = $laravel
            ? 'Configure either the self-hosted shared Durable Workflow token or scoped client and worker tokens, not both.'
            : 'Configure either the shared Durable Workflow token or scoped control and worker tokens, not both.';
        return $error instanceof InvalidArgumentException && $error->getMessage() === $message;
    }
}
