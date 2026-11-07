<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Exception\ServerException;

/** @internal One reserved slot and its original completion-request identity. */
final class CarriedPoll
{
    /** @var array<string, mixed>|null */
    public ?array $completion = null;
    private bool $uncertain = false;
    private bool $leased = false;
    /** @var array<string, mixed>|null */
    private ?array $answer = null;

    /** @param array{worker_id: string, task_queue: string, poll_request_id: string, timeout_seconds: int} $request */
    public function __construct(
        public readonly array $request,
        public readonly float $startedAt,
    ) {
    }

    public function uncertainCompletion(): void
    {
        $this->uncertain = true;
    }

    /** Successful omissions are conclusive only without a preceding uncertain attempt. */
    public function observeCompletion(string $member): bool
    {
        if ($this->completion === null) {
            return true;
        }
        if (!array_key_exists($member, $this->completion)) {
            return $this->uncertain;
        }
        $result = $this->completion[$member];
        if (!is_array($result) || !is_bool($result['leased'] ?? null)
            || array_diff(array_keys($result), ['leased', 'answer']) !== []) {
            throw self::invalid();
        }
        $this->leased = $result['leased'];
        if (!$this->leased) {
            if (array_key_exists('answer', $result)) {
                throw self::invalid();
            }

            return $this->uncertain;
        }
        if (array_key_exists('answer', $result)) {
            $this->answer = $this->validateAnswer($result['answer']);
        }

        return true;
    }

    public function hasLease(): bool
    {
        return $this->leased;
    }

    /** @return array<string, mixed>|null */
    public function takeAnswer(): ?array
    {
        $answer = $this->answer;
        $this->answer = null;

        return $answer;
    }

    /** @return array<string, mixed> */
    private function validateAnswer(mixed $answer): array
    {
        $keys = ['task', 'poll_status', 'protocol_version', 'server_capabilities', 'server_time'];
        if (!is_array($answer) || array_diff($keys, array_keys($answer)) !== []
            || array_diff(array_keys($answer), $keys) !== []
            || !is_array($answer['task']) || array_is_list($answer['task'])
            || ($answer['task']['lease_owner'] ?? null) !== $this->request['worker_id']
            || $answer['poll_status'] !== 'leased'
            || $answer['protocol_version'] !== '1.20' || !is_array($answer['server_capabilities'])
            || !is_string($answer['server_time'])) {
            throw self::invalid();
        }

        return $answer;
    }

    private static function invalid(): ServerException
    {
        return new ServerException('The carried poll result violates the advertised contract.', 200,
            'invalid_carried_poll_response');
    }
}
