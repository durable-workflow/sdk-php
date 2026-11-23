<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

final class NonDeterministicWorkflow extends DurableWorkflowException
{
    /** @var array{function: string, file: string, line: int}|null */
    private ?array $workflowSource = null;

    public function __construct(
        string $message,
        public readonly ?int $sequence = null,
        public readonly ?string $expected = null,
        public readonly ?string $actual = null,
        public readonly string $reason = 'workflow_nondeterministic',
    ) {
        parent::__construct($message);
    }

    /** @return array{function: string, file: string, line: int}|null */
    public function workflowSource(): ?array
    {
        return $this->workflowSource;
    }

    /**
     * @internal Record diagnostics while the offending workflow is suspended.
     * @param array{function: string, file: string, line: int} $source
     */
    public function recordWorkflowSource(array $source): void
    {
        if ($this->workflowSource !== null) {
            return;
        }

        $this->workflowSource = $source;
        $this->message .= sprintf(' Authored at %s (%s:%d).', $source['function'], $source['file'], $source['line']);
    }
}
