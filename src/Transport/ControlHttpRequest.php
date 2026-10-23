<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

/** @internal Converts the mature client's incomplete-transfer warnings to failures. */
final class ControlHttpRequest extends \HTTP_Request2
{
    /** @param array<string, mixed> $configuration */
    public function __construct(string $url, string $method, array $configuration)
    {
        parent::__construct($url, $method, $configuration);
        $this->attach(new class implements \SplObserver {
            public function update(\SplSubject $subject): void
            {
                if ($subject instanceof \HTTP_Request2 && $subject->getLastEvent()['name'] === 'warning') {
                    throw new \HTTP_Request2_MessageException('The HTTP response transfer was incomplete.',
                        \HTTP_Request2_Exception::DECODE_ERROR);
                }
            }
        });
    }
}
