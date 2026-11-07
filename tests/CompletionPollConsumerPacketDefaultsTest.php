<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Qualification\CompletionPollPacketDefaults;
use PHPUnit\Framework\TestCase;

require_once __DIR__.'/../scripts/conformance/Support/CompletionPollFailure.php';
require_once __DIR__.'/../scripts/conformance/Support/CompletionPollPacketDefaults.php';

final class CompletionPollConsumerPacketDefaultsTest extends TestCase
{
    public function testGeneratedIdentityNormalizationPreservesReuseRelationships(): void
    {
        $first = [['body' => ['poll_request_id' => 'first']], ['body' => ['poll_request_id' => 'first']]];
        $same = [['body' => ['poll_request_id' => 'renamed']], ['body' => ['poll_request_id' => 'renamed']]];
        $different = [['body' => ['poll_request_id' => 'first']], ['body' => ['poll_request_id' => 'second']]];
        self::assertSame(CompletionPollPacketDefaults::digest($first), CompletionPollPacketDefaults::digest($same));
        self::assertNotSame(CompletionPollPacketDefaults::digest($first), CompletionPollPacketDefaults::digest($different));
    }

    public function testPayloadChangesCannotHideBehindGeneratedIdsOrDurations(): void
    {
        $first = [['body' => ['poll_request_id' => 'first', 'result' => ['data' => 'original'], 'duration_ms' => 20]]];
        $second = [['body' => ['poll_request_id' => 'second', 'result' => ['data' => 'changed'], 'duration_ms' => 30]]];
        self::assertNotSame(CompletionPollPacketDefaults::digest($first), CompletionPollPacketDefaults::digest($second));
    }

    public function testTheExistingFixedSocketConsumerKeepsDefaultsIdenticalAtSourceScope(): void
    {
        $result = CompletionPollPacketDefaults::run();
        self::assertTrue($result['default_option_false']);
        self::assertFalse($result['runtime_observed']);
        self::assertSame($result['implicit_request_count'], $result['explicit_request_count']);
        self::assertMatchesRegularExpression('/\A[a-f0-9]{64}\z/', $result['normalized_packets_sha256']);
    }
}
