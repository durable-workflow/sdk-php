<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\FailureReportRefused;
use DurableWorkflow\Failure\FailureReportLimiter;
use DurableWorkflow\Failure\FailureReportLimits;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\JsonDocument;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Transport\RequestSizeLimitTransport;
use DurableWorkflow\Transport\Transport;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

final class FailureReportTest extends TestCase
{
    #[DataProvider('largeReports')]
    public function testActualPsrRequestBoundsDiagnosticsAndPreservesClaimAndSemantics(string $scenario): void
    {
        $http = new class implements ClientInterface {
            public array $requests = [];
            public function sendRequest(RequestInterface $request): ResponseInterface
            {
                $this->requests[] = $request;
                return new Response(200, ['Content-Type' => 'application/json'], '{}');
            }
        };
        $client = new Client('https://recording.invalid', transport: new Psr18Transport(client: $http));
        if (in_array($scenario, ['lower-limit', 'lower-details'], true)) {
            $client = $client->withFailureReportLimit(4096);
        }
        $large = str_repeat('m', 3 * 1024 * 1024);
        $message = in_array($scenario, ['message', 'combined', 'lower-limit'], true) ? $large : 'original message';
        $stack = in_array($scenario, ['stack', 'combined', 'lower-limit'], true) ? $large : null;
        if ($scenario === 'escaped') {
            $message = str_repeat("\0\"\\雪🦀", 300000);
            $stack = str_repeat("\0\"\\雪🦀", 300000);
        }
        $details = match ($scenario) {
            'details' => ['nested' => ['cause' => $large]],
            'lower-details' => ['text' => str_repeat('d', 45000)],
            default => ['whole' => 1.0, 'zero' => -0.0],
        };
        $client->failActivityTask('original-task', 'original-attempt', 'original-owner', $message,
            'OriginalFailure', true, $details, $stack, ['authored_retry_delay' => 12.5]);
        $reports = array_values(array_filter($http->requests,
            static fn (RequestInterface $request): bool => str_ends_with($request->getUri()->getPath(), '/fail')));
        self::assertCount(1, $reports);
        $request = $reports[0];
        $wire = (string) $request->getBody();
        self::assertLessThanOrEqual(in_array($scenario, ['lower-limit', 'lower-details'], true) ? 4096 : 1048576, strlen($wire));
        self::assertSame('/api/worker/activity-tasks/original-task/fail', $request->getUri()->getPath());
        $body = json_decode($wire, true, flags: JSON_THROW_ON_ERROR);
        self::assertSame('original-attempt', $body['activity_attempt_id']);
        self::assertSame('original-owner', $body['lease_owner']);
        $failure = $body['failure'];
        self::assertSame('OriginalFailure', $failure['type']);
        self::assertTrue($failure['non_retryable']);
        self::assertLessThanOrEqual(63488, strlen($failure['message']));
        self::assertLessThanOrEqual(131072, strlen($failure['stack_trace'] ?? ''));
        self::assertSame(1, preg_match('//u', $failure['message']));
        $metadata = array_intersect_key($failure, array_flip(['details', 'diagnostics', 'runtime_diagnostics']));
        self::assertLessThanOrEqual(65536, JsonDocument::size($metadata));
        if ($scenario === 'small') {
            self::assertSame($message, $failure['message']);
            self::assertSame(['authored_retry_delay' => 12.5], $failure['diagnostics']);
        } else {
            self::assertStringContainsString('[truncated failure report;', $failure['message']);
            self::assertNotEmpty($failure['diagnostics']['truncation']['fields']);
        }
        if (in_array($scenario, ['details', 'lower-details'], true)) {
            self::assertArrayNotHasKey('details', $failure);
            self::assertTrue($failure['diagnostics']['truncation']['fields']['details']['omitted']);
        } elseif ($scenario !== 'lower-limit') {
            self::assertSame($client->payloadCodec()->envelope($details), $failure['details']);
            self::assertSame($details, $client->payloadCodec()->decodeEnvelope($failure['details']));
        }
    }

    public static function largeReports(): iterable
    {
        foreach (['message', 'stack', 'combined', 'escaped', 'details', 'lower-limit', 'lower-details', 'small'] as $case) {
            yield $case => [$case];
        }
    }

    public function testWorkflowTaskUsesOnlyPublishedStackCarrier(): void
    {
        $transport = new FakeTransport([[]]);
        $client = new Client('http://recording.invalid', transport: $transport);
        $client->failWorkflowTask('task', 'owner', 7, str_repeat('m', 3 * 1024 * 1024),
            'OriginalFailure', 'original-reason', 9, str_repeat('s', 3 * 1024 * 1024));
        $body = $transport->requests[0]['body'];
        self::assertSame('owner', $body['lease_owner']);
        self::assertSame(7, $body['workflow_task_attempt']);
        $failure = $body['failure'];
        self::assertSame('OriginalFailure', $failure['type']);
        self::assertSame('original-reason', $failure['reason']);
        self::assertSame(9, $failure['sequence']);
        self::assertArrayNotHasKey('diagnostics', $failure);
        self::assertStringContainsString('[truncation diagnostics]', $failure['stack_trace']);
        self::assertStringContainsString('3145728', $failure['stack_trace']);
        self::assertLessThanOrEqual(131072, strlen($failure['stack_trace']));
        self::assertLessThanOrEqual(1048576, JsonDocument::size($body));
    }

    public function testCompletionBoundsNestedExceptionWithoutChangingClassification(): void
    {
        $transport = new FakeTransport([[]]);
        $client = new Client('http://recording.invalid', transport: $transport);
        $large = str_repeat('x', 3 * 1024 * 1024);
        $command = ['type' => 'fail_workflow', 'message' => 'outer', 'exception_type' => 'OriginalFailure',
            'non_retryable' => false, 'failed_step_sequence' => 8, 'failed_activity_execution_id' => 'activity',
            'exception' => ['message' => $large, 'properties' => ['previous' => ['message' => $large]], 'stack_trace' => $large]];
        $client->completeWorkflowTask('task', 'owner', 4, [$command]);
        $body = $transport->requests[0]['body'];
        $bounded = $body['commands'][0];
        foreach (['type', 'exception_type', 'non_retryable', 'failed_step_sequence', 'failed_activity_execution_id'] as $key) {
            self::assertSame($command[$key], $bounded[$key]);
        }
        self::assertArrayNotHasKey('class', $bounded['exception']);
        self::assertArrayNotHasKey('type', $bounded['exception']);
        self::assertArrayNotHasKey('properties', $bounded['exception']);
        self::assertStringContainsString('[truncated failure report;', $bounded['message']);
        self::assertStringContainsString('[truncated failure report;', $bounded['exception']['message']);
        self::assertTrue($bounded['exception']['diagnostics']['truncation']['fields']['details']['omitted']);
        self::assertSame('owner', $body['lease_owner']);
        self::assertSame(4, $body['workflow_task_attempt']);
    }

    public function testLowerConfiguredLimitCountsEscapingAfterAllMarkers(): void
    {
        $transport = new FakeTransport([[]]);
        $client = (new Client('http://recording.invalid', transport: $transport))->withFailureReportLimit(4096);
        $client->failActivityTask('task', 'attempt', 'owner', str_repeat("\0\"\\雪", 600000), 'OriginalFailure', false,
            ['oversized' => str_repeat('d', 100000)], str_repeat("\0", 3000000));
        $body = $transport->requests[0]['body'];
        self::assertLessThanOrEqual(4096, JsonDocument::size($body));
        self::assertFalse($body['failure']['non_retryable']);
        self::assertStringContainsString('[truncated failure report;', $body['failure']['message']);
        self::assertArrayNotHasKey('details', $body['failure']);
    }

    public function testSmallTransportBudgetRetainsMarkersInsteadOfAnArbitraryContentFloor(): void
    {
        foreach (['activity', 'workflow'] as $kind) {
            $transport = new FakeTransport([[]]);
            $client = (new Client('http://recording.invalid', transport: $transport))->withFailureReportLimit(512);
            $large = str_repeat('m', 3 * 1024 * 1024);
            if ($kind === 'activity') {
                $client->failActivityTask('task', 'attempt', 'owner', $large, stackTrace: $large);
            } else {
                $client->failWorkflowTask('task', 'owner', 1, $large, stackTrace: $large);
            }
            self::assertCount(1, $transport->requests);
            self::assertLessThanOrEqual(512, JsonDocument::size($transport->requests[0]['body']));
            self::assertStringContainsString('[truncated failure report;', $transport->requests[0]['body']['failure']['message']);
        }
    }

    public function testKnownTransportLimitIsRespectedAndImpossibleIdentityRefusesBeforeSend(): void
    {
        $transport = new class implements Transport, RequestSizeLimitTransport {
            public array $requests = [];
            public function maximumRequestBytes(): ?int { return 4096; }
            public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
            {
                $this->requests[] = $body;
                return [];
            }
        };
        $client = new Client('http://recording.invalid', transport: $transport);
        $client->failActivityTask('task', 'attempt', 'owner', str_repeat('x', 3000000));
        self::assertLessThanOrEqual(4096, JsonDocument::size($transport->requests[0]));
        try {
            $client->failActivityTask('task', str_repeat('identity', 1000), 'owner', 'small', 'OriginalFailure');
            self::fail('The fixed identity cannot fit.');
        } catch (FailureReportRefused $error) {
            self::assertSame('failure_report_refused', $error->response['reason']);
            self::assertFalse($error->transientConnectionFailure);
        }
        self::assertCount(1, $transport->requests);
    }

    public function testAggregateCompletionBudgetIncludesEveryCommandAndFixedBusinessContent(): void
    {
        $limiter = new FailureReportLimiter(new FailureReportLimits(), new FakeTransport());
        $command = ['type' => 'fail_workflow', 'message' => str_repeat("\0", 3000000), 'exception_type' => 'Original'];
        $body = ['lease_owner' => 'owner', 'workflow_task_attempt' => 3, 'commands' => array_fill(0, 8, $command)];
        $result = $limiter->request('/worker/workflow-tasks/task/complete', $body);
        self::assertLessThanOrEqual(1048576, JsonDocument::size($result));
        self::assertCount(8, $result['commands']);
        $body['commands'][] = ['type' => 'record_marker', 'name' => 'business', 'details' => str_repeat('x', 1100000)];
        $this->expectException(FailureReportRefused::class);
        $limiter->request('/worker/workflow-tasks/task/complete', $body);
    }

    public function testSuccessfulLocalRecordKeepsItsOrdinaryBusinessPayloadBudget(): void
    {
        $transport = new FakeTransport([[]]);
        $client = new Client('http://recording.invalid', transport: $transport);
        $result = $client->payloadCodec()->envelope(str_repeat('p', 900000));
        $command = ['type' => 'record_local_activity', 'activity_type' => 'business', 'outcome' => 'completed',
            'result' => $result, 'attempts' => [['attempt_number' => 1, 'outcome' => 'completed', 'heartbeats' => []]]];
        $client->completeWorkflowTask('task', 'owner', 1, [$command]);
        self::assertCount(1, $transport->requests);
        self::assertGreaterThan(1048576, JsonDocument::size($transport->requests[0]['body']));
        self::assertSame($result, $transport->requests[0]['body']['commands'][0]['result']);
    }

    public function testInvalidUtf8MessageRepairsBoundedPrefixWithoutChangingType(): void
    {
        $transport = new FakeTransport([[]]);
        $client = new Client('http://recording.invalid', transport: $transport);
        $client->failActivityTask('task', 'attempt', 'owner', "broken\xff message", 'OriginalType');
        $failure = $transport->requests[0]['body']['failure'];
        self::assertSame('OriginalType', $failure['type']);
        self::assertSame(1, preg_match('//u', $failure['message']));
        self::assertStringContainsString('[truncated failure report;', $failure['message']);
    }
}
