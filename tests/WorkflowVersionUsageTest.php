<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerCapabilityUnavailable;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Metrics\MetricsRoutes;
use DurableWorkflow\Model\ClusterInfo;
use DurableWorkflow\Model\WorkflowVersionUsagePage;
use DurableWorkflow\Tests\Support\FakeTransport;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use UnexpectedValueException;

final class WorkflowVersionUsageTest extends TestCase
{
    private const CAPABILITY = ['capabilities' => ['workflow_version_usage' => true]];

    /** @return array<string, mixed> */
    private static function page(): array
    {
        return [
            'schema_version' => 1, 'namespace' => 'orders', 'workflow_type' => 'shipment',
            'change_id' => 'carrier-api', 'min_version' => 2, 'state' => null, 'consistency' => 'live',
            'through_run_id' => 'run-3', 'counts' => ['total' => 3, 'legacy' => 1, 'current' => 1, 'unknown' => 1],
            'runs' => [
                ['instance_id' => 'shipment-1', 'run_id' => 'run-1', 'status' => 'waiting',
                    'state' => 'legacy', 'recorded_version' => 1, 'unknown_reason' => null],
                ['instance_id' => 'shipment-2', 'run_id' => 'run-2', 'status' => 'running',
                    'state' => 'current', 'recorded_version' => 2, 'unknown_reason' => null],
                ['instance_id' => 'shipment-3', 'run_id' => 'run-3', 'status' => 'pending',
                    'state' => 'unknown', 'recorded_version' => null, 'unknown_reason' => 'marker_absent'],
            ],
            'limit' => 50, 'returned_count' => 3, 'has_more' => false, 'next_cursor' => null,
        ];
    }

    public function testTypedMetadataUsesExactQueryNamespaceAndControlCredential(): void
    {
        $transport = new FakeTransport([self::CAPABILITY, self::page()]);
        $client = new Client('https://runtime.example', namespace: 'orders', controlToken: 'control', workerToken: 'worker', transport: $transport);
        $page = $client->workflowVersionUsage('shipment', 'carrier-api', 2);

        self::assertInstanceOf(WorkflowVersionUsagePage::class, $page);
        self::assertSame(3, $page->counts->total);
        self::assertSame([1, 2, null], array_map(static fn ($run) => $run->recordedVersion, $page->runs));
        self::assertSame('marker_absent', $page->runs[2]->unknownReason);
        self::assertNull($page->nextCursor);
        self::assertSame(self::page(), $page->raw);
        self::assertCount(2, $transport->requests);
        foreach ($transport->requests as $request) {
            self::assertSame('GET', $request['method']);
            self::assertNull($request['body']);
            self::assertSame('orders', $request['headers']['X-Namespace']);
            self::assertSame('Bearer control', $request['headers']['Authorization']);
            self::assertArrayHasKey('X-Durable-Workflow-Control-Plane-Version', $request['headers']);
            self::assertArrayNotHasKey('X-Durable-Workflow-Protocol-Version', $request['headers']);
        }
        self::assertStringEndsWith('/api/workflow-version-usage?workflow_type=shipment&change_id=carrier-api&min_version=2&limit=50', $transport->requests[1]['uri']);
        self::assertSame('/workflow-version-usage', MetricsRoutes::template('/workflow-version-usage?change_id=private&cursor=secret'));
    }

    public function testOpaqueLongCursorAndExactStringsArePassedWithoutInspection(): void
    {
        $value = self::page();
        $value['workflow_type'] = " shipment/雪 ";
        $value['change_id'] = 'carrier?name=one&two';
        $value['state'] = 'unknown';
        $value['through_run_id'] = 'run-9';
        $value['counts'] = ['total' => 4, 'legacy' => 1, 'current' => 1, 'unknown' => 2];
        $value['runs'] = [$value['runs'][2]];
        $value['returned_count'] = 1;
        $value['limit'] = 1;
        $value['has_more'] = true;
        $value['next_cursor'] = 'opaque-next/+=';
        $cursor = str_repeat('x', 9000).'+/=?&opaque';
        $transport = new FakeTransport([self::CAPABILITY, $value]);
        $page = (new Client('https://runtime.example', namespace: 'orders', transport: $transport))
            ->workflowVersionUsage($value['workflow_type'], $value['change_id'], 2, 'unknown', 1, $cursor);
        parse_str(parse_url($transport->requests[1]['uri'], PHP_URL_QUERY), $query);
        self::assertSame($cursor, $query['cursor']);
        self::assertSame($value['workflow_type'], $query['workflow_type']);
        self::assertSame($value['change_id'], $query['change_id']);
        self::assertSame('opaque-next/+=', $page->nextCursor);
        self::assertSame(4, $page->counts->total, 'Counts precede the state filter.');
        self::assertSame(2, $page->counts->unknown);
    }

    public function testSignedIntegerExtremesRemainExact(): void
    {
        foreach ([-2147483648, 2147483647] as $minimum) {
            $value = self::page();
            $value['min_version'] = $minimum;
            $value['runs'] = [$value['runs'][1]];
            $value['runs'][0]['recorded_version'] = $minimum;
            $value['returned_count'] = 1;
            $value['counts'] = ['total' => 1, 'legacy' => 0, 'current' => 1, 'unknown' => 0];
            $value['through_run_id'] = 'run-2';
            $transport = new FakeTransport([self::CAPABILITY, $value]);
            $page = (new Client('https://runtime.example', namespace: 'orders', transport: $transport))
                ->workflowVersionUsage('shipment', 'carrier-api', $minimum);
            self::assertSame($minimum, $page->minVersion);
            self::assertSame($minimum, $page->runs[0]->recordedVersion);
        }
    }

    public function testEachUnknownReasonIsPreservedWithoutGuessingFromDefinition(): void
    {
        foreach (['marker_absent', 'history_unavailable', 'history_invalid'] as $reason) {
            $value = self::page();
            $value['runs'][2]['unknown_reason'] = $reason;
            $page = WorkflowVersionUsagePage::fromArray($value);
            self::assertSame($reason, $page->runs[2]->unknownReason);
            self::assertNull($page->runs[2]->recordedVersion);
        }
    }

    #[DataProvider('invalidQueries')]
    public function testInvalidQueriesFailBeforeDiscovery(array $arguments): void
    {
        $transport = new FakeTransport();
        try {
            (new Client('https://runtime.example', transport: $transport))->workflowVersionUsage(...$arguments);
            self::fail('Invalid query must be refused before a request.');
        } catch (InvalidArgumentException) {
            self::assertSame([], $transport->requests);
        }
    }

    public static function invalidQueries(): iterable
    {
        foreach ([['workflowType' => ' '], ['workflowType' => "\xFF"], ['workflowType' => str_repeat('雪', 86)],
            ['changeId' => "\u{2003}"], ['changeId' => str_repeat('x', 1025)], ['minVersion' => -2147483649],
            ['minVersion' => 2147483648], ['state' => 'all'], ['limit' => 0], ['limit' => 201],
            ['cursor' => ''], ['cursor' => "\xFF"]] as $change) {
            yield [array_replace(['workflowType' => 'shipment', 'changeId' => 'carrier-api', 'minVersion' => 2], $change)];
        }
    }

    public function testMissingOrFalseCapabilitiesHaveAnExplicitUnsupportedError(): void
    {
        foreach ([[], ['workflow_version_usage' => false], ['workflow_version_usage' => 'true']] as $capabilities) {
            self::assertFalse(ClusterInfo::fromArray(['capabilities' => $capabilities])->supportsWorkflowVersionUsage());
            $transport = new FakeTransport([['capabilities' => $capabilities]]);
            try {
                (new Client('https://runtime.example', transport: $transport))->workflowVersionUsage('shipment', 'carrier-api', 2);
                self::fail('No version-usage read without the capability.');
            } catch (ServerCapabilityUnavailable $error) {
                self::assertSame('workflow_version_usage', $error->capability);
                self::assertSame(501, $error->status);
                self::assertCount(1, $transport->requests);
            }
        }
    }

    public function testWorkerOnlyCredentialsCannotAuthorizeTheControlRead(): void
    {
        $transport = new FakeTransport();
        try {
            (new Client('https://runtime.example', workerToken: 'worker-only', transport: $transport))
                ->workflowVersionUsage('shipment', 'carrier-api', 2);
            self::fail('A worker credential cannot be used as a control credential.');
        } catch (InvalidArgumentException $error) {
            self::assertStringContainsString('control credential', $error->getMessage());
            self::assertSame([], $transport->requests);
        }
    }

    public function testAuthenticationValidationStorageAndTimeoutErrorsRemainErrors(): void
    {
        foreach ([401, 403, 404, 422, 503] as $status) {
            foreach ([true, false] as $discovery) {
                $error = TransportException::fromResponse($status, ['reason' => 'retained-refusal'], '');
                $transport = new FakeTransport($discovery ? [$error] : [self::CAPABILITY, $error]);
                try {
                    (new Client('https://runtime.example', transport: $transport))->workflowVersionUsage('shipment', 'carrier-api', 2);
                    self::fail('Server refusal cannot become an empty page.');
                } catch (ServerException $failure) {
                    self::assertSame($status, $failure->status);
                    self::assertSame('retained-refusal', $failure->reason);
                    self::assertCount($discovery ? 1 : 2, $transport->requests);
                }
            }
        }
        $transport = new FakeTransport([self::CAPABILITY, new TransportException('timeout')]);
        $this->expectException(ServerException::class);
        (new Client('https://runtime.example', transport: $transport))->workflowVersionUsage('shipment', 'carrier-api', 2);
    }

    #[DataProvider('invalidResponses')]
    public function testMalformedOrForeignResponsesCannotBecomeAnEmptySuccess(string $mutation): void
    {
        $value = self::page();
        match ($mutation) {
            'schema' => $value['schema_version'] = 2,
            'snapshot' => $value['consistency'] = 'snapshot',
            'count' => $value['counts']['total'] = 4,
            'bool-count' => $value['counts']['legacy'] = true,
            'unknown-version' => $value['runs'][2]['recorded_version'] = 2,
            'unknown-reason' => $value['runs'][2]['unknown_reason'] = null,
            'classification' => $value['runs'][0]['recorded_version'] = 3,
            'duplicate' => $value['runs'][1]['run_id'] = 'run-1',
            'upper-bound' => $value['through_run_id'] = 'run-2',
            'returned-count' => $value['returned_count'] = 2,
            'cursor' => $value['next_cursor'] = 'orphaned',
            'closed' => $value['runs'][0]['status'] = 'completed',
            'namespace' => $value['namespace'] = 'foreign',
            'type' => $value['workflow_type'] = 'other',
            'change' => $value['change_id'] = 'other',
            'minimum' => $value['min_version'] = 3,
            'limit' => $value['limit'] = 49,
            'state' => $value['state'] = 'legacy',
            'safe-to-remove' => $value['safe_to_remove'] = true,
            'state-counts' => $value['counts'] = ['total' => 3, 'legacy' => 0, 'current' => 2, 'unknown' => 1],
            'payload' => $value['result_envelope'] = self::unexpectedPayload(),
        };
        $transport = new FakeTransport([self::CAPABILITY, $value]);
        $this->expectException(UnexpectedValueException::class);
        (new Client('https://runtime.example', namespace: 'orders', transport: $transport))->workflowVersionUsage('shipment', 'carrier-api', 2);
    }

    public static function invalidResponses(): iterable
    {
        foreach (['schema', 'snapshot', 'count', 'bool-count', 'unknown-version', 'unknown-reason', 'classification',
            'duplicate', 'upper-bound', 'returned-count', 'cursor', 'closed', 'namespace', 'type', 'change',
            'minimum', 'limit', 'state', 'safe-to-remove', 'state-counts', 'payload'] as $mutation) {
            yield $mutation => [$mutation];
        }
    }

    public function testEmptyCohortIsDifferentFromUnknownHistory(): void
    {
        $value = self::page();
        $value['counts'] = ['total' => 0, 'legacy' => 0, 'current' => 0, 'unknown' => 0];
        $value['runs'] = [];
        $value['returned_count'] = 0;
        $value['through_run_id'] = null;
        $page = WorkflowVersionUsagePage::fromArray($value);
        self::assertSame([], $page->runs);
        self::assertSame(0, $page->counts->total);
    }

    public function testDiscoveryDoesNotHydrateApplicationPayloadFields(): void
    {
        $transport = new FakeTransport([self::CAPABILITY + ['result_envelope' => self::unexpectedPayload()], self::page()]);
        $page = (new Client('https://runtime.example', namespace: 'orders', transport: $transport))
            ->workflowVersionUsage('shipment', 'carrier-api', 2);
        self::assertSame(3, $page->counts->total);
        self::assertCount(2, $transport->requests);
    }

    #[DataProvider('invalidPageProgress')]
    public function testPageProgressRequiresRequestContext(string $mutation): void
    {
        $value = self::page();
        $state = $mutation === 'filtered-first-terminal' ? 'legacy' : null;
        $cursor = $mutation === 'repeated-cursor' ? 'unchanged-opaque-cursor' : null;
        $value['state'] = $state;
        $value['runs'] = [$value['runs'][0]];
        $value['returned_count'] = 1;
        if ($mutation === 'filtered-first-terminal') {
            $value['runs'] = [];
            $value['returned_count'] = 0;
        }
        if (in_array($mutation, ['short-has-more', 'repeated-cursor'], true)) {
            $value['has_more'] = true;
            $value['next_cursor'] = $cursor ?? 'next-opaque-cursor';
        }
        if ($mutation === 'repeated-cursor') {
            $value['limit'] = 1;
        }
        $transport = new FakeTransport([self::CAPABILITY, $value]);
        $this->expectException(UnexpectedValueException::class);
        (new Client('https://runtime.example', namespace: 'orders', transport: $transport))
            ->workflowVersionUsage('shipment', 'carrier-api', 2, state: $state, limit: $value['limit'], cursor: $cursor);
    }

    public static function invalidPageProgress(): iterable
    {
        foreach (['unfiltered-first-terminal', 'filtered-first-terminal', 'short-has-more', 'repeated-cursor'] as $mutation) {
            yield $mutation => [$mutation];
        }
    }

    public function testLaterTerminalCursorPageCanBeEmptyWithRemainingCohortCounts(): void
    {
        $value = self::page();
        $value['runs'] = [];
        $value['returned_count'] = 0;
        $transport = new FakeTransport([self::CAPABILITY, $value]);
        $page = (new Client('https://runtime.example', namespace: 'orders', transport: $transport))
            ->workflowVersionUsage('shipment', 'carrier-api', 2, cursor: 'earlier-opaque-cursor');
        self::assertSame(3, $page->counts->total);
        self::assertSame([], $page->runs);
        self::assertFalse($page->hasMore);
        self::assertNull($page->nextCursor);
    }

    /** @return array<string, mixed> */
    private static function unexpectedPayload(): array
    {
        return ['codec' => 'avro', 'external_payload' => [
            'schema' => 'durable-workflow.v2.runtime-external-payload-reference.v1',
            'reference_id' => 'ep_00000000000000000000000000', 'codec' => 'avro',
            'size_bytes' => 1, 'sha256' => str_repeat('a', 64),
        ]];
    }
}
