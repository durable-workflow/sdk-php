<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Qualification\CompletionPollTrace;
use DurableWorkflow\Tests\Support\CompletionPipelineScenario;
use DurableWorkflow\Transport\Psr18Transport;
use PHPUnit\Framework\TestCase;

require_once __DIR__.'/../scripts/conformance/Support/CompletionPollTrace.php';

final class CompletionPollConsumerRelayTest extends TestCase
{
    public function testTheRelayForwardsActualRoleAndBodyWithoutRecordingThem(): void
    {
        $this->withRelay(static function (string $relay, CompletionPollTrace $trace): void {
            $transport = new Psr18Transport();
            $answer = $transport->send('POST', $relay.'/api/worker/workflow-tasks/poll',
                ['Content-Type' => 'application/json', 'Authorization' => 'Bearer synthetic-relay-role'],
                ['worker_id' => 'fixture-worker', 'poll_request_id' => 'fixture-poll', 'timeout_seconds' => 0,
                    'fixture_payload' => 'synthetic-payload-not-retained']);
            self::assertSame('empty', $answer['poll_status']);
            $events = $trace->state()['events'];
            self::assertSame(['request', 'upstream', 'delivered'], array_column($events, 'kind'));
            self::assertSame($answer['fixture_wire_sha256'], $events[0]['body']['body_sha256']);
            self::assertSame(200, $events[1]['status']);
            self::assertStringNotContainsString('synthetic-relay-role', json_encode($events, JSON_THROW_ON_ERROR));
            self::assertStringNotContainsString('synthetic-payload-not-retained', json_encode($events, JSON_THROW_ON_ERROR));
        });
    }

    public function testAnActualUpstreamFailureIsNotTurnedIntoAnEmptyOrSuccessAnswer(): void
    {
        $this->withRelay(static function (string $relay, CompletionPollTrace $trace): void {
            try {
                (new Psr18Transport())->send('POST', $relay.'/api/worker/workflow-tasks/poll',
                    ['Content-Type' => 'application/json', 'Authorization' => 'Bearer synthetic-relay-role'],
                    ['fixture_failure' => true]);
                self::fail('The original upstream error must propagate.');
            } catch (TransportException $failure) {
                self::assertSame(500, $failure->status);
                self::assertSame('fixture_original_upstream_failure', $failure->response['reason']);
            }
            $events = $trace->state()['events'];
            self::assertSame(500, $events[1]['status']);
            self::assertSame(500, $events[2]['status']);
        });
    }

    public function testTheSelectedLossWithholdsOnlyOneActuallySuccessfulUpstreamReply(): void
    {
        $this->withRelay(static function (string $relay, CompletionPollTrace $trace): void {
            self::lossState($trace);
            $transport = new Psr18Transport();
            $headers = ['Content-Type' => 'application/json', 'Authorization' => 'Bearer synthetic-relay-role'];
            // A real upstream fault is preserved and does not consume the selected successful-answer loss.
            try {
                $transport->send('POST', $relay.'/api/worker/workflow-tasks/parent/complete', $headers, ['fixture_failure' => true]);
                self::fail('The upstream fault must remain a fault.');
            } catch (TransportException $failure) {
                self::assertSame(500, $failure->status);
            }
            try {
                $transport->send('POST', $relay.'/api/worker/workflow-tasks/parent/complete', $headers, ['fixture_result' => 2]);
                self::fail('The first successful upstream answer is deliberately withheld.');
            } catch (TransportException $failure) {
                self::assertSame(502, $failure->status);
                self::assertNull($failure->response, 'The deliberate loss is an unstructured upstream failure.');
            }
            $answer = $transport->send('POST', $relay.'/api/worker/workflow-tasks/parent/complete', $headers, ['fixture_result' => 2]);
            self::assertSame('empty', $answer['poll_status']);
            $events = $trace->state()['events'];
            $upstream = array_filter($events, static fn (array $event): bool => $event['kind'] === 'upstream' && $event['request'] !== 'seed');
            $delivered = array_filter($events, static fn (array $event): bool => $event['kind'] === 'delivered');
            self::assertSame([500, 200, 200], array_values(array_column($upstream, 'status')));
            self::assertSame([500, 502, 200], array_values(array_column($delivered, 'status')));
        });
    }

    private static function lossState(CompletionPollTrace $trace): void
    {
        $trace->change(static function (array &$state): void {
            $state['scenario'] = 'pipeline-loss';
            $state['release_parent'] = true;
            // Fixed lookup metadata for the relay control, not a Server lease or consumer acceptance result.
            $state['events'][] = ['kind' => 'upstream', 'request' => 'seed',
                'answer' => ['task' => ['task_id' => 'parent', 'workflow_id' => 'fixture-2']]];
        });
    }

    private function withRelay(callable $check): void
    {
        CompletionPipelineScenario::run(static function (string $upstream) use ($check): void {
            CompletionPipelineScenario::run(static function (string $relay, string $file) use ($upstream, $check): void {
                file_put_contents($file, json_encode(['events' => [], 'endpoint' => $upstream,
                    'scenario' => 'carriers', 'prefix' => 'fixture'], JSON_THROW_ON_ERROR));
                $check($relay, new CompletionPollTrace($file));
            }, __DIR__.'/../scripts/conformance/Support/completion-poll-relay.php', true);
        }, __DIR__.'/Support/completion-consumer-upstream-fixture.php');
    }
}
