<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\CompletionPipelineScenario;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Transport\RequestBodySent;
use Fiber;
use GuzzleHttp\Client as HttpClient;
use GuzzleHttp\Promise\FulfilledPromise;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;

final class Psr18TransportSuspensionTest extends TestCase
{
    public function testOwnedRequestYieldsAfterTheWholeBodyAndReadsItsAnswerOnResumeOnlyOnce(): void
    {
        CompletionPipelineScenario::run(static function (string $uri, string $state): void {
            $transport = new Psr18Transport();
            $body = ['message' => str_repeat('x', 65536), 'hold' => true];
            $fiber = new Fiber(static function () use ($transport, $uri, $body): array {
                $first = $transport->send('POST', $uri.'/complete', [], $body);
                $second = $transport->send('POST', $uri.'/complete', [], ['message' => 'second']);
                return [$first, $second];
            });
            self::assertTrue($transport->armCompletionSuspension($fiber, $uri.'/complete'));
            self::assertInstanceOf(RequestBodySent::class, $fiber->start());
            self::assertTrue($fiber->isSuspended());
            $deadline = microtime(true) + 1;
            do {
                $events = CompletionPipelineScenario::events($state);
                if ($events !== []) break;
                usleep(1000);
            } while (microtime(true) < $deadline);
            self::assertSame($body, $events[0]['body']);
            self::assertGreaterThan(65536, $events[0]['bytes']);
            self::assertSame('request', $events[0]['kind']);
            self::assertCount(1, $events, 'The response is still pending while useful work can run.');
            CompletionPipelineScenario::allowAnswer($state);
            self::assertNull($fiber->resume());
            self::assertTrue($fiber->isTerminated(), 'Only the first request in the armed Fiber can suspend.');
            self::assertSame([['accepted' => true], ['accepted' => true]], $fiber->getReturn());
            self::assertSame(['request', 'answer', 'request', 'answer'], array_column(CompletionPipelineScenario::events($state), 'kind'));
        });
    }

    public function testAnUnarmedFiberUsesTheOriginalSynchronousPath(): void
    {
        CompletionPipelineScenario::run(static function (string $uri, string $state): void {
            $transport = new Psr18Transport();
            $fiber = new Fiber(static fn (): ?array => $transport->send('POST', $uri.'/complete', [], ['value' => 1]));
            self::assertNull($fiber->start());
            self::assertTrue($fiber->isTerminated());
            self::assertSame(['accepted' => true], $fiber->getReturn());
            self::assertSame(['request', 'answer'], array_column(CompletionPipelineScenario::events($state), 'kind'));
        });
    }

    public function testAnotherFiberCannotConsumeTheOwnersArm(): void
    {
        CompletionPipelineScenario::run(static function (string $uri, string $state): void {
            $transport = new Psr18Transport();
            $owner = new Fiber(static fn (): ?array => $transport->send('POST', $uri.'/complete', [], ['owner' => true, 'hold' => true]));
            $other = new Fiber(static fn (): ?array => $transport->send('POST', $uri.'/complete', [], ['owner' => false]));
            self::assertTrue($transport->armCompletionSuspension($owner, $uri.'/complete'));
            self::assertFalse($transport->armCompletionSuspension($other, $uri.'/complete'));
            self::assertNull($other->start());
            self::assertTrue($other->isTerminated());
            self::assertInstanceOf(RequestBodySent::class, $owner->start());
            CompletionPipelineScenario::allowAnswer($state);
            $owner->resume();
            self::assertTrue($owner->isTerminated());
        });
    }

    public function testAnEarlierIneligibleRequestConsumesTheArmWithoutSuspendingALaterRequest(): void
    {
        CompletionPipelineScenario::run(static function (string $uri, string $state): void {
            $transport = new Psr18Transport();
            $fiber = new Fiber(static function () use ($transport, $uri): void {
                $transport->send('POST', $uri.'/preflight', [], ['preflight' => true]);
                $transport->send('POST', $uri.'/complete', [], ['value' => 1]);
            });
            self::assertTrue($transport->armCompletionSuspension($fiber, $uri.'/complete'));
            self::assertNull($fiber->start());
            self::assertTrue($fiber->isTerminated());
            self::assertSame(['request', 'answer', 'request', 'answer'], array_column(CompletionPipelineScenario::events($state), 'kind'));
        });
    }

    public function testPayloadProgressNeverSuspendsOrArmsTheFollowingCompletion(): void
    {
        CompletionPipelineScenario::run(static function (string $uri, string $state): void {
            $transport = new Psr18Transport();
            $progress = 0;
            $fiber = new Fiber(static function () use ($transport, $uri, &$progress): void {
                self::assertSame('fixture', $transport->fetchPayloadWithProgress($uri.'/payload', [
                    'X-Durable-Workflow-Payload-Codec' => 'avro', 'X-Durable-Workflow-Payload-Size' => '7',
                    'X-Durable-Workflow-Payload-SHA256' => hash('sha256', 'fixture'),
                ], 7,
                    static function () use (&$progress): void { ++$progress; }));
                $transport->send('POST', $uri.'/complete', [], ['value' => 1]);
            });
            self::assertTrue($transport->armCompletionSuspension($fiber, $uri.'/complete'));
            self::assertNull($fiber->start());
            self::assertTrue($fiber->isTerminated());
            self::assertGreaterThan(0, $progress);
        });
    }

    public function testARefusedResponseIsThrownOnResumeAndReleasesTheOwner(): void
    {
        CompletionPipelineScenario::run(static function (string $uri, string $state): void {
            $transport = new Psr18Transport();
            $fiber = new Fiber(static fn (): ?array => $transport->send('POST', $uri.'/complete', [], ['status' => 503, 'hold' => true]));
            self::assertTrue($transport->armCompletionSuspension($fiber, $uri.'/complete'));
            self::assertInstanceOf(RequestBodySent::class, $fiber->start());
            CompletionPipelineScenario::allowAnswer($state);
            try {
                $fiber->resume();
                self::fail('The original HTTP refusal must remain visible.');
            } catch (TransportException $error) {
                self::assertSame(503, $error->status);
                self::assertSame('fixture_unavailable', $error->response['reason']);
            }
            self::assertTrue($fiber->isTerminated());
            CompletionPipelineScenario::allowAnswer($state, false);
            $next = new Fiber(static fn (): ?array => $transport->send('POST', $uri.'/complete', [], ['status' => 200, 'hold' => true]));
            self::assertTrue($transport->armCompletionSuspension($next, $uri.'/complete'));
            self::assertInstanceOf(RequestBodySent::class, $next->start());
            CompletionPipelineScenario::allowAnswer($state);
            $next->resume();
            self::assertSame(['accepted' => true], $next->getReturn());
        });
    }

    public function testInjectedHttpClientsCannotOptIntoNativeSuspension(): void
    {
        $transport = new Psr18Transport(new HttpClient(['handler' => static fn (): FulfilledPromise =>
            new FulfilledPromise(new Response(200, [], '{"accepted":true}'))]));
        $fiber = new Fiber(static fn (): ?array => $transport->send('POST', 'https://server.example/complete', [], ['value' => 1]));
        self::assertFalse($transport->supportsCompletionSuspension());
        self::assertFalse($transport->armCompletionSuspension($fiber, 'https://server.example/complete'));
        self::assertNull($fiber->start());
        self::assertTrue($fiber->isTerminated());
    }

    public function testOnlyTheOwnerCanReleaseAnArmBeforeTheRequestStarts(): void
    {
        $transport = new Psr18Transport();
        $owner = new Fiber(static fn () => null);
        $other = new Fiber(static fn () => null);
        self::assertTrue($transport->armCompletionSuspension($owner, 'https://server.example/complete'));
        $transport->disarmCompletionSuspension($other);
        self::assertFalse($transport->armCompletionSuspension($other, 'https://server.example/complete'));
        $transport->disarmCompletionSuspension($owner);
        self::assertTrue($transport->armCompletionSuspension($other, 'https://server.example/complete'));
        $transport->disarmCompletionSuspension($other);
    }

    public function testAbandonmentCancelsTheOwnedTransferAndAllowsAnotherOwner(): void
    {
        CompletionPipelineScenario::run(static function (string $uri, string $state): void {
            $transport = new Psr18Transport();
            $owner = new Fiber(static fn (): ?array => $transport->send('POST', $uri.'/complete', [], ['hold' => true]));
            self::assertTrue($transport->armCompletionSuspension($owner, $uri.'/complete'));
            self::assertInstanceOf(RequestBodySent::class, $owner->start());
            $transport->disarmCompletionSuspension(new Fiber(static fn () => null));
            self::assertTrue($owner->isSuspended(), 'Another Fiber cannot cancel the owned transfer.');
            $transport->disarmCompletionSuspension($owner);
            CompletionPipelineScenario::allowAnswer($state);
            try {
                $owner->resume();
                self::fail('An abandoned transfer must not publish a successful answer.');
            } catch (TransportException $error) {
                self::assertStringContainsString('cancel', strtolower($error->getMessage()));
            }
            self::assertTrue($owner->isTerminated());
            $next = new Fiber(static fn (): ?array => $transport->send('POST', $uri.'/complete', [], ['next' => true]));
            self::assertTrue($transport->armCompletionSuspension($next, $uri.'/complete'));
            $yielded = $next->start();
            if ($next->isSuspended()) {
                self::assertInstanceOf(RequestBodySent::class, $yielded);
                $next->resume();
            }
            self::assertSame(['accepted' => true], $next->getReturn());
        });
    }
}
