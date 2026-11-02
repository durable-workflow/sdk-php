<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Metrics\MetricsRoutes;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Tests\Support\RecordingMetricsRecorder;
use DurableWorkflow\Worker;
use PHPUnit\Framework\TestCase;

/**
 * Metric labels must stay bounded: identifiers, names and other caller- or
 * Server-generated values can never become label values, however many there are.
 */
final class MetricsLabelCardinalityTest extends TestCase
{
    public function testSharedRoutesMatchThePythonSdkInOrder(): void
    {
        $python = json_decode((string) file_get_contents(__DIR__.'/Support/python-metric-routes.json'), true, flags: JSON_THROW_ON_ERROR);
        self::assertSame($python['routes'], array_slice(MetricsRoutes::routes(), 0, count($python['routes'])));
        self::assertSame(count(MetricsRoutes::routes()), count(array_unique(MetricsRoutes::routes())));
        self::assertNotContains(MetricsRoutes::UNKNOWN, MetricsRoutes::routes());
    }

    public function testEveryReviewedTemplateMatchesItselfWithConcreteValues(): void
    {
        foreach (MetricsRoutes::routes() as $route) {
            $concrete = preg_replace('/\{[a-z_]+\}/', 'value-'.bin2hex(random_bytes(4)), $route);
            self::assertSame($route, MetricsRoutes::template((string) $concrete.'?secret=1#fragment'));
        }
    }

    public function testRandomPathsOnlyYieldReviewedTemplatesOrUnknown(): void
    {
        $allowed = [...MetricsRoutes::routes(), MetricsRoutes::UNKNOWN];
        $segments = ['workflows', 'runs', 'worker', 'sessions', 'poll', 'signal', 'schedules', 'api', ''];
        for ($i = 0; $i < 500; ++$i) {
            $token = 'tok'.bin2hex(random_bytes(6));
            $parts = [];
            for ($n = random_int(0, 7); $n > 0; --$n) {
                $parts[] = random_int(0, 1) === 0 ? $segments[array_rand($segments)] : $token;
            }
            $label = MetricsRoutes::template('/'.implode('/', $parts).(random_int(0, 1) === 0 ? '?q='.$token : ''));
            self::assertContains($label, $allowed);
            self::assertStringNotContainsString($token, $label);
        }
    }

    public function testDistinctLabelValuesDoNotGrowWithDistinctIdentifiers(): void
    {
        [$few, $fewIds] = $this->exerciseClient(3);
        [$many, $manyIds] = $this->exerciseClient(40);

        self::assertNotEmpty($few);
        self::assertSame($few, $many, 'Label value sets must not depend on how many distinct identifiers were used.');
        $labels = json_encode($many, JSON_THROW_ON_ERROR);
        foreach ([...$fewIds, ...$manyIds] as $id) {
            self::assertStringNotContainsString($id, $labels);
        }
        foreach ($many['route'] ?? [] as $route) {
            self::assertContains($route, [...MetricsRoutes::routes(), MetricsRoutes::UNKNOWN]);
        }
    }

    public function testWorkerPollLabelsStayBoundedAcrossWorkerIds(): void
    {
        $values = [];
        $afterFirst = null;
        foreach (range(1, 25) as $n) {
            $recorder = new RecordingMetricsRecorder();
            $empty = ['task' => null, 'poll_status' => 'empty'];
            $client = new Client('https://server.invalid', transport: new FakeTransport(array_fill(0, 6, $empty)), metrics: $recorder);
            $worker = new Worker($client, 'bounded-queue', workerId: 'worker-'.$n.'-'.bin2hex(random_bytes(6)));
            self::assertFalse($worker->tick(0));
            foreach ($recorder->events as $event) {
                foreach ($event['tags'] as $key => $value) {
                    $values[$key][$value] = true;
                }
                self::assertStringNotContainsString('worker-'.$n.'-', json_encode($event['tags'], JSON_THROW_ON_ERROR));
            }
            $afterFirst ??= $values;
        }
        // 25 distinct worker IDs add no label values beyond the first worker's.
        self::assertSame($afterFirst, $values);
        self::assertSame(['bounded-queue'], array_keys($values['task_queue']));
        self::assertSame(['workflow', 'activity', 'query'], array_keys($values['task_kind']));
    }

    /**
     * Drive many client and worker operations whose paths carry identifiers.
     *
     * @return array{array<string, list<string>>, list<string>} distinct values per tag key, and the identifiers used
     */
    private function exerciseClient(int $distinct): array
    {
        $recorder = new RecordingMetricsRecorder();
        $client = new Client('https://server.invalid', token: 'secret-token',
            transport: new FakeTransport(array_fill(0, $distinct * 20, [])), metrics: $recorder);
        $ids = [];
        for ($i = 0; $i < $distinct; ++$i) {
            // Identifiers that look like routes, queries and Unicode must still collapse to templates.
            $id = 'id'.$i.'-'.bin2hex(random_bytes(5)).['', '/poll', '?x=1', '#frag', '-é'][$i % 5];
            $ids[] = explode('/', explode('?', explode('#', $id)[0])[0])[0];
            $calls = [
                static fn () => $client->describeWorkflow($id, $id),
                static fn () => $client->workflowHistory($id, $id, 10),
                static fn () => $client->signalWorkflow($id, 'signal-'.$id),
                static fn () => $client->queryWorkflow($id, 'query-'.$id),
                static fn () => $client->cancelWorkflow($id),
                static fn () => $client->terminateWorkflow($id, runId: $id),
                static fn () => $client->registerWorker($id, 'bounded-queue', ['T'], []),
                static fn () => $client->heartbeatActivityTask($id, $id, $id),
                static fn () => $client->failWorkflowTask($id, $id, 1, 'failure'),
                static fn () => $client->closeWorkerSession($id, $id),
                static fn () => $client->deregisterWorkerRegistration($id),
            ];
            foreach ($calls as $call) {
                try {
                    $call();
                } catch (\Throwable) {
                    // Response validation may refuse the fake reply; the request was still observed.
                }
            }
        }
        $values = [];
        foreach ($recorder->events as $event) {
            foreach ($event['tags'] as $key => $value) {
                $values[$key][$value] = true;
            }
        }
        ksort($values);

        return [array_map(static function (array $set): array {
            $keys = array_map('strval', array_keys($set));
            sort($keys);

            return $keys;
        }, $values), $ids];
    }
}
