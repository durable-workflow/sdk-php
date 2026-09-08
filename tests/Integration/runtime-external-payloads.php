<?php

declare(strict_types=1);

use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Attribute\Query;
use DurableWorkflow\Attribute\Signal;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ExternalPayloadException;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;

require getenv('RUNTIME_SDK_AUTOLOAD') ?: dirname(__DIR__, 2).'/vendor/autoload.php';

final class ExternalPayloadWorkflow
{
    #[Workflow('external-payload.proof')]
    public function run(WorkflowContext $context, string $value, bool $wait): string
    {
        $result = $context->activity('external-payload.echo', [$value]);
        if ($wait) {
            $context->waitCondition(fn (): bool => $context->signals('release') !== [], key: 'release');
            if ($context->signals('release')[0] !== [$result]) {
                throw new RuntimeException('Signal payload did not survive transport.');
            }
        }

        return $result;
    }

    #[Query('value')]
    public function value(QueryContext $context): string
    {
        $event = $context->events('ActivityCompleted')[0] ?? null;
        if ($event === null) {
            throw new RuntimeException('Query did not receive completed activity history.');
        }

        return (new AvroPayloadCodec())->decodeEnvelope($event['payload']['result']);
    }

    #[Signal('release')]
    public function release(string $value): void
    {
        // Signals are read from committed workflow history, not a shared handler object.
    }
}

final class ExternalPayloadActivity
{
    #[Activity('external-payload.echo')]
    public function echoValue(ActivityContext $context, string $value): string
    {
        $context->heartbeat(['bytes' => strlen($value)]);

        return $value;
    }
}

final class MaximumPayloadWorkflow
{
    #[Workflow('external-payload.maximum')]
    public function run(WorkflowContext $context): string
    {
        return str_repeat('m', 50331633);
    }
}

$url = getenv('RUNTIME_URL') ?: 'http://server:8080';
if (!in_array(parse_url($url, PHP_URL_HOST), ['server', 'localhost', '127.0.0.1'], true)) {
    throw new RuntimeException('Use an isolated local Server for this destructive restart experiment.');
}
$mode = $argv[1] ?? '';
$credentials = match ($mode) {
    'prepare', 'boundaries' => ['token' => 'external-payload-fixture'],
    'worker' => ['workerToken' => hash('sha256', 'external-proof-worker')],
    default => ['controlToken' => hash('sha256', 'external-proof-operator')],
};
$client = new Client($url, ...$credentials, namespace: 'external-proof');
$value = str_repeat('durable-external-value-', 131072);
$state = (getenv('RUNTIME_PROOF_DIRECTORY') ?: '/proof').'/runs.json';
switch ($mode) {
    case 'prepare':
        $client->createNamespace('external-proof');
        $client->setNamespaceExternalStorage('external-proof', 'local', thresholdBytes: 1024, config: ['uri' => 'file:///payloads']);
        foreach (['operator', 'worker'] as $role) {
            (new Psr18Transport())->send('PUT', $url.'/api/runtime-credentials/external-proof-'.$role, [
                'Authorization' => 'Bearer external-payload-fixture', 'X-Namespace' => 'external-proof',
                'Content-Type' => 'application/json', 'X-Durable-Workflow-Control-Plane-Version' => '2',
            ], ['token' => hash('sha256', 'external-proof-'.$role), 'subject' => 'external-proof-'.$role,
                'roles' => [$role], 'tenant' => 'external-proof']);
        }
        break;
    case 'worker':
        Worker::create($client, 'external-proof')->register(ExternalPayloadWorkflow::class, ExternalPayloadActivity::class, MaximumPayloadWorkflow::class)->run();
        break;
    case 'maximum':
    case 'verify-maximum':
        $handle = $mode === 'maximum'
            ? $client->startWorkflow('external-payload.maximum', 'external-maximum', 'external-proof', runTimeoutSeconds: 3600)
            : $client->workflowHandle('external-maximum');
        $result = $handle->result(timeoutSeconds: 120);
        if (!is_string($result) || strlen($result) !== 50331633
            || hash('sha256', $result) !== hash('sha256', str_repeat('m', 50331633))
            || strlen($client->payloadCodec()->encode($result)) !== 67108864) {
            throw new RuntimeException('Maximum-size result did not survive upload and download.');
        }
        echo "Exact 64 MiB encoded workflow result: passed ({$mode}).\n";
        break;
    case 'start':
        $runs = is_file($state) ? json_decode(file_get_contents($state), true, flags: JSON_THROW_ON_ERROR) : [];
        foreach (['completed' => false, 'waiting' => true] as $kind => $wait) {
            $handle = isset($runs[$kind])
                ? $client->workflowHandle($runs[$kind]['workflow_id'], $runs[$kind]['run_id'])
                : $client->startWorkflow('external-payload.proof', 'external-'.$kind, 'external-proof', [$value, $wait], runTimeoutSeconds: 3600);
            $runs[$kind] = ['workflow_id' => $handle->workflowId, 'run_id' => $handle->selectedRunId];
            file_put_contents($state, json_encode($runs, JSON_THROW_ON_ERROR));
            if (!$wait && $handle->result(timeoutSeconds: 90) !== $value) {
                throw new RuntimeException('Completed source workflow returned different bytes.');
            }
        }
        $deadline = microtime(true) + 60;
        do {
            $events = $client->workflowHistory($runs['waiting']['workflow_id'], $runs['waiting']['run_id'])['events'];
            if (in_array('ConditionWaitOpened', array_column($events, 'event_type'), true)) {
                echo "Completed result and durable waiting history verified.\n";
                exit(0);
            }
            usleep(100000);
        } while (microtime(true) < $deadline);
        throw new RuntimeException('Waiting workflow never reached its durable condition.');
    case 'verify':
        $runs = json_decode(file_get_contents($state), true, flags: JSON_THROW_ON_ERROR);
        foreach ($runs as $kind => $run) {
            if ($kind === 'waiting') {
                if ($client->queryWorkflow($run['workflow_id'], 'value', [], $run['run_id']) !== $value) {
                    throw new RuntimeException('Replayed query returned different bytes.');
                }
                $client->signalWorkflow($run['workflow_id'], 'release', [$value], $run['run_id']);
            }
            if ($client->workflowHandle($run['workflow_id'], $run['run_id'])->result(timeoutSeconds: 90) !== $value) {
                throw new RuntimeException('Restarted workflow returned different bytes.');
            }
            echo json_encode(['case' => $kind, 'run_id' => $run['run_id'], 'status' => 'completed',
                'bytes' => strlen($value), 'sha256' => hash('sha256', $value)], JSON_THROW_ON_ERROR).PHP_EOL;
        }
        break;
    case 'boundaries':
        $runs = json_decode(file_get_contents($state), true, flags: JSON_THROW_ON_ERROR);
        $transport = new Psr18Transport();
        $adminHeaders = ['Authorization' => 'Bearer external-payload-fixture', 'X-Namespace' => 'external-proof',
            'Content-Type' => 'application/json', 'Accept' => 'application/json', 'X-Durable-Workflow-Control-Plane-Version' => '2'];
        $client->createNamespace('external-other');
        foreach (['operator', 'worker'] as $role) {
            $transport->send('PUT', $url.'/api/runtime-credentials/external-proof-'.$role, $adminHeaders, [
                'token' => hash('sha256', 'external-proof-'.$role), 'subject' => 'external-proof-'.$role,
                'roles' => [$role], 'tenant' => 'external-proof',
            ]);
        }
        $run = $runs['completed'];
        $response = $transport->send('GET', $url.'/api/workflows/'.$run['workflow_id'].'/runs/'.$run['run_id'], $adminHeaders);
        $reference = $response['output_envelope']['external_payload'];
        $uri = $url.'/api/external-payloads/v1/'.$reference['reference_id'];
        $headers = ['Accept' => 'application/octet-stream', 'X-Namespace' => 'external-proof',
            'X-Durable-Workflow-Payload-Codec' => 'avro', 'X-Durable-Workflow-Payload-Size' => (string) $reference['size_bytes'],
            'X-Durable-Workflow-Payload-SHA256' => $reference['sha256']];
        foreach (['operator', 'worker'] as $role) {
            $roleHeaders = $headers + ['Authorization' => 'Bearer '.hash('sha256', 'external-proof-'.$role)];
            $blob = $transport->fetchPayload($uri, $roleHeaders, $reference['size_bytes']);
            if (hash('sha256', $blob) !== $reference['sha256'] || (new AvroPayloadCodec())->decode($blob) !== $value) {
                throw new RuntimeException('Role-scoped download returned different bytes.');
            }
            try {
                $transport->fetchPayload($uri, array_replace($roleHeaders, ['X-Namespace' => 'external-other']), $reference['size_bytes']);
                throw new RuntimeException('Cross-namespace credential was accepted.');
            } catch (ExternalPayloadException $exception) {
                if ($exception->status !== 403) {
                    throw $exception;
                }
            }
        }
        // Even an administrator cannot use a valid reference in the wrong namespace.
        try {
            $transport->fetchPayload($uri, array_replace($headers, ['Authorization' => $adminHeaders['Authorization'],
                'X-Namespace' => 'external-other']), $reference['size_bytes']);
            throw new RuntimeException('Reference crossed its owning namespace.');
        } catch (ExternalPayloadException $exception) {
            if ($exception->status !== 404 || $exception->reason !== 'external_payload_not_found') {
                throw $exception;
            }
        }
        echo "Client and worker downloads verified; cross-namespace requests rejected.\n";
        break;
    default:
        throw new RuntimeException('Choose prepare, worker, start, verify or boundaries.');
}
