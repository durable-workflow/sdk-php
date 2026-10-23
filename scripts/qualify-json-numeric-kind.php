<?php

declare(strict_types=1);

// A source-free recording PSR-18 boundary; no runtime or reader coercion.
require_once $argv[1];

$http = new class implements Psr\Http\Client\ClientInterface {
    /** @var list<Psr\Http\Message\RequestInterface> */
    public array $requests = [];
    public ?int $requestLimit = null;

    public function sendRequest(Psr\Http\Message\RequestInterface $request): Psr\Http\Message\ResponseInterface
    {
        $this->requests[] = $request;

        $body = $this->requestLimit === null ? '{}' : json_encode(
            ['limits' => ['max_payload_bytes' => $this->requestLimit]], JSON_THROW_ON_ERROR,
        );

        return new GuzzleHttp\Psr7\Response(200, ['Content-Type' => 'application/json'], $body);
    }
};
$client = new DurableWorkflow\Client('https://recording.invalid',
    transport: new DurableWorkflow\Transport\Psr18Transport(client: $http));
$values = ['whole' => 1.0, 'positive_zero' => 0.0, 'negative_zero' => -0.0, 'integer' => 1,
    'max' => PHP_INT_MAX, 'fraction' => 1.25, 'nested' => ['whole' => 2.0, 'list' => [3.0, -0.0, 3]]];
$results = [];
foreach (['start', 'schedule'] as $operation) {
    if ($operation === 'start') {
        $client->startWorkflow('numeric-kind', 'numeric-kind', 'numeric-kind', [$values],
            memo: $values, searchAttributes: $values);
    } else {
        $client->createSchedule(new DurableWorkflow\Model\ScheduleSpec(cronExpressions: ['0 * * * *']),
            new DurableWorkflow\Model\ScheduleAction('numeric-kind', 'numeric-kind', [$values]), 'numeric-kind',
            memo: $values, searchAttributes: $values);
    }
    $request = $http->requests[array_key_last($http->requests)];
    $wire = (string) $request->getBody();
    $decoded = json_decode($wire, true, flags: JSON_THROW_ON_ERROR);
    $metadata = true;
    foreach (['memo', 'search_attributes'] as $field) {
        $metadata = $metadata && $decoded[$field] === $values
            && bin2hex(pack('E', $decoded[$field]['negative_zero'])) === '8000000000000000'
            && bin2hex(pack('E', $decoded[$field]['nested']['list'][1])) === '8000000000000000';
    }
    $envelope = $operation === 'start' ? $decoded['input'] : $decoded['action']['input'];
    $results[$operation] = ['metadata_preserved' => $metadata,
        'avro_envelope_unchanged' => $envelope === $client->payloadCodec()->envelope([$values]),
        'wire_whole_float' => str_contains($wire, '"whole":1.0'),
        'wire_negative_zero' => str_contains($wire, '"negative_zero":-0.0')];
}
$http->requests = [];
$http->requestLimit = 2500;
$client = new DurableWorkflow\Client('https://recording.invalid',
    transport: new DurableWorkflow\Transport\Psr18Transport(client: $http));
$rejected = false;
try {
    $client->startWorkflow('numeric-kind', 'numeric-kind', 'numeric-kind', [],
        memo: ['values' => array_fill(0, 800, 1.0)]);
} catch (DurableWorkflow\Exception\ExternalPayloadException $exception) {
    $rejected = $exception->status === 413 && $exception->reason === 'payload_too_large';
}
$results['request_budget'] = ['rejected_before_upload_or_start' => $rejected && count($http->requests) === 1];
$pass = true;
foreach ($results as $checks) {
    $pass = $pass && !in_array(false, $checks, true);
}
echo json_encode(['outcome' => $pass ? 'pass' : 'fail',
    'sdk_version' => Composer\InstalledVersions::getPrettyVersion('durable-workflow/sdk'),
    'sdk_reference' => Composer\InstalledVersions::getReference('durable-workflow/sdk'),
    'operations' => $results], JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR)."\n";
exit($pass ? 0 : 1);
