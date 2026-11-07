<?php

declare(strict_types=1);

$path = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);
$state = getenv('PIPELINE_TEST_STATE');
$record = static function (array $event) use ($state): void {
    $handle = fopen($state, 'c+');
    flock($handle, LOCK_EX);
    $value = json_decode(stream_get_contents($handle), true, flags: JSON_THROW_ON_ERROR);
    $value['events'][] = $event + ['time' => microtime(true)];
    rewind($handle);
    ftruncate($handle, 0);
    fwrite($handle, json_encode($value, JSON_THROW_ON_ERROR));
    flock($handle, LOCK_UN);
    fclose($handle);
};
if ($path === '/payload') {
    $record(['kind' => 'payload']);
    header('Content-Type: application/octet-stream');
    header('X-Durable-Workflow-Payload-Codec: avro');
    header('X-Durable-Workflow-Payload-Size: 7');
    header('X-Durable-Workflow-Payload-SHA256: '.hash('sha256', 'fixture'));
    echo 'fixture';
    return;
}
$raw = file_get_contents('php://input');
$body = $raw === '' ? [] : json_decode($raw, true, flags: JSON_THROW_ON_ERROR);
$record(['kind' => 'request', 'path' => $path, 'bytes' => strlen($raw), 'body' => $body]);
if (($body['hold'] ?? false) === true) {
    $deadline = microtime(true) + 2;
    do {
        $handle = fopen($state, 'r');
        flock($handle, LOCK_SH);
        $value = json_decode(stream_get_contents($handle), true, flags: JSON_THROW_ON_ERROR);
        flock($handle, LOCK_UN);
        fclose($handle);
        if (($value['release'] ?? false) === true) break;
        usleep(1000);
    } while (microtime(true) < $deadline);
}
usleep(200_000);
$record(['kind' => 'answer', 'path' => $path]);
$status = $body['status'] ?? 200;
http_response_code($status);
header('Content-Type: application/json');
echo json_encode($status === 200 ? ['accepted' => true] : ['reason' => 'fixture_unavailable'], JSON_THROW_ON_ERROR);
