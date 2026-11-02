<?php

declare(strict_types=1);

[$script, $directory, $mode] = $argv;
$certificate = $argv[3] ?? '';
$key = $argv[4] ?? '';
$context = stream_context_create($certificate === '' ? [] : ['ssl' => ['local_cert' => $certificate, 'local_pk' => $key]]);
$server = stream_socket_server('tcp://127.0.0.1:0', $errno, $error, STREAM_SERVER_BIND | STREAM_SERVER_LISTEN, $context);
if ($server === false) {
    exit(1);
}
file_put_contents($directory.'/address.tmp', stream_socket_get_name($server, false));
rename($directory.'/address.tmp', $directory.'/address');
$until = microtime(true) + 30;
while (microtime(true) < $until) {
    $peer = @stream_socket_accept($server, 1);
    if ($peer === false) {
        continue;
    }
    stream_set_timeout($peer, 3);
    if ($mode === 'stall_tls') {
        sleep(5);
        fclose($peer);
        continue;
    }
    if ($certificate !== '' && @stream_socket_enable_crypto($peer, true, STREAM_CRYPTO_METHOD_TLS_SERVER) !== true) {
        fclose($peer);
        continue;
    }
    $line = fgets($peer);
    $parts = explode(' ', $line === false ? '' : $line);
    if (count($parts) < 2) {
        fclose($peer);
        continue;
    }
    $headers = [];
    while (($line = fgets($peer)) !== false && $line !== "\r\n") {
        $pair = explode(':', trim($line), 2);
        if (count($pair) === 2) {
            $headers[strtolower($pair[0])] = trim($pair[1]);
        }
    }
    $length = (int) ($headers['content-length'] ?? 0);
    $body = '';
    while (strlen($body) < $length) {
        $chunk = fread($peer, $length - strlen($body));
        if ($chunk === '' || $chunk === false) {
            break;
        }
        $body .= $chunk;
    }
    file_put_contents($directory.'/requests', json_encode(['method' => $parts[0], 'path' => $parts[1],
        'headers' => $headers, 'body' => $body], JSON_THROW_ON_ERROR)."\n", FILE_APPEND | LOCK_EX);
    $replyMode = str_starts_with($parts[1], '/api/cluster/info') ? 'echo' : $mode;
    if (str_starts_with($parts[1], '/api/cluster/info')) {
        $payload = '{"limits":{"max_payload_bytes":1048576}}';
    } elseif ($replyMode === 'echo') {
        $payload = '{"status":"ok","workflows":[]}';
    } else {
        $payload = '{"workflows":[]}';
    }
    if ($replyMode === 'stall' && !str_starts_with($parts[1], '/api/cluster/info')) {
        sleep(5);
        fclose($peer);
        continue;
    }
    if ($mode === 'delay') {
        usleep(250000);
    }
    $status = $replyMode === 'redirect' ? '302 Found' : '200 OK';
    $wirePayload = $replyMode === 'gzip' ? gzencode($payload) : $payload;
    $chunked = in_array($replyMode, ['chunked', 'chunked_bare', 'truncated_chunked'], true);
    $replyHeaders = "HTTP/1.1 {$status}\r\nContent-Type: application/json\r\n"
        .($chunked ? "Transfer-Encoding: chunked\r\n" : 'Content-Length: '.(strlen($wirePayload) + ($replyMode === 'truncated_length' ? 10 : 0))."\r\n")
        .($replyMode === 'gzip' ? "Content-Encoding: gzip\r\n" : '')
        ."Connection: close\r\n".($replyMode === 'redirect' ? "Location: /unexpected-redirect\r\n" : '')."\r\n";
    if ($replyMode === 'interim') {
        @fwrite($peer, "HTTP/1.1 100 Continue\r\n\r\n");
    }
    if ($chunked) {
        $wirePayload = dechex(strlen($payload))."\r\n".$payload."\r\n"
            .($replyMode === 'chunked' ? "0\r\nX-Fixture-Trailer: complete\r\n\r\n" : ($replyMode === 'chunked_bare' ? "0\r\n\r\n" : ''));
    }
    if ($replyMode === 'slow_headers') {
        foreach (str_split($replyHeaders) as $byte) {
            if (@fwrite($peer, $byte) === false) {
                break;
            }
            fflush($peer);
            usleep(50000);
        }
    } else {
        @fwrite($peer, $replyHeaders);
    }
    if ($replyMode === 'slow_body') {
        foreach (str_split($payload) as $byte) {
            if (@fwrite($peer, $byte) === false) {
                break;
            }
            fflush($peer);
            usleep(100000);
        }
    } else {
        @fwrite($peer, $wirePayload);
    }
    fclose($peer);
}
fclose($server);
