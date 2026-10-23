<?php

declare(strict_types=1);

[$script, $directory] = $argv;
$certificate = $argv[2] ?? '';
$key = $argv[3] ?? '';
$context = stream_context_create($certificate === '' ? [] : ['ssl' => ['local_cert' => $certificate, 'local_pk' => $key]]);
$server = stream_socket_server('tcp://127.0.0.1:0', $errno, $error, STREAM_SERVER_BIND | STREAM_SERVER_LISTEN, $context);
if ($server === false) {
    exit(1);
}
file_put_contents($directory.'/proxy-address', stream_socket_get_name($server, false));
while (($peer = @stream_socket_accept($server, 5)) !== false) {
    stream_set_timeout($peer, 2);
    if ($certificate !== '' && @stream_socket_enable_crypto($peer, true, STREAM_CRYPTO_METHOD_TLS_SERVER) !== true) {
        fclose($peer);
        continue;
    }
    $line = fgets($peer);
    $parts = explode(' ', trim($line === false ? '' : $line));
    if (count($parts) < 2) {
        fclose($peer);
        continue;
    }
    $headers = [];
    while (($line = fgets($peer)) !== false && $line !== "\r\n") {
        $headers[] = $line;
    }
    file_put_contents($directory.'/proxy-requests', json_encode(['method' => $parts[0], 'target' => $parts[1],
        'headers' => $headers], JSON_THROW_ON_ERROR)."\n", FILE_APPEND);
    if (is_file($directory.'/proxy-delay')) {
        usleep((int) ((float) file_get_contents($directory.'/proxy-delay') * 1_000_000));
    }
    if ($parts[0] === 'CONNECT') {
        $address = $parts[1];
        $prefix = '';
    } else {
        $uri = parse_url($parts[1]);
        $address = isset($uri['host']) ? $uri['host'].':'.($uri['port'] ?? 80) : '';
        // PHP's HTTP stream wrapper can forward origin-form through a proxy.
        // This fixture accepts its Host header as a real forward proxy does.
        if ($address === '') {
            foreach ($headers as $header) {
                if (str_starts_with(strtolower($header), 'host:')) {
                    $address = trim(substr($header, 5));
                }
            }
        }
        $prefix = $parts[0].' '.($uri['path'] ?? '/').(isset($uri['query']) ? '?'.$uri['query'] : '')." HTTP/1.1\r\n";
        foreach ($headers as $header) {
            if (!str_starts_with(strtolower($header), 'proxy-')) {
                $prefix .= $header;
            }
        }
        $prefix .= "\r\n";
    }
    $upstream = @stream_socket_client('tcp://'.$address, $errno, $error, 1);
    if ($upstream === false) {
        @fwrite($peer, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n");
        fclose($peer);
        continue;
    }
    if ($parts[0] === 'CONNECT') {
        fwrite($peer, "HTTP/1.1 200 Connection Established\r\n\r\n");
    } else {
        fwrite($upstream, $prefix);
    }
    stream_set_blocking($peer, false);
    stream_set_blocking($upstream, false);
    $until = microtime(true) + 3;
    while (!feof($peer) && !feof($upstream) && microtime(true) < $until) {
        $read = [$peer, $upstream];
        $write = $except = [];
        if (stream_select($read, $write, $except, 0, 100000) === false) {
            break;
        }
        foreach ($read as $socket) {
            $chunk = fread($socket, 65536);
            if ($chunk !== '' && $chunk !== false) {
                @fwrite($socket === $peer ? $upstream : $peer, $chunk);
            }
        }
    }
    fclose($upstream);
    fclose($peer);
}
fclose($server);
