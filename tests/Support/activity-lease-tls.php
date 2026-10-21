<?php

declare(strict_types=1);

// Local TLS termination forwards real requests to the existing HTTP fixture.
// Certificate material is generated for one test and never leaves its directory.
[$script, $certificate, $key, $upstream, $ready] = $argv;
$context = stream_context_create(['ssl' => ['local_cert' => $certificate, 'local_pk' => $key]]);
$server = stream_socket_server('tcp://127.0.0.1:0', $errno, $error, STREAM_SERVER_BIND | STREAM_SERVER_LISTEN, $context);
if ($server === false) {
    exit(1);
}
file_put_contents($ready, stream_socket_get_name($server, false));
$until = microtime(true) + 30;
while (microtime(true) < $until) {
    $client = @stream_socket_accept($server, 1);
    if ($client === false) {
        continue;
    }
    stream_set_timeout($client, 3);
    if (@stream_socket_enable_crypto($client, true, STREAM_CRYPTO_METHOD_TLS_SERVER) !== true) {
        fclose($client);
        continue;
    }
    $peer = @stream_socket_client($upstream, $errno, $error, 3);
    if ($peer === false) {
        fclose($client);
        continue;
    }
    stream_set_timeout($peer, 3);
    $headers = '';
    $length = 0;
    while (($line = fgets($client, 65537)) !== false) {
        $headers .= $line;
        if (strlen($headers) > 65536) {
            break;
        }
        if (preg_match('/^Content-Length:\s*(\d+)\s*$/i', trim($line), $matches)) {
            $length = (int) $matches[1];
        }
        if ($line === "\r\n") {
            break;
        }
    }
    if (strlen($headers) <= 65536 && $length <= 65536 && str_ends_with($headers, "\r\n\r\n")) {
        $body = '';
        while (strlen($body) < $length) {
            $chunk = fread($client, $length - strlen($body));
            if ($chunk === false || $chunk === '') {
                break;
            }
            $body .= $chunk;
        }
        if (strlen($body) === $length) {
            fwrite($peer, $headers.$body);
            stream_copy_to_stream($peer, $client);
        }
    }
    fclose($peer);
    fclose($client);
}
fclose($server);
