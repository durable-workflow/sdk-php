<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

/** @internal Keeps the library's CONNECT-to-TLS upgrade inside its existing deadline. */
final class ControlSocketBudget extends \HTTP_Request2_SocketWrapper
{
    public static function enable(\HTTP_Request2_SocketWrapper $wrapper, float $deadline): void
    {
        $method = STREAM_CRYPTO_METHOD_TLSv1_2_CLIENT;
        if (defined('STREAM_CRYPTO_METHOD_TLSv1_3_CLIENT')) {
            $method |= STREAM_CRYPTO_METHOD_TLSv1_3_CLIENT;
        }
        set_error_handler(static fn (): bool => true, E_WARNING);
        try {
            while (true) {
                $remaining = $deadline - microtime(true);
                if ($remaining <= 0) {
                    throw new \HTTP_Request2_MessageException('The control request deadline expired.', \HTTP_Request2_Exception::TIMEOUT);
                }
                $enabled = stream_socket_enable_crypto($wrapper->socket, true, $method);
                if ($enabled === true) {
                    return;
                }
                if ($enabled === false) {
                    throw new \HTTP_Request2_ConnectionException('The proxy tunnel TLS handshake failed.');
                }
                // OpenSSL performs the handshake. Wait for peer data without
                // switching the socket into the library's unbounded blocking mode.
                $read = [$wrapper->socket];
                $write = $except = [];
                $seconds = (int) floor($remaining);
                stream_select($read, $write, $except, $seconds, (int) floor(($remaining - $seconds) * 1_000_000));
            }
        } finally {
            restore_error_handler();
        }
    }
}
