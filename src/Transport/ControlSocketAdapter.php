<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

/** @internal Shares one library deadline across CONNECT, TLS and the origin response. */
final class ControlSocketAdapter extends \HTTP_Request2_Adapter_Socket
{
    private bool $terminalChunkDecoded = false;

    public function __construct(private readonly float $deadline, private readonly float $seconds) {}

    protected function connect(): bool
    {
        $remaining = $this->deadline - microtime(true);
        if ($remaining <= 0) {
            throw new \HTTP_Request2_MessageException('The control request deadline expired.', \HTTP_Request2_Exception::TIMEOUT);
        }
        $this->request->setConfig(['timeout' => $remaining, 'connect_timeout' => $remaining]);

        $tunnel = $this->request->getMethod() === \HTTP_Request2::METHOD_CONNECT;
        $connection = $this->request->getHeaders()['connection'] ?? null;
        if ($tunnel) {
            // Open our own socket without adding a Connection: close directive
            // to the CONNECT request sent to the configured proxy.
            $this->request->setHeader('Connection', 'close');
        }
        try {
            $keepAlive = parent::connect();
        } finally {
            if ($tunnel) {
                $this->request->setHeader('Connection', $connection);
            }
        }
        if (strtolower((string) $this->request->getUrl()->getScheme()) === 'https'
            && $this->request->getMethod() !== \HTTP_Request2::METHOD_CONNECT) {
            $this->request->setHeader('Proxy-Authorization');
        }

        return $keepAlive;
    }

    protected function establishTunnel(): void
    {
        $donor = new self($this->deadline, $this->seconds);
        $connect = new ControlHttpRequest((string) $this->request->getUrl(), \HTTP_Request2::METHOD_CONNECT,
            array_merge($this->request->getConfig(), ['adapter' => $donor]));
        $headers = $this->request->getHeaders();
        if (isset($headers['proxy-authorization'])) {
            $connect->setHeader('Proxy-Authorization', $headers['proxy-authorization']);
        }
        $response = $connect->send();
        if ($response->getStatus() < 200 || $response->getStatus() >= 300) {
            throw new \HTTP_Request2_ConnectionException('The HTTP CONNECT proxy refused the tunnel: '.$response->getStatus().'.');
        }
        $this->socket = $donor->socket;
        $donor->socket = null; // @phpstan-ignore assign.propertyType (The library also clears its cached socket with null on disconnect.)
        $this->socket->setDeadline($this->deadline, (int) ceil($this->seconds));
        ControlSocketBudget::enable($this->socket, $this->deadline);
    }
    protected function readResponse(): \HTTP_Request2_Response
    {
        $this->terminalChunkDecoded = false;
        $response = parent::readResponse();
        $hasBody = $response->getStatus() >= 200 && !in_array($response->getStatus(), [204, 304], true)
            && !in_array($this->request->getMethod(), [\HTTP_Request2::METHOD_HEAD, \HTTP_Request2::METHOD_CONNECT], true);
        if ($hasBody && strtolower((string) $response->getHeader('transfer-encoding')) === 'chunked' && !$this->terminalChunkDecoded) {
            throw new \HTTP_Request2_MessageException('The chunked HTTP response ended before its terminal chunk.',
                \HTTP_Request2_Exception::DECODE_ERROR);
        }

        return $response;
    }

    protected function readChunked($bufferSize): string
    {
        $data = parent::readChunked($bufferSize);
        if ($data === '' && $this->chunkLength === 0) {
            $this->terminalChunkDecoded = true;
        }

        return $data;
    }

}
