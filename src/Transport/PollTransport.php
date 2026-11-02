<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

use DurableWorkflow\Exception\TransportException;
use stdClass;

/** The negotiated envelope carries HTTP outcomes after response progress starts. */
final class PollTransport
{
    public const MEDIA_TYPE = 'application/vnd.durable-workflow.poll.v1+json';
    private const SCHEMA = 'durable-workflow.poll-transport.v1';
    private const KINDS = ['workflow', 'activity', 'query', 'update-validation'];

    public static function taskKind(string $method, string $uri): ?string
    {
        $path = parse_url($uri, PHP_URL_PATH);
        if (strtoupper($method) === 'POST' && is_string($path)) {
            foreach (self::KINDS as $kind) {
                if (str_ends_with($path, '/worker/'.$kind.'-tasks/poll')) {
                    return $kind;
                }
            }
        }

        return null;
    }

    /** @return array{status: int, headers: array<string, string>, body: array<string, mixed>} */
    public static function decode(string $rawBody, int $outerStatus): array
    {
        $frame = json_decode($rawBody, flags: JSON_THROW_ON_ERROR);
        if (!$frame instanceof stdClass || ($frame->schema ?? null) !== self::SCHEMA
            || !is_int($frame->status ?? null) || $frame->status < 200 || $frame->status > 599
            || !($frame->headers ?? null) instanceof stdClass || !($frame->body ?? null) instanceof stdClass
            || ($outerStatus >= 400 && $frame->status < 400)) {
            throw new TransportException('The server returned an invalid worker poll transport envelope.');
        }
        $headers = [];
        foreach (get_object_vars($frame->headers) as $name => $value) {
            $normalized = strtolower($name);
            if (!in_array($normalized, ['retry-after', 'x-durable-workflow-protocol-version'], true)
                || !is_string($value) || str_contains($value, "\r") || str_contains($value, "\n")
                || array_key_exists($normalized, $headers)) {
                throw new TransportException('The server returned invalid worker poll transport headers.');
            }
            $headers[$normalized] = $value;
        }
        // Decode the original bytes with the SDK's existing associative JSON
        // semantics, without re-encoding payloads or changing their numbers.
        /** @var array{body: array<string, mixed>} $decoded */
        $decoded = json_decode($rawBody, true, flags: JSON_THROW_ON_ERROR);

        return ['status' => $frame->status, 'headers' => $headers, 'body' => $decoded['body']];
    }
}
