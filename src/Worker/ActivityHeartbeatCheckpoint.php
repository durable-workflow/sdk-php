<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DateTimeImmutable;
use DateTimeZone;
use UnexpectedValueException;

/**
 * The acknowledged heartbeat checkpoint received with this activity claim.
 *
 * It is a snapshot from an earlier attempt of the same logical activity
 * execution, never a pending write. Heartbeat writes made by this attempt do
 * not change it, whether they are acknowledged, rejected or their response is
 * lost. Acknowledgement does not commit an external business effect
 * atomically, so keep using stable idempotency keys for repeatable effects.
 */
final class ActivityHeartbeatCheckpoint
{
    /**
     * The negotiated wire contract version. Server v1 supplies no
     * per-checkpoint write sequence, so this is not a sequence number.
     */
    public readonly int $contractVersion;

    private const TIMESTAMP = '/^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.(\d{1,9}))?(Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/D';

    /**
     * @param string $acknowledgedAt The producer's RFC 3339 acknowledgement time, as received.
     */
    private function __construct(
        private readonly string $detailsJson,
        public readonly string $acknowledgedAt,
    ) {
        $this->contractVersion = 1;
    }

    /** @internal Built only from a negotiated claim. */
    public static function received(mixed $details, mixed $acknowledgedAt): self
    {
        if ($details === null || !is_string($acknowledgedAt) || self::parse($acknowledgedAt) === null) {
            throw self::invalid();
        }
        try {
            $json = json_encode($details, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION);
        } catch (\JsonException) {
            throw self::invalid();
        }

        return new self($json, $acknowledgedAt);
    }

    /** An independent copy of the acknowledged details, without defaults. */
    public function details(): mixed
    {
        return json_decode($this->detailsJson, true, flags: JSON_THROW_ON_ERROR);
    }

    /**
     * The acknowledgement time in UTC. PHP keeps microseconds, so finer
     * fractions are truncated here; {@see $acknowledgedAt} keeps the original.
     */
    public function acknowledgedAtTime(): DateTimeImmutable
    {
        return self::parse($this->acknowledgedAt) ?? throw self::invalid();
    }

    private static function parse(string $value): ?DateTimeImmutable
    {
        if (preg_match(self::TIMESTAMP, $value, $match) !== 1) {
            return null;
        }
        $fraction = substr(str_pad($match[1], 6, '0'), 0, 6);
        $offset = $match[2] === 'Z' ? '+00:00' : $match[2];
        $normalized = substr($value, 0, 19).'.'.$fraction.$offset;
        $parsed = DateTimeImmutable::createFromFormat('!Y-m-d\TH:i:s.uP', $normalized);
        $errors = DateTimeImmutable::getLastErrors();
        if ($parsed === false || ($errors !== false && ($errors['warning_count'] > 0 || $errors['error_count'] > 0))) {
            return null;
        }

        return $parsed->setTimezone(new DateTimeZone('UTC'));
    }

    /** @internal */
    public static function invalid(): UnexpectedValueException
    {
        return new UnexpectedValueException('activity_heartbeat_resume v1 claim has an invalid checkpoint');
    }
}
