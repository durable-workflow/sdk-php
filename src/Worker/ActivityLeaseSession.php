<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** @internal Required worker-session identity and its immutable original TTL. */
final class ActivityLeaseSession
{
    /** @var array<string, mixed> */
    private array $value;
    /** @var array<string, mixed> */
    private array $options;
    private float $ttl;
    private float $ttlUntil;
    private float $leaseExpiry;
    private float $leaseUntil;
    private float $serverTime;

    public function __construct(mixed $value, private readonly string $namespace, private readonly string $owner,
        float $server, float $started)
    {
        if (!is_array($value)) {
            throw new \UnexpectedValueException('Invalid required activity session.');
        }
        $this->ttl = ActivityLeaseAuthority::timestamp($value['ttl_expires_at'] ?? null);
        $this->leaseExpiry = ActivityLeaseAuthority::timestamp($value['lease_expires_at'] ?? null);
        $this->ttlUntil = $started + $this->ttl - $server;
        $this->leaseUntil = $started + $this->leaseExpiry - $server;
        $this->serverTime = $server;
        $this->options = self::options($value);
        $this->acknowledge($value, $server, $started);
    }

    public function bound(): float
    {
        return min($this->ttlUntil, $this->leaseUntil);
    }

    public function ttlBound(): float
    {
        return $this->ttlUntil;
    }

    /** @return array<string, mixed> */
    public function snapshot(): array
    {
        return $this->value;
    }

    /** @param array<string, mixed> $value */
    public function acknowledge(array $value, float $server, float $started): void
    {
        $expiry = ActivityLeaseAuthority::timestamp($value['lease_expires_at'] ?? null);
        if (self::options($value) !== $this->options || ($value['status'] ?? null) !== 'active'
            || ($value['lease_owner'] ?? null) !== $this->owner
            || ($value['namespace'] ?? $this->namespace) !== $this->namespace
            || ActivityLeaseAuthority::timestamp($value['ttl_expires_at'] ?? null) !== $this->ttl
            || $expiry < $this->leaseExpiry || $server < $this->serverTime) {
            throw new \UnexpectedValueException('Session renewal changed the original holder, policy or TTL.');
        }
        $this->ttlUntil = min($this->ttlUntil, $started + $this->ttl - $server);
        // Re-reading the same grant is an observation, not a lease extension.
        $this->leaseUntil = $expiry > $this->leaseExpiry
            ? max($this->leaseUntil, $started + $expiry - $server)
            : min($this->leaseUntil, $started + $expiry - $server);
        $this->leaseExpiry = $expiry;
        $this->serverTime = $server;
        $this->value = $value;
    }

    /** @param array<string, mixed> $value
     * @return array<string, mixed>
     */
    private static function options(array $value): array
    {
        $required = ['session_id', 'lease_seconds', 'ttl_seconds', 'max_concurrent_activities',
            'create_if_missing', 'allow_reacquire_after_failure'];
        foreach ($required as $key) {
            if (!array_key_exists($key, $value)) {
                throw new \UnexpectedValueException('Missing original session option '.$key.'.');
            }
        }
        if (!is_string($value['session_id']) || trim($value['session_id']) === ''
            || !is_array($value['requirements'] ?? null)
            || (isset($value['queue']) && !is_string($value['queue']))
            || !is_bool($value['create_if_missing']) || !is_bool($value['allow_reacquire_after_failure'])) {
            throw new \UnexpectedValueException('Malformed original session options.');
        }
        foreach (['lease_seconds', 'ttl_seconds', 'max_concurrent_activities'] as $key) {
            if (!is_int($value[$key]) || $value[$key] < 1) {
                throw new \UnexpectedValueException('Invalid session duration or concurrency.');
            }
        }
        foreach ($value['requirements'] as $item) {
            if (!is_string($item) || trim($item) === '') {
                throw new \UnexpectedValueException('Invalid session requirement.');
            }
        }

        $options = [];
        foreach ([...$required, 'queue', 'requirements'] as $key) {
            $options[$key] = $value[$key] ?? null;
        }

        return $options;
    }
}
