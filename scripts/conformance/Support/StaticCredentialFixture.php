<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use InvalidArgumentException;

/** Shared canonical data loader for source controls and the caller-owned consumer runner. */
final class StaticCredentialFixture
{
    public const SHA256 = '0d4f01b763d72850943acbd9a74a0cc5da1ca6db5bc1b6fa5ca37fda11810acb';
    public const BYTES = 60716;
    public const IDS = 319;

    /** @var array<string, mixed> */
    public readonly array $data;

    public function __construct(string $path)
    {
        if (!is_file($path) || !is_readable($path) || filesize($path) !== self::BYTES
            || hash_file('sha256', $path) !== self::SHA256) {
            throw new InvalidArgumentException('The canonical static credential fixture identity differs.');
        }
        $data = json_decode((string) file_get_contents($path), true, flags: JSON_THROW_ON_ERROR);
        if (!is_array($data)) {
            throw new InvalidArgumentException('The canonical static credential fixture must be an object.');
        }
        $this->data = $data;
        $ids = $this->ids();
        if (count($ids) !== self::IDS || count(array_unique($ids)) !== self::IDS) {
            throw new InvalidArgumentException('The canonical static credential fixture inventory differs.');
        }
    }

    /** @return list<string> */
    public function ids(): array
    {
        $ids = [$this->data['binary_key_boundary']['id']];
        foreach (['common_scalar_cases', 'php_byte_loader_cases', 'pre_selection_cases',
            'php_adapter_cases', 'server_pre_selection_cases'] as $group) {
            array_push($ids, ...array_column($this->data[$group], 'id'));
        }
        return $ids;
    }

    /** @return array<string, mixed> */
    public function scalar(string $id): array
    {
        return $this->find(['common_scalar_cases', 'php_byte_loader_cases'], $id);
    }

    /** @return array<string, mixed> */
    public function adapter(string $id): array
    {
        return $this->find(['pre_selection_cases', 'php_adapter_cases'], $id);
    }

    public function value(string $id): string
    {
        $case = $this->scalar($id);
        if (isset($case['input_hex'])) {
            $bytes = hex2bin($case['input_hex']);
            if ($bytes === false) {
                throw new InvalidArgumentException('The canonical byte fixture must contain hex.');
            }
            return $bytes;
        }
        return self::points($case['input_code_points']);
    }

    /** @param list<mixed> $points */
    public static function points(array $points): string
    {
        $value = '';
        foreach ($points as $point) {
            if (!is_int($point) || $point < 0 || $point > 0x10ffff || ($point >= 0xd800 && $point <= 0xdfff)) {
                throw new InvalidArgumentException('The fixture must contain Unicode scalar values.');
            }
            $value .= match (true) {
                $point <= 0x7f => chr($point),
                $point <= 0x7ff => chr(0xc0 | ($point >> 6)).chr(0x80 | ($point & 0x3f)),
                $point <= 0xffff => chr(0xe0 | ($point >> 12)).chr(0x80 | (($point >> 6) & 0x3f)).chr(0x80 | ($point & 0x3f)),
                default => chr(0xf0 | ($point >> 18)).chr(0x80 | (($point >> 12) & 0x3f))
                    .chr(0x80 | (($point >> 6) & 0x3f)).chr(0x80 | ($point & 0x3f)),
            };
        }
        return $value;
    }

    /**
     * @param list<string> $groups
     * @return array<string, mixed>
     */
    private function find(array $groups, string $id): array
    {
        foreach ($groups as $group) {
            foreach ($this->data[$group] as $case) {
                if ($case['id'] === $id) {
                    return $case;
                }
            }
        }
        throw new InvalidArgumentException('The canonical credential case must exist in the selected family.');
    }
}
