<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

use InvalidArgumentException;

/**
 * A signal name the Server would refuse with invalid_signal_name, raised in
 * workflow code before any command is emitted.
 *
 * A name must be non-blank, contain no control characters (bytes below 0x20
 * or 0x7f), and be at most 191 characters and 255 bytes of UTF-8.
 */
final class InvalidSignalName extends InvalidArgumentException
{
    public const REASON = 'invalid_signal_name';

    public readonly string $reason;

    /** @param string $problem blank, control_character, invalid_utf8, too_many_characters or too_many_bytes */
    public function __construct(public readonly string $problem, string $message)
    {
        $this->reason = self::REASON;
        parent::__construct(self::REASON.': '.$message);
    }

    /** @throws self when the Server would refuse the name */
    public static function assertValid(string $signalName): void
    {
        if (trim($signalName) === '') {
            throw new self('blank', 'The signal name must not be empty or blank.');
        }
        if (preg_match('/[\x00-\x1f\x7f]/', $signalName) === 1) {
            throw new self('control_character', 'The signal name must not contain control characters.');
        }
        if (strlen($signalName) > 255) {
            throw new self('too_many_bytes', 'The signal name must be at most 255 bytes.');
        }
        $characters = preg_match_all('/./su', $signalName);
        if ($characters === false) {
            throw new self('invalid_utf8', 'The signal name must be valid UTF-8.');
        }
        if ($characters > 191) {
            throw new self('too_many_characters', 'The signal name must be at most 191 characters.');
        }
    }
}
