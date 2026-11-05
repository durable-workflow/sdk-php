<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

use JsonException;

/** @internal Check original member names before associative decoding can discard them. */
final class UniqueJson
{
    public static function decode(string $json): mixed
    {
        if (preg_match('//u', $json) !== 1) {
            throw new JsonException('The JSON document is not valid UTF-8.');
        }
        $offset = 0;
        self::value($json, $offset, 0);
        self::whitespace($json, $offset);
        if ($offset !== strlen($json)) {
            throw new JsonException('Unexpected data after the JSON document.');
        }

        return json_decode($json, true, 512, JSON_THROW_ON_ERROR);
    }

    private static function value(string $json, int &$offset, int $depth): void
    {
        if ($depth >= 512) {
            throw new JsonException('Maximum JSON depth exceeded.');
        }
        self::whitespace($json, $offset);
        $token = $json[$offset] ?? '';
        if ($token === '{') {
            self::object($json, $offset, $depth + 1);
        } elseif ($token === '[') {
            self::items($json, $offset, $depth + 1);
        } elseif ($token === '"') {
            self::stringToken($json, $offset);
        } elseif (preg_match('/\G(?:true|false|null)/A', $json, $match, 0, $offset) === 1) {
            $offset += strlen($match[0]);
        } elseif (preg_match('/\G-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?/A', $json, $match, 0, $offset) === 1) {
            $offset += strlen($match[0]);
            if (preg_match('/\G[eE][+-]?[0-9]+/A', $json, $exponent, 0, $offset) === 1) {
                $offset += strlen($exponent[0]);
            }
        } else {
            throw new JsonException('Invalid JSON value.');
        }
    }

    private static function object(string $json, int &$offset, int $depth): void
    {
        ++$offset;
        self::whitespace($json, $offset);
        if (($json[$offset] ?? '') === '}') {
            ++$offset;
            return;
        }
        $seen = [];
        while (true) {
            if (($json[$offset] ?? '') !== '"') {
                throw new JsonException('Invalid JSON object member.');
            }
            $key = json_decode(self::stringToken($json, $offset), true, 512, JSON_THROW_ON_ERROR);
            $identity = "\0".$key;
            if (isset($seen[$identity])) {
                throw new JsonException('Duplicate JSON object member.');
            }
            $seen[$identity] = true;
            self::whitespace($json, $offset);
            if (($json[$offset] ?? '') !== ':') {
                throw new JsonException('Invalid JSON object separator.');
            }
            ++$offset;
            self::value($json, $offset, $depth);
            self::whitespace($json, $offset);
            $separator = $json[$offset] ?? '';
            ++$offset;
            if ($separator === '}') {
                return;
            }
            if ($separator !== ',') {
                throw new JsonException('Invalid JSON object separator.');
            }
            self::whitespace($json, $offset);
        }
    }

    private static function items(string $json, int &$offset, int $depth): void
    {
        ++$offset;
        self::whitespace($json, $offset);
        if (($json[$offset] ?? '') === ']') {
            ++$offset;
            return;
        }
        while (true) {
            self::value($json, $offset, $depth);
            self::whitespace($json, $offset);
            $separator = $json[$offset] ?? '';
            ++$offset;
            if ($separator === ']') {
                return;
            }
            if ($separator !== ',') {
                throw new JsonException('Invalid JSON array separator.');
            }
        }
    }

    private static function stringToken(string $json, int &$offset): string
    {
        $start = $offset++;
        while (($end = strpos($json, '"', $offset)) !== false) {
            $slashes = 0;
            for ($index = $end - 1; $index > $start && $json[$index] === '\\'; --$index) {
                ++$slashes;
            }
            $offset = $end + 1;
            if ($slashes % 2 === 0) {
                return substr($json, $start, $offset - $start);
            }
        }

        throw new JsonException('Unterminated JSON string.');
    }

    private static function whitespace(string $json, int &$offset): void
    {
        $offset += strspn($json, " \t\r\n", $offset);
    }
}
