<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Closure;
use PhpToken;
use ReflectionFunction;
use Throwable;

/** @internal Normalizes callable source without evaluating the predicate or using compiler identities. */
final class ConditionPredicateSource
{
    /** @var array<string, array{hash: string, tokens: list<PhpToken>}> */
    private static array $files = [];

    public static function canonical(Closure $predicate): ?string
    {
        if (!class_exists(PhpToken::class)) {
            return null;
        }
        try {
            $reflection = new ReflectionFunction($predicate);
            $file = $reflection->getFileName();
            if (!is_string($file) || !is_file($file)) {
                return null;
            }
            $tokens = self::fileTokens($file);
            $candidates = [];
            foreach ($tokens as $index => $token) {
                if (!$token->is([T_FUNCTION, T_FN]) || $token->line !== $reflection->getStartLine()) {
                    continue;
                }
                $candidate = self::candidate($tokens, $index, $reflection);
                if ($candidate !== null) {
                    $candidates[] = $candidate;
                }
            }
            if ($candidates === []) {
                return null;
            }
            // Reflection cannot always distinguish same-line closures. Preserve
            // every possible candidate rather than guess or invoke user code.
            $candidates = array_values(array_unique($candidates));
            sort($candidates, SORT_STRING);

            return self::frame($candidates);
        } catch (Throwable) {
            return null;
        }
    }

    /** @return list<PhpToken> */
    private static function fileTokens(string $file): array
    {
        $source = @file_get_contents($file);
        if (!is_string($source)) {
            return [];
        }
        $hash = hash('sha256', $source);
        if ((self::$files[$file]['hash'] ?? null) === $hash) {
            return self::$files[$file]['tokens'];
        }
        $tokens = array_values(array_filter(PhpToken::tokenize($source),
            static fn (PhpToken $token): bool => !$token->is([T_WHITESPACE, T_COMMENT, T_DOC_COMMENT])));
        // Bound process memory and replace a file's entry when its bytes change.
        // File paths and content hashes are cache keys, never fingerprint input.
        if (count(self::$files) >= 8) {
            array_shift(self::$files);
        }
        self::$files[$file] = ['hash' => $hash, 'tokens' => $tokens];

        return $tokens;
    }

    /** @param list<PhpToken> $tokens */
    private static function candidate(array $tokens, int $index, ReflectionFunction $reflection): ?string
    {
        $cursor = $index + 1;
        if (($tokens[$cursor]->text ?? null) === '&') {
            ++$cursor;
        }
        $name = null;
        if (($tokens[$cursor] ?? null)?->is(T_STRING)) {
            $name = $tokens[$cursor++]->text;
        }
        if (($reflection->isAnonymous() && $name !== null)
            || (!$reflection->isAnonymous() && $name !== $reflection->getShortName())
            || ($tokens[$cursor]->text ?? null) !== '(') {
            return null;
        }
        $parametersEnd = self::balancedEnd($tokens, $cursor);
        if ($parametersEnd === null || !self::parametersMatch($tokens, $cursor, $parametersEnd, $reflection)) {
            return null;
        }
        $start = self::declarationStart($tokens, $index);
        $static = false;
        foreach (array_slice($tokens, $start, $index - $start) as $modifier) {
            $static = $static || $modifier->is(T_STATIC);
        }
        if ($static !== $reflection->isStatic()) {
            return null;
        }
        $end = self::bodyEnd($tokens, $parametersEnd + 1, $tokens[$index]->is(T_FN));
        if ($end === null || self::endLine($tokens[$end]) !== $reflection->getEndLine()) {
            return null;
        }

        return self::frame(array_map(static fn (PhpToken $token): string => $token->text,
            array_slice($tokens, $start, $end - $start + 1)));
    }

    /** @param list<PhpToken> $tokens */
    private static function declarationStart(array $tokens, int $index): int
    {
        while ($index > 0 && $tokens[$index - 1]->is([T_STATIC, T_PUBLIC, T_PROTECTED, T_PRIVATE, T_FINAL, T_ABSTRACT])) {
            --$index;
        }

        return $index;
    }

    /** @param list<PhpToken> $tokens */
    private static function parametersMatch(array $tokens, int $start, int $end, ReflectionFunction $reflection): bool
    {
        $names = [];
        for ($index = $start + 1; $index < $end; ++$index) {
            $token = $tokens[$index];
            if (self::opening($token) !== null) {
                $index = self::balancedEnd($tokens, $index) ?? $end;
            } elseif ($token->is(T_VARIABLE)) {
                $names[] = substr($token->text, 1);
            }
        }
        $parameters = array_map(static fn (\ReflectionParameter $parameter): string => $parameter->getName(), $reflection->getParameters());

        return $names === $parameters;
    }

    /** @param list<PhpToken> $tokens */
    private static function bodyEnd(array $tokens, int $start, bool $arrow): ?int
    {
        for ($index = $start, $count = count($tokens); $index < $count; ++$index) {
            if ($arrow && $tokens[$index]->is(T_DOUBLE_ARROW)) {
                return self::expressionEnd($tokens, $index + 1);
            }
            if (!$arrow && $tokens[$index]->text === '{') {
                return self::balancedEnd($tokens, $index);
            }
            if (self::opening($tokens[$index]) !== null) {
                $index = self::balancedEnd($tokens, $index) ?? $count;
            }
        }

        return null;
    }

    /** @param list<PhpToken> $tokens */
    private static function expressionEnd(array $tokens, int $start): ?int
    {
        $ternaries = 0;
        for ($index = $start, $count = count($tokens); $index < $count; ++$index) {
            $text = $tokens[$index]->text;
            if (in_array($text, [',', ';', ')', ']', '}'], true) || ($text === ':' && $ternaries === 0)) {
                return $index > $start ? $index - 1 : null;
            }
            if (self::opening($tokens[$index]) !== null) {
                $end = self::balancedEnd($tokens, $index);
                if ($end === null) {
                    return null;
                }
                $index = $end;
            } elseif ($text === '?') {
                ++$ternaries;
            } elseif ($text === ':') {
                --$ternaries;
            }
        }

        return $index > $start ? $index - 1 : null;
    }

    /** @param list<PhpToken> $tokens */
    private static function balancedEnd(array $tokens, int $start): ?int
    {
        $stack = [];
        for ($index = $start, $count = count($tokens); $index < $count; ++$index) {
            $opening = self::opening($tokens[$index]);
            if ($opening !== null) {
                $stack[] = $opening;
            } elseif (in_array($tokens[$index]->text, [')', ']', '}'], true)) {
                if (array_pop($stack) !== $tokens[$index]->text) {
                    return null;
                }
                if ($stack === []) {
                    return $index;
                }
            }
        }

        return null;
    }

    private static function opening(PhpToken $token): ?string
    {
        return match ($token->text) {
            '(' => ')',
            '[', '#[' => ']',
            '{', '${' => '}',
            default => null,
        };
    }

    private static function endLine(PhpToken $token): int
    {
        return $token->line + substr_count($token->text, "\n");
    }

    /** @param list<string> $parts */
    private static function frame(array $parts): string
    {
        // Length framing preserves token boundaries, including + + versus ++.
        return implode('', array_map(static fn (string $part): string => strlen($part).':'.$part, $parts));
    }
}
