<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use DurableWorkflow\Tests\Support\WorkerPipelineScenario;
use DurableWorkflow\Worker;
use ReflectionMethod;
use RuntimeException;

require_once dirname(__DIR__, 3).'/tests/Support/CompletionPipelineScenario.php';
require_once dirname(__DIR__, 3).'/tests/Support/WorkerPipelineScenario.php';
require_once dirname(__DIR__, 3).'/tests/Support/RecordingMetricsRecorder.php';

/** Installed-package packet regression only. Its fixed socket peer is not Native conformance. */
final class CompletionPollPacketDefaults
{
    /** @return array<string, mixed> */
    public static function run(): array
    {
        foreach (['__construct', 'create'] as $method) {
            $options = array_filter((new ReflectionMethod(Worker::class, $method))->getParameters(),
                static fn ($parameter): bool => $parameter->getName() === 'enableCompletionPipelining');
            if (count($options) !== 1 || current($options)->getDefaultValue() !== false) {
                throw CompletionPollFailure::product('completion_default_option_not_false');
            }
        }
        $implicit = WorkerPipelineScenario::run(null);
        $explicit = WorkerPipelineScenario::run(false);
        if ($implicit['error'] !== null || $explicit['error'] !== null) throw new RuntimeException('completion_packet_fixture_failed');
        $first = self::digest($implicit['requests']);
        $second = self::digest($explicit['requests']);
        if ($first !== $second) throw CompletionPollFailure::product('completion_default_packets_changed');
        return ['outcome' => 'pass', 'runtime_observed' => false,
            'scope' => 'installed SDK fixed-socket packet regression; no Native producer acceptance',
            'normalized_packets_sha256' => $first, 'implicit_request_count' => count($implicit['requests']),
            'explicit_request_count' => count($explicit['requests']),
            'normalization' => ['generated poll identities preserve equality and reuse', 'local attempt duration_ms'],
            'default_option_false' => true];
    }

    /** @param list<array<string, mixed>> $requests */
    public static function digest(array $requests): string
    {
        $identities = [];
        $normalize = static function (mixed $value) use (&$normalize, &$identities): mixed {
            if (!is_array($value)) return $value;
            foreach ($value as $key => $item) {
                if ($key === 'poll_request_id' && is_string($item)) {
                    $identities[$item] ??= 'poll-'.(count($identities) + 1);
                    $value[$key] = $identities[$item];
                } elseif ($key === 'duration_ms') {
                    $value[$key] = '<measured-local-duration>';
                } else {
                    $value[$key] = $normalize($item);
                }
            }
            return $value;
        };
        return hash('sha256', json_encode($normalize($requests), JSON_THROW_ON_ERROR));
    }
}
