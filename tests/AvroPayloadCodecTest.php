<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use PHPUnit\Framework\TestCase;

final class AvroPayloadCodecTest extends TestCase
{
    public function testGenericWrapperRoundTripsJsonSafeValues(): void
    {
        $codec = new AvroPayloadCodec();
        $value = ['integer' => 7, 'float' => 7.0, 'unicode' => 'héllo', 'list' => [true, null]];
        $blob = $codec->encode($value);

        self::assertSame("\x00", base64_decode($blob, true)[0]);
        self::assertSame($value, $codec->decode($blob));
    }

    public function testTypedWriterSchemaIsEmbeddedAndReadableWithoutRegistry(): void
    {
        $codec = (new AvroPayloadCodec())->withSchema(
            '{"type":"record","name":"Greeting","fields":[{"name":"message","type":"string"}]}',
        );
        $blob = $codec->encode(['message' => 'hello']);

        self::assertSame("\x01", base64_decode($blob, true)[0]);
        self::assertSame(['message' => 'hello'], (new AvroPayloadCodec())->decode($blob));
    }
}
