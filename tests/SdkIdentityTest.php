<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\SdkIdentity;
use PHPUnit\Framework\TestCase;

final class SdkIdentityTest extends TestCase
{
    public function testSourceCheckoutUsesTheExplicitDevelopmentIdentity(): void
    {
        self::assertSame(SdkIdentity::SOURCE_DEVELOPMENT_VERSION, SdkIdentity::version());
        self::assertSame('durable-workflow-php/source-development', SdkIdentity::registration());
    }
}
