<?php

declare(strict_types=1);

namespace DurableWorkflow;

use Composer\InstalledVersions;

/** Resolves the SDK package identity from Composer's installed-package metadata. */
final class SdkIdentity
{
    public const PACKAGE = 'durable-workflow/sdk';
    public const SOURCE_DEVELOPMENT_VERSION = 'source-development';

    private const REGISTRATION_NAME = 'durable-workflow-php';

    public static function version(): string
    {
        $rootPackage = InstalledVersions::getRootPackage();
        if ($rootPackage['name'] === self::PACKAGE || !InstalledVersions::isInstalled(self::PACKAGE)) {
            return self::SOURCE_DEVELOPMENT_VERSION;
        }

        $version = InstalledVersions::getPrettyVersion(self::PACKAGE);
        if ($version === null || trim($version) === '' || str_starts_with($version, 'dev-') || str_ends_with($version, '-dev')) {
            return self::SOURCE_DEVELOPMENT_VERSION;
        }

        return $version;
    }

    public static function registration(): string
    {
        return self::REGISTRATION_NAME.'/'.self::version();
    }

    private function __construct()
    {
    }
}
