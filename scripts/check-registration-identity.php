<?php

declare(strict_types=1);

use Composer\InstalledVersions;
use DurableWorkflow\Client;
use DurableWorkflow\SdkIdentity;
use DurableWorkflow\Transport\Transport;

if ($argc !== 3) {
    fwrite(STDERR, "Usage: php scripts/check-registration-identity.php AUTOLOAD EXPECTED_VERSION\n");
    exit(2);
}

$autoload = $argv[1];
$expectedVersion = $argv[2];
if (!is_file($autoload)) {
    fwrite(STDERR, "Composer autoloader not found: {$autoload}\n");
    exit(2);
}

require $autoload;

$installedVersion = InstalledVersions::getPrettyVersion(SdkIdentity::PACKAGE);
if ($expectedVersion === SdkIdentity::SOURCE_DEVELOPMENT_VERSION) {
    if (!is_string($installedVersion) || !(str_starts_with($installedVersion, 'dev-') || str_ends_with($installedVersion, '-dev'))) {
        fwrite(STDERR, "Expected a development Composer package version, got ".var_export($installedVersion, true)."\n");
        exit(1);
    }
} elseif ($installedVersion !== $expectedVersion) {
    fwrite(STDERR, "Installed Composer package version {$installedVersion} does not match selected release {$expectedVersion}\n");
    exit(1);
}

$transport = new class implements Transport {
    /** @var array<string, mixed>|null */
    public ?array $registration = null;

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        $this->registration = $body;

        return ['registered' => true];
    }
};

$client = new Client('https://server.example', transport: $transport);
$client->registerWorker('identity-check', 'identity-check', [], []);

$expectedIdentity = 'durable-workflow-php/'.$expectedVersion;
$advertisedIdentity = $transport->registration['sdk_version'] ?? null;
if ($advertisedIdentity !== $expectedIdentity) {
    fwrite(STDERR, 'Worker registration advertised '.var_export($advertisedIdentity, true)."; expected {$expectedIdentity}\n");
    exit(1);
}

fwrite(STDOUT, "Worker registration identity verified: {$advertisedIdentity}\n");
