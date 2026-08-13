const composer = require('../../../composer.json');
const quickstart = require('../../quickstart-contract.json');

const release = composer.extra['durable-workflow'];
const sdkVersion = release['product-train'];
const channel = sdkVersion.includes('-') ? sdkVersion.split('-')[1].split('.')[0] : 'stable';
const versionParts = /^(\d+)\.(\d+)\.\d+(?:-[a-z]+\.\d+)?$/.exec(sdkVersion);

if (versionParts === null) {
  throw new Error('The PHP SDK release identity is not a supported semantic version.');
}

const sdkSeries = `${versionParts[1]}.${versionParts[2]}`;
const sdkChannel = `${sdkSeries}${channel === 'stable' ? '' : ' prerelease'}`;

if (quickstart.package.name !== composer.name || quickstart.package.published_version !== sdkVersion) {
  throw new Error('Quickstart package identity must match the release-owned Composer metadata.');
}
const qualificationBase = quickstart.reference_resolution?.bases?.[
  quickstart.qualification_provenance?.subject_base
];
if (qualificationBase?.kind !== 'composer_package' || qualificationBase.package_pointer !== '/package') {
  throw new Error('Quickstart qualification must resolve the release-owned Composer package metadata.');
}
const serverVersion = release['supported-server-versions'];
if (quickstart.runtime_targets?.server?.image !== `durableworkflow/server:${serverVersion}`) {
  throw new Error('Quickstart Server image must match the release-owned compatibility metadata.');
}
const serverSeries = serverVersion.split('.').slice(0, 2).join('.');
const serverChannel = `${serverSeries}${serverVersion.includes('-') ? ' prerelease' : ''}`;

const serverImageCommand = `export DW_SERVER_IMAGE="$(
  curl -fsSL https://php.durable-workflow.com/quickstart-contract.json \\
    | php -r '$contract = json_decode(stream_get_contents(STDIN), true, 512, JSON_THROW_ON_ERROR); echo $contract["runtime_targets"]["server"]["image"];'
)"`;

module.exports = Object.freeze({
  sdkVersion,
  sdkChannel,
  sdkChannelLabel: `PHP SDK ${sdkChannel}`,
  serverVersion,
  serverChannel,
  workerProtocolVersion: release['worker-protocol-version'],
  composerPackage: `${quickstart.package.name}:${quickstart.package.onboarding_requirement}`,
  composerCommand: `composer require ${quickstart.package.name}:${quickstart.package.onboarding_requirement}`,
  serverImageCommand,
  channel,
  quickstart,
});
