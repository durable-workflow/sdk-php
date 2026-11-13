# PHP CI runtime

This is the SDK adaptation of Sample App's reviewed PHP/Composer container
pattern from main `c680ef92d578ee837d2d960c4235d5919d890134` (PR #341).
The literal official PHP 8.1, 8.2 and 8.4 image indexes are in
`scripts/ci/php-container-images.json`. The fixed Bookworm APT prerequisite
must come from central source `18783be6bef737cc778e4c7282a8797e39daa61a` at
`.central-apt-source`; setup verifies its commit and helper hash before building.

PHP 8.1 uses the separate `Dockerfile.81`. Its last official image contains
APT 2.6.1, so the build copies only `/usr/local` and the hashed PHP source
archive from that immutable image. It runs on the fixed linux/amd64 Bookworm
parent already bound by the official 8.2/8.4 images, checks the APT prerequisite
before any APT request, then installs the PHP ABI/runtime and build libraries.
It never starts the old image or runs its APT. This source proposal preserves
the existing PHP 8.1 and Carbon 2 jobs; the actual build, modules, Composer TLS
and original consumers must qualify independently before caller activation.

The common runtime compiles bcmath, intl, mbstring, pcntl, sockets and zip with
two build jobs. It retains the official image's cURL, POSIX, XML and SQLite
modules and probes the SDK/PHPUnit requirements, production INI and absent
coverage drivers. It adds no MySQL, PostgreSQL, Redis or GD profile. The
existing vendor Brotli verification and Composer 2.10.3 trust inspection run
during the image build.

Each probe and launcher invocation also mounts the checked-in CI defaults
read-only over the production INI. These preserve the original setup-php
f3e473d1 `php.ini`/`jit.ini` values: UTC, `memory_limit=-1`, enabled OPcache,
256M JIT buffer and mode1235. CLI OPcache is not enabled by this file. The
container memory cap still bounds the process. A plain production128M limit
fails the PHP8.1 suite; the original hosted caller uses more than128MiB and
passes with its existing unlimited PHP limit. Runtime files remain unchanged,
so this SDK-specific test profile needs no shared-base rebuild.

The launcher binds the inspected image config ID and selected CA paths. Each
setup runs the actual Composer TLS control: ambient synthetic trust reaches
the local repository; the selected clean bundle refuses before HTTP. Normal
dependency requests still require the reviewed live Composer guard. Runtime
and trust files require SDK-specific recipe review before CI caller admission;
Sample App's policy entry does not authorize this target.

The supported PHP matrix stays unchanged while caller migration and its exact
runtime/CI-defaults recipe are reviewed under shared #297.
