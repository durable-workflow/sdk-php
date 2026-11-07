#!/bin/sh
# Adds only the pcntl extension to the cached official PHP 8.4.27 CLI image,
# compiled from the source tarball that image already contains. It runs inside
# one bounded, network-none container (packet phase P); nothing is downloaded.
set -eu
test "$PHP_VERSION" = 8.4.27
test "$(sha256sum /usr/src/php.tar.xz | cut -d' ' -f1)" = "$PHP_SHA256"
test "$PHP_SHA256" = dcf93923b0e38e0d5252b25b796520b5a47761a08b3488b6d0b5d1115764d3f0
if php -m | grep -qx pcntl; then exit 1; fi
build="$(mktemp -d)"
tar -xJf /usr/src/php.tar.xz -C "$build" "php-$PHP_VERSION/ext/pcntl"
cd "$build/php-$PHP_VERSION/ext/pcntl"
phpize
CFLAGS="$PHP_CFLAGS" CPPFLAGS="$PHP_CPPFLAGS" LDFLAGS="$PHP_LDFLAGS" ./configure
make -j2
test "$(sha256sum modules/pcntl.so | cut -d' ' -f1)" = caeb0d94d6b8b1853aed199acd575dac92d1bb74e6b3de2cfba56befdcdfa8de
install -m 0644 modules/pcntl.so "$(php-config --extension-dir)/pcntl.so"
printf 'extension=pcntl\n' > "$PHP_INI_DIR/conf.d/docker-php-ext-pcntl.ini"
cd /
rm -rf "$build"
php -m | grep -qx pcntl
php -r '$p = pcntl_fork(); if ($p === -1) exit(3); if ($p === 0) exit(7); pcntl_waitpid($p, $s); exit(pcntl_wexitstatus($s) === 7 ? 0 : 4);'
