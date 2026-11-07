# Runner PHP image for the installed completion-poll matrix: the cached official
# PHP 8.4.27 CLI image plus the pcntl extension, compiled from the source tarball
# that image already contains. Build with no network: no package, PHAR or source
# download. See completion-polls-execution-packet.md, phase P.
FROM php@sha256:dd0e2b902f1edfb6ca02fbcb391c28b1554c7d35a4e36e5925876b90aa804851

RUN set -eu; \
    test "$PHP_VERSION" = 8.4.27; \
    test "$(sha256sum /usr/src/php.tar.xz | cut -d' ' -f1)" = "$PHP_SHA256"; \
    test "$PHP_SHA256" = dcf93923b0e38e0d5252b25b796520b5a47761a08b3488b6d0b5d1115764d3f0; \
    if php -m | grep -qx pcntl; then exit 1; fi; \
    build="$(mktemp -d)"; \
    tar -xJf /usr/src/php.tar.xz -C "$build" "php-$PHP_VERSION/ext/pcntl"; \
    cd "$build/php-$PHP_VERSION/ext/pcntl"; \
    phpize; \
    CFLAGS="$PHP_CFLAGS" CPPFLAGS="$PHP_CPPFLAGS" LDFLAGS="$PHP_LDFLAGS" ./configure; \
    make -j2; \
    install -m 0644 modules/pcntl.so "$(php-config --extension-dir)/pcntl.so"; \
    printf 'extension=pcntl\n' > "$PHP_INI_DIR/conf.d/docker-php-ext-pcntl.ini"; \
    cd /; \
    rm -rf "$build"; \
    php -m | grep -qx pcntl; \
    php -r '$p = pcntl_fork(); if ($p === -1) exit(3); if ($p === 0) exit(7); pcntl_waitpid($p, $s); exit(pcntl_wexitstatus($s) === 7 ? 0 : 4);'
