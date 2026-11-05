FROM composer@sha256:af98f42dfff7c68ba8d53c2164fd9fde1087b7d449514baa38c418b1f6bc4bac AS composer

FROM php@sha256:dd334c86ae15a5a77e77ea1cdcbad84a86f891f46a760bfa797a5a8873b7ad4a
LABEL org.opencontainers.image.source="https://github.com/durable-workflow/sdk-php"
COPY require-fixed-apt.sh /usr/local/lib/durable-workflow/require-fixed-apt.sh
RUN /usr/bin/env -i PATH=/usr/bin:/bin LC_ALL=C /bin/sh /usr/local/lib/durable-workflow/require-fixed-apt.sh \
    && docker-php-ext-install -j1 bcmath opcache pcntl \
    && groupmod --gid 1000 www-data \
    && usermod --uid 1000 --gid 1000 www-data \
    && mkdir -p /app \
    && chown 1000:1000 /app \
    && docker-php-ext-install -j1 sockets \
    && apt-get update \
    && apt-get install -y --no-install-recommends unzip \
    && rm -rf /var/lib/apt/lists/*
COPY --from=composer /usr/bin/composer /usr/bin/composer
COPY opcache.ini /usr/local/etc/php/conf.d/99-qualification.ini
WORKDIR /app
USER 1000:1000
