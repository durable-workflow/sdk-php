#!/usr/bin/env bash
# Phase P outer controller: build the runner PHP image (cached official PHP
# 8.4.27 plus pcntl) in one bounded container, commit it, verify it, and clean
# up on every path. See completion-polls-execution-packet.md, phase P.
#
# Usage: build-php-pcntl-image.sh <php-pcntl-build.sh> <work-dir>
#   <work-dir> is an empty owner directory for markers and capped output.
#
# Exit status:
#   0  success: exactly one image is retained, receipt JSON on stdout
#   2  refused before any Docker write (no effect)
#   1  failed after an effect started; every owned object was removed
#   3  failed and an owned object could not be proven removed (residue)
#
# Test-only inputs: PHASE_P_DOCKER (Docker CLI path) and PHASE_P_START_TIMEOUT
# (seconds, may only lower the 300-second bound).
set -uo pipefail
umask 077

readonly BASE='php@sha256:dd0e2b902f1edfb6ca02fbcb391c28b1554c7d35a4e36e5925876b90aa804851'
readonly BASE_ID='sha256:dd0e2b902f1edfb6ca02fbcb391c28b1554c7d35a4e36e5925876b90aa804851'
readonly SCRIPT_SHA256='6ec44aaaa1cc287b4345286623cd9ffbca8ba4b02e78eff434f67398b07d057b'
readonly NAME='sdk-php-cpq-p-build'
readonly VERIFY_NAME='sdk-php-cpq-p-verify'
readonly TAG='sdk-php-runner-php:8.4.27-pcntl-6ec44aaaa1cc'
readonly SO='/usr/local/lib/php/extensions/no-debug-non-zts-20240924/pcntl.so'
readonly INI='/usr/local/etc/php/conf.d/docker-php-ext-pcntl.ini'
readonly SO_SHA256='caeb0d94d6b8b1853aed199acd575dac92d1bb74e6b3de2cfba56befdcdfa8de'
readonly INI_SHA256='93099d97d237257a925ee62ecf79886c2afa5bfc6731006ac631f2c622db7677'
readonly EXPECTED_DIFF='C /usr
C /usr/local
C /usr/local/etc
C /usr/local/etc/php
C /usr/local/etc/php/conf.d
A /usr/local/etc/php/conf.d/docker-php-ext-pcntl.ini
C /usr/local/lib
C /usr/local/lib/php
C /usr/local/lib/php/extensions
C /usr/local/lib/php/extensions/no-debug-non-zts-20240924
A /usr/local/lib/php/extensions/no-debug-non-zts-20240924/pcntl.so'
readonly MAX_SIZE_RW=1048576
readonly SMALL=65536
readonly LOG=1048576

DOCKER=${PHASE_P_DOCKER:-docker}
START_TIMEOUT=300
WORK=''
RUN=''
STAGE='preflight'
OUTCOME='refused'

say() { printf '%s\n' "$*" >&2; }

refuse() {
    say "phase-p refused: $1"
    exit 2
}

fail() {
    say "phase-p failed at $STAGE: $1"
    OUTCOME='failed'
    exit 1
}

# bounded <seconds> <max-bytes> <label> <command...>
# Runs one command with a wall bound. Its stdout and stderr go to capped files
# in the work directory. Returns the command's status; output over the cap
# returns 125.
bounded() {
    local seconds=$1 max=$2 label=$3 status
    shift 3
    timeout --kill-after=5 "$seconds" "$@" \
        2> >(head -c "$((max + 1))" > "$WORK/$label.err") \
        | head -c "$((max + 1))" > "$WORK/$label.out"
    status=${PIPESTATUS[0]}
    wait 2>/dev/null
    if [ "$(stat -c %s "$WORK/$label.out")" -gt "$max" ] || [ "$(stat -c %s "$WORK/$label.err")" -gt "$max" ]; then
        return 125
    fi
    return "$status"
}

out() { cat "$WORK/$1.out"; }

exists_container() { bounded 30 "$SMALL" "probe-$2" "$DOCKER" container inspect --format '{{.Id}}' "$1"; }
exists_image() { bounded 30 "$SMALL" "probe-$2" "$DOCKER" image inspect --format '{{.Id}}' "$1"; }

cleanup() {
    local residue=0 id
    trap - EXIT
    set +e
    if [ -n "$WORK" ] && [ -e "$WORK/marker.create" ]; then
        for name in "$NAME" "$VERIFY_NAME"; do
            bounded 30 "$SMALL" "cleanup-rm-$name" "$DOCKER" rm -f "$name"
            if exists_container "$name" "cleanup-$name"; then residue=1; fi
        done
    fi
    if [ -n "$WORK" ] && [ -e "$WORK/marker.commit" ] && [ "$OUTCOME" != 'success' ]; then
        # Only images labeled with this run's random identity are owned.
        bounded 30 "$SMALL" cleanup-images "$DOCKER" images -q --no-trunc --filter "label=durable-workflow.phase-p-run=$RUN"
        for id in $(out cleanup-images); do
            bounded 60 "$SMALL" "cleanup-rmi-${id#sha256:}" "$DOCKER" rmi -f "$id"
        done
        bounded 30 "$SMALL" cleanup-images-after "$DOCKER" images -q --no-trunc --filter "label=durable-workflow.phase-p-run=$RUN"
        if [ -s "$WORK/cleanup-images-after.out" ] || [ "$(stat -c %s "$WORK/cleanup-images-after.err")" -gt 0 ]; then residue=1; fi
        if exists_image "$TAG" cleanup-tag; then
            [ "$(out probe-cleanup-tag)" = "$(cat "$WORK/image.id" 2>/dev/null)" ] && residue=1
        fi
    fi
    if [ "$residue" -ne 0 ]; then
        say "phase-p residue: an owned container or image could not be proven removed"
        exit 3
    fi
    if [ "$OUTCOME" = 'success' ]; then exit 0; fi
    if [ "$OUTCOME" = 'refused' ]; then exit 2; fi
    exit 1
}

# --- preflight: no Docker writes -------------------------------------------
[ "$#" -eq 2 ] || refuse 'usage: build-php-pcntl-image.sh <php-pcntl-build.sh> <work-dir>'
if [ -n "${PHASE_P_START_TIMEOUT:-}" ]; then
    [[ $PHASE_P_START_TIMEOUT =~ ^[1-9][0-9]{0,2}$ ]] && [ "$PHASE_P_START_TIMEOUT" -le 300 ] || refuse 'start timeout may only be lowered'
    START_TIMEOUT=$PHASE_P_START_TIMEOUT
fi
[ -f "$1" ] && [ ! -L "$1" ] || refuse 'build script must be a regular file'
SCRIPT=$(head -c 65537 "$1") || refuse 'build script unreadable'
[ "$(head -c 65537 "$1" | sha256sum | cut -d' ' -f1)" = "$SCRIPT_SHA256" ] || refuse 'build script differs from the reviewed bytes'
[ -d "$2" ] && [ ! -L "$2" ] && [ -O "$2" ] || refuse 'work directory must be an owned directory'
[ -z "$(ls -A "$2")" ] || refuse 'work directory must be empty'
WORK=$(cd "$2" && pwd -P)
RUN=$(head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n')
trap cleanup EXIT

bounded 30 "$SMALL" base-id "$DOCKER" image inspect --format '{{.Id}}' "$BASE" || refuse 'base image is not present'
[ "$(out base-id)" = "$BASE_ID" ] || refuse 'base image identity differs'
if exists_container "$NAME" pre-name; then refuse 'build container name already exists'; fi
if exists_container "$VERIFY_NAME" pre-verify; then refuse 'verify container name already exists'; fi
if exists_image "$TAG" pre-tag; then refuse 'image tag already exists'; fi
bounded 30 "$SMALL" base-config "$DOCKER" image inspect --format \
    '{{json .Config.Env}}|{{json .Config.Entrypoint}}|{{json .Config.Cmd}}|{{json .Config.User}}|{{json .Config.WorkingDir}}' "$BASE" \
    || refuse 'base configuration unreadable'
bounded 30 "$SMALL" base-layers "$DOCKER" image inspect --format '{{range .RootFS.Layers}}{{.}} {{end}}' "$BASE" \
    || refuse 'base layers unreadable'
[ "$(wc -w < "$WORK/base-layers.out")" -eq 10 ] || refuse 'base must have ten layers'

# --- build container ---------------------------------------------------------
STAGE='create'
OUTCOME='failed'
: > "$WORK/marker.create"
bounded 60 "$SMALL" create "$DOCKER" create --name "$NAME" \
    --label durable-workflow.owner=sdk-php --label "durable-workflow.phase-p-run=$RUN" \
    --network none --cap-drop ALL --security-opt no-new-privileges \
    --cpus 2 --memory 1g --memory-swap 1g --pids-limit 256 \
    --tmpfs /tmp:rw,exec,nosuid,nodev,size=512m \
    --log-driver json-file --log-opt max-size=1m --log-opt max-file=1 \
    --entrypoint /bin/sh "$BASE" -c "$SCRIPT" || fail 'create'

STAGE='start'
bounded "$START_TIMEOUT" "$LOG" start "$DOCKER" start -a "$NAME"
status=$?
if [ "$status" -ne 0 ]; then
    bounded 30 "$SMALL" kill "$DOCKER" kill "$NAME"
    [ "$status" -eq 124 ] && fail 'build exceeded its wall bound'
    [ "$status" -eq 125 ] && fail 'build output exceeded its bound'
    fail "build exited $status"
fi

STAGE='verify-container'
bounded 30 "$SMALL" state "$DOCKER" container inspect --size --format '{{.State.ExitCode}} {{.State.OOMKilled}} {{.SizeRw}}' "$NAME" || fail 'inspect'
read -r code oom size_rw < "$WORK/state.out"
[ "$code" = 0 ] && [ "$oom" = false ] || fail "container exit $code oom $oom"
[[ $size_rw =~ ^[0-9]+$ ]] && [ "$size_rw" -le "$MAX_SIZE_RW" ] || fail "writable layer $size_rw bytes"
bounded 30 "$SMALL" diff "$DOCKER" diff "$NAME" || fail 'diff'
[ "$(LC_ALL=C sort "$WORK/diff.out")" = "$(printf '%s\n' "$EXPECTED_DIFF" | LC_ALL=C sort)" ] || fail 'container diff is not exactly the two intended files'

# --- commit ------------------------------------------------------------------
STAGE='commit'
: > "$WORK/marker.commit"
bounded 120 "$SMALL" commit "$DOCKER" commit \
    --change 'ENTRYPOINT ["docker-php-entrypoint"]' --change 'CMD ["php","-a"]' \
    "$NAME" "$TAG" || fail 'commit'
out commit | tr -d '\n' > "$WORK/image.id"
IMAGE=$(cat "$WORK/image.id")
[[ $IMAGE =~ ^sha256:[a-f0-9]{64}$ ]] || fail 'commit returned no image identity'

STAGE='verify-image'
bounded 30 "$SMALL" tag-id "$DOCKER" image inspect --format '{{.Id}}' "$TAG" || fail 'tag unreadable'
[ "$(out tag-id)" = "$IMAGE" ] || fail 'tag does not name the committed image'
bounded 30 "$SMALL" image-config "$DOCKER" image inspect --format \
    '{{json .Config.Env}}|{{json .Config.Entrypoint}}|{{json .Config.Cmd}}|{{json .Config.User}}|{{json .Config.WorkingDir}}' "$IMAGE" || fail 'image config'
[ "$(out image-config)" = "$(out base-config)" ] || fail 'image configuration differs from the base'
bounded 30 "$SMALL" image-labels "$DOCKER" image inspect --format '{{len .Config.Labels}} {{index .Config.Labels "durable-workflow.owner"}} {{index .Config.Labels "durable-workflow.phase-p-run"}}' "$IMAGE" || fail 'image labels'
[ "$(out image-labels)" = "2 sdk-php $RUN" ] || fail 'image labels differ'
bounded 30 "$SMALL" image-layers "$DOCKER" image inspect --format '{{range .RootFS.Layers}}{{.}} {{end}}' "$IMAGE" || fail 'image layers'
[ "$(wc -w < "$WORK/image-layers.out")" -eq 11 ] || fail 'image must add exactly one layer'
[ "$(cut -d' ' -f1-10 "$WORK/image-layers.out")" = "$(cut -d' ' -f1-10 "$WORK/base-layers.out")" ] || fail 'image does not extend the base layers'

check='sha256sum '"$SO $INI"' | cut -d" " -f1; php -m | grep -cx pcntl; php-config --configure-options | sha256sum | cut -d" " -f1; php -r '"'"'$p = pcntl_fork(); if ($p === -1) exit(3); if ($p === 0) exit(7); pcntl_waitpid($p, $s); echo pcntl_wexitstatus($s), PHP_EOL;'"'"''
bounded 60 "$SMALL" image-check "$DOCKER" run --rm --name "$VERIFY_NAME" --label "durable-workflow.phase-p-run=$RUN" \
    --network none --read-only --user 1000:1000 --cap-drop ALL --security-opt no-new-privileges \
    --cpus 1 --memory 256m --pids-limit 64 --entrypoint /bin/sh "$IMAGE" -c "$check" || fail 'image check'
bounded 60 "$SMALL" base-check "$DOCKER" run --rm --name "$VERIFY_NAME" \
    --network none --read-only --user 1000:1000 --cap-drop ALL --security-opt no-new-privileges \
    --cpus 1 --memory 256m --pids-limit 64 --entrypoint /bin/sh "$BASE" -c 'php-config --configure-options | sha256sum | cut -d" " -f1' || fail 'base check'
expected_check=$(printf '%s\n%s\n1\n%s\n7' "$SO_SHA256" "$INI_SHA256" "$(out base-check | tr -d '\n')")
[ "$(out image-check)" = "$expected_check" ] || fail 'image check output differs'

# --- success -----------------------------------------------------------------
STAGE='done'
OUTCOME='success'
printf '{"schema":"durable-workflow.sdk-php-phase-p-receipt/v1","image":"%s","tag":"%s","run":"%s","base":"%s","pcntl_so_sha256":"%s","ini_sha256":"%s","size_rw":%s,"layers":%s}\n' \
    "$IMAGE" "$TAG" "$RUN" "$BASE_ID" "$SO_SHA256" "$INI_SHA256" "$size_rw" "$(wc -w < "$WORK/image-layers.out")"
