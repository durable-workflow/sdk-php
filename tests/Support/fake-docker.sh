#!/usr/bin/env bash
# Fake Docker CLI for the phase-P controller controls. It models only the
# commands the controller issues, keeps containers/images/tag as files under
# $FAKE_DOCKER_STATE (a container or image file holds its run label), and
# injects failures named in $FAKE_DOCKER_SCENARIO (a comma-separated list).
# It never contacts a real Docker daemon.
set -u
S=$FAKE_DOCKER_STATE
SCENARIO=",${FAKE_DOCKER_SCENARIO:-},"
has() { [[ $SCENARIO == *",$1,"* ]]; }
call=$*
printf '%s\n' "${call//$'\n'/\\n}" >> "$S/calls"
mkdir -p "$S/containers" "$S/images"

BASE='php@sha256:dd0e2b902f1edfb6ca02fbcb391c28b1554c7d35a4e36e5925876b90aa804851'
BASE_ID='sha256:dd0e2b902f1edfb6ca02fbcb391c28b1554c7d35a4e36e5925876b90aa804851'
TAG='sdk-php-runner-php:8.4.27-pcntl-6ec44aaaa1cc'
CONFIG='["PATH=/usr/local/bin"]|["docker-php-entrypoint"]|["php","-a"]|""|""'
LAYERS='l1 l2 l3 l4 l5 l6 l7 l8 l9 l10 '
CONF='c0f1c0f1c0f1c0f1c0f1c0f1c0f1c0f1c0f1c0f1c0f1c0f1c0f1c0f1c0f1c0f1'
DIFF='C /usr
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

last() { local value; for value in "$@"; do :; done; printf '%s' "$value"; }
option() { local key=$1; shift; while [ "$#" -gt 0 ]; do [ "$1" = "$key" ] && { printf '%s' "$2"; return; }; shift; done; }
run_label() { local previous=''; for value in "$@"; do [ "$previous" = --label ] && [[ $value == durable-workflow.phase-p-run=* ]] && printf '%s' "${value#*=}"; previous=$value; done; }
missing() { echo "Error response from daemon: No such $1: $2" >&2; exit 1; }
daemon_error() { echo 'Cannot connect to the Docker daemon at unix:///var/run/docker.sock.' >&2; exit 1; }
config_json() { printf '{"Env":[],"Labels":{"durable-workflow.owner":"sdk-php","durable-workflow.phase-p-run":"%s"}}\n' "$1"; }

pulls_never() { local value; for value in "$@"; do [ "$value" = --pull=never ] && return 0; done; return 1; }
if { [ "$1" = create ] || [ "$1" = run ]; } && ! pulls_never "$@"; then
    echo "fake docker: $1 without --pull=never could pull an image" >&2
    : > "$S/pull_possible"
    exit 64
fi

case "$1 ${2:-}" in
'image inspect')
    format=$(option --format "$@"); ref=$(last "$@")
    if [ "$ref" = "$BASE" ]; then
        case $format in
        '{{.Id}}') has base_mismatch && echo sha256:0000 || echo "$BASE_ID" ;;
        *Config.Env*) echo "$CONFIG" ;;
        *RootFS*) echo "$LAYERS" ;;
        esac
        exit 0
    fi
    if has image_probe_error && [ -e "$S/rmi_called" ]; then daemon_error; fi
    if [ "$ref" = "$TAG" ]; then [ -f "$S/tag" ] || missing image "$ref"; ref=$(cat "$S/tag"); fi
    [ -f "$S/images/${ref#sha256:}" ] || missing image "$ref"
    run=$(cat "$S/images/${ref#sha256:}")
    case $format in
    '{{.Id}}') echo "$ref" ;;
    '{{json .Config}}') config_json "$run" ;;
    *Config.Env*) has config_differs && echo "${CONFIG/php/sh}" || echo "$CONFIG" ;;
    *Labels*) has labels_wrong && echo '3 sdk-php x' || echo "2 sdk-php $run" ;;
    *RootFS*) has layers_wrong && echo "${LAYERS}l11 l12 " || echo "${LAYERS}l11 " ;;
    esac
    ;;
'container inspect')
    if has probe_error; then daemon_error; fi
    if has probe_error_after_rm && [ -e "$S/rm_called" ]; then daemon_error; fi
    name=$(last "$@"); name=${name#id-}
    [ -f "$S/containers/$name" ] || missing container "$name"
    case $(option --format "$@") in
    '{{.Id}}') echo "id-$name" ;;
    '{{json .Config}}') config_json "$(cat "$S/containers/$name")" ;;
    *State*) has size_over && echo '0 false 2000000' || echo '0 false 147456' ;;
    esac
    ;;
'create --name')
    name=$3
    last "$@" > "$S/create_script"
    has create_fail && exit 1
    if has create_foreign; then echo foreign > "$S/containers/$name"; exit 1; fi
    run_label "$@" > "$S/containers/$name"
    has create_partial && exit 1
    echo "id-$name"
    ;;
'start -a')
    has start_fail && { echo 'build failed' >&2; exit 2; }
    has start_timeout && sleep 30
    has start_flood && head -c 2000000 /dev/zero
    echo 'Build complete.'
    ;;
'kill '*) echo killed >> "$S/kills" ;;
'diff '*) echo "$DIFF"; has diff_extra && echo 'A /etc/extra' ;;
'commit '*)
    name=$(last "${@:1:$#-1}")
    has commit_fail && exit 1
    id=$(head -c 32 /dev/urandom | sha256sum | cut -d' ' -f1)
    cp "$S/containers/$name" "$S/images/$id"
    echo "sha256:$id" > "$S/tag"
    has commit_partial && exit 1
    echo "sha256:$id"
    ;;
'run --rm')
    image=''
    for value in "$@"; do [[ $value == sha256:* || $value == php@* ]] && image=$value; done
    verify=$(option --name "$@")
    if [ "$image" = "$BASE" ]; then
        # A partially failed --rm run can leave its named container behind.
        if has base_check_partial; then run_label "$@" > "$S/containers/$verify"; exit 1; fi
        echo "$CONF"; exit 0
    fi
    if has image_check_partial; then run_label "$@" > "$S/containers/$verify"; exit 1; fi
    so=caeb0d94d6b8b1853aed199acd575dac92d1bb74e6b3de2cfba56befdcdfa8de
    has check_wrong && so=0000000000000000000000000000000000000000000000000000000000000000
    printf '%s\n%s\n1\n%s\n7\n' "$so" 93099d97d237257a925ee62ecf79886c2afa5bfc6731006ac631f2c622db7677 "$CONF"
    ;;
'rm -f')
    : > "$S/rm_called"
    has rm_fail && exit 1
    rm -f "$S/containers/${3#id-}"
    ;;
'rmi -f')
    : > "$S/rmi_called"
    has rmi_fail && exit 1
    rm -f "$S/images/${3#sha256:}"
    [ -f "$S/tag" ] && [ "$(cat "$S/tag")" = "$3" ] && rm -f "$S/tag"
    ;;
'images -q')
    has images_error && daemon_error
    run=$(option --filter "$@"); run=${run#label=durable-workflow.phase-p-run=}
    for file in "$S/images"/*; do
        [ -f "$file" ] && [ "$(cat "$file")" = "$run" ] && echo "sha256:$(basename "$file")"
    done
    ;;
*) echo "fake docker: unsupported $*" >&2; exit 64 ;;
esac
exit 0
