#!/usr/bin/env bash
# Fake Docker CLI for the phase-A acquisition controls. Registry manifests are
# the real captured bytes in tests/fixtures/phase-a; local images are files
# under $FAKE_DOCKER_STATE/images named by reference digest. Failures come
# from $FAKE_DOCKER_SCENARIO (comma-separated). No real daemon or registry.
set -u
S=$FAKE_DOCKER_STATE
FIXTURES=$(cd "$(dirname "$0")/../fixtures/phase-a" && pwd)
SCENARIO=",${FAKE_DOCKER_SCENARIO:-},"
has() { [[ $SCENARIO == *",$1,"* ]]; }
printf '%s\n' "$*" >> "$S/calls"
mkdir -p "$S/images"
last() { local value; for value in "$@"; do :; done; printf '%s' "$value"; }
fixture() {
    local digest=${1#*@} file
    for file in "$FIXTURES"/*.json; do
        [ "sha256:$(sha256sum < "$file" | cut -d' ' -f1)" = "$digest" ] && { printf '%s' "$file"; return 0; }
    done
    return 1
}
key() { local digest=${1#*@}; printf '%s' "${digest#sha256:}"; }

case "$1 ${2:-}" in
'buildx imagetools')
    ref=$(last "$@"); file=$(fixture "$ref") || exit 1
    if has manifest_tampered && [[ $ref == *mysql* ]]; then printf ' '; fi
    cat "$file"
    ;;
'image inspect')
    has probe_error && { echo 'Cannot connect to the Docker daemon.' >&2; exit 1; }
    has probe_error_after_rm && [ -e "$S/rm_called" ] && { echo 'Cannot connect to the Docker daemon.' >&2; exit 1; }
    ref=$(last "$@"); k=$(key "$ref")
    [ -f "$S/images/$k" ] || { echo "Error response from daemon: No such image: $ref" >&2; exit 1; }
    case "$*" in
    *Descriptor*)
        layers=$(python3 -I -c 'import json,sys; print(len(json.load(open(sys.argv[1]))["layers"]))' "$(fixture "$ref")")
        has wrong_identity && [[ $ref == *postgres* ]] && layers=99
        echo "sha256:$k sha256:$k amd64 linux $layers" ;;
    *) echo "sha256:$k" ;;
    esac
    ;;
'info --format') echo / ;;
'pull --quiet')
    ref=$(last "$@"); k=$(key "$ref")
    if has pull_partial && [[ $ref == *mysql* ]]; then : > "$S/images/$k"; exit 1; fi
    if has pull_timeout && [[ $ref == *mysql* ]]; then sleep 600; fi
    if has pull_fail && [[ $ref == *postgres* ]]; then exit 1; fi
    : > "$S/images/$k"
    echo "$ref"
    ;;
'image rm')
    : > "$S/rm_called"
    has rm_fail && exit 1
    rm -f "$S/images/$(key "$(last "$@")")"
    ;;
*) echo "fake docker: unsupported $*" >&2; exit 64 ;;
esac
exit 0
