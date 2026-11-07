#!/usr/bin/env bash
# Phase A: acquire the four runtime images for the installed completion-poll
# matrix by exact linux/amd64 manifest digest, with a total wall bound, no
# controller retry, and cleanup of everything this run pulled on failure.
# See completion-polls-execution-packet.md, phase A.
#
# Usage: acquire-cell-images.sh [--metadata-only] <work-dir>
#   --metadata-only  read and verify the registry manifests; pull nothing.
#   <work-dir>       an empty owner directory for markers and capped output.
#
# Exit status:
#   0  every image is present with its exact manifest; receipt JSON on stdout
#   2  refused before any pull (no effect)
#   1  failed after a pull started; every image this run pulled was removed
#   3  failed and a pulled image could not be proven removed (residue)
#
# Test-only inputs: PHASE_A_DOCKER (Docker CLI path) and PHASE_A_TOTAL_SECONDS
# (may only lower the 300-second total).
set -uo pipefail
umask 077

# name | pull reference (exact platform manifest) | config digest | index digest
readonly IMAGES='server-3.2.0|ghcr.io/durable-workflow/server@sha256:2a1c9659d6f00a8c891e901835054c12cdc913a91167717d9d508c3ca1a95653|sha256:3979dbc8061da903edb50eb7b03b90051c2ba435c2defaca1f8c40c5318763b2|sha256:6916f6f3bb1ee58d071f10f888068fd321cd1576fb70dc97ce8e9e61da493d62
server-3.0.5|ghcr.io/durable-workflow/server@sha256:2659280f3ad58d1812ce00f44a927beed9781237143b8487ef12e1301eed551e|sha256:0abb6928655d3a56e3ea9fb061f7edefd479a731eade94a30346dde9f7634800|sha256:3cb6e5bf8ec766ac2abd933369b40a77af62794b6d7b921ce21d3fb263e88b72
mysql-8.0.46|docker.io/library/mysql@sha256:62fb722c78b24245ddff1796a0fcee4a49cc5b87e0aaaf20c92d1da9e0a2497b|sha256:6cd09145362dfe6831b14545de3d5fd6cc75c37cfd6ef8561429c1fc73518b39|
postgres-16.15|docker.io/library/postgres@sha256:43f9275df91e012c3da3d86bea5d7bb5e67a473296cc906c5f9e80b8176b1a37|sha256:ba07bdb15f0f2ac1ba7a88891da731cae65ac36d663fdb7de8f698ea47e0e381|sha256:f9a7b9ebf6f56201f949db745e47bc423c0f0f8294bafe991cc10bf74b029657'
TOTAL_SECONDS=300
readonly SMALL=65536

DOCKER=${PHASE_A_DOCKER:-docker}
WORK=''
OUTCOME='refused'
METADATA_ONLY=0

say() { printf '%s\n' "$*" >&2; }
refuse() { say "phase-a refused: $1"; exit 2; }
fail() { say "phase-a failed: $1"; OUTCOME='failed'; exit 1; }

# bounded <seconds> <max-bytes> <label> <command...>: see build-php-pcntl-image.sh.
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

# probe <ref> <label>: present, absent or unknown. Only an explicit
# "No such image/object" answer is absence.
probe() {
    local status
    bounded 30 "$SMALL" "probe-$2" "$DOCKER" image inspect --format '{{.Id}}' "$1"
    status=$?
    if [ "$status" -eq 0 ] && [ -s "$WORK/probe-$2.out" ]; then
        echo present
    elif [ "$status" -eq 1 ] && grep -qiE '^Error( response from daemon)?: No such (object|image)' "$WORK/probe-$2.err"; then
        echo absent
    else
        echo unknown
    fi
}

remaining() { echo $(( DEADLINE - $(date +%s) )); }

cleanup() {
    local residue=0 name ref
    trap - EXIT
    set +e
    if [ -n "$WORK" ] && [ "$OUTCOME" != 'success' ]; then
        for marker in "$WORK"/marker.pull.*; do
            [ -e "$marker" ] || continue
            name=${marker##*/marker.pull.}
            ref=$(cat "$marker")
            # Only a reference that was absent before this run's pull is owned.
            if [ "$(probe "$ref" "cleanup-$name")" != absent ]; then
                bounded 60 "$SMALL" "cleanup-rm-$name" "$DOCKER" image rm "$ref"
                [ "$(probe "$ref" "cleanup-after-$name")" = absent ] || residue=1
            fi
        done
    fi
    if [ "$residue" -ne 0 ]; then
        say 'phase-a residue: a pulled image could not be proven removed'
        exit 3
    fi
    [ "$OUTCOME" = success ] && exit 0
    [ "$OUTCOME" = refused ] && exit 2
    exit 1
}

# --- preflight: no pull --------------------------------------------------------
if [ -n "${PHASE_A_TOTAL_SECONDS:-}" ]; then
    [[ $PHASE_A_TOTAL_SECONDS =~ ^[1-9][0-9]{0,2}$ ]] && [ "$PHASE_A_TOTAL_SECONDS" -le 300 ] || refuse 'total bound may only be lowered'
    TOTAL_SECONDS=$PHASE_A_TOTAL_SECONDS
fi
if [ "${1:-}" = --metadata-only ]; then METADATA_ONLY=1; shift; fi
[ "$#" -eq 1 ] || refuse 'usage: acquire-cell-images.sh [--metadata-only] <work-dir>'
[ -d "$1" ] && [ ! -L "$1" ] && [ -O "$1" ] || refuse 'work directory must be an owned directory'
[ -z "$(ls -A "$1")" ] || refuse 'work directory must be empty'
WORK=$(cd "$1" && pwd -P)
DEADLINE=$(( $(date +%s) + TOTAL_SECONDS ))
trap cleanup EXIT

total_bytes=0
while IFS='|' read -r name ref config index; do
    digest=${ref#*@}
    # Registry metadata only: the exact platform manifest bytes.
    bounded 30 "$SMALL" "manifest-$name" "$DOCKER" buildx imagetools inspect --raw "$ref" \
        || refuse "$name manifest unreadable"
    [ "sha256:$(sha256sum < "$WORK/manifest-$name.out" | cut -d' ' -f1)" = "$digest" ] \
        || refuse "$name manifest bytes differ from $digest"
    facts=$(python3 -I -c '
import json, sys
m = json.load(sys.stdin)
types = ("application/vnd.oci.image.manifest.v1+json", "application/vnd.docker.distribution.manifest.v2+json")
if m.get("mediaType") not in types or not isinstance(m.get("layers"), list):
    sys.exit(3)
print(m["config"]["digest"], sum(int(layer["size"]) for layer in m["layers"]), len(m["layers"]))
' < "$WORK/manifest-$name.out" 2>/dev/null) || refuse "$name reference is not a single platform image manifest"
    read -r got_config bytes layers <<< "$facts"
    [ "$got_config" = "$config" ] || refuse "$name config $got_config differs from $config"
    total_bytes=$(( total_bytes + bytes ))
    state=$(probe "$ref" "pre-$name")
    [ "$state" = unknown ] && refuse "$name local state unknown"
    printf '%s|%s|%s|%s|%s|%s|%s\n' "$name" "$ref" "$config" "$index" "$bytes" "$layers" "$state" >> "$WORK/plan"
done <<< "$IMAGES"

bounded 30 "$SMALL" docker-root "$DOCKER" info --format '{{.DockerRootDir}}' || refuse 'Docker root unreadable'
free_bytes=$(df --output=avail -B1 "$(tr -d '\n' < "$WORK/docker-root.out")" 2>/dev/null | tail -1 | tr -d ' ')
[[ $free_bytes =~ ^[0-9]+$ ]] || refuse 'free Docker storage unreadable'
[ "$free_bytes" -ge $(( total_bytes * 3 + 2147483648 )) ] || refuse "free Docker storage $free_bytes below 3 x $total_bytes compressed + 2 GiB"

if [ "$METADATA_ONLY" -eq 1 ]; then
    OUTCOME='success'
    printf '{"schema":"durable-workflow.sdk-php-phase-a-plan/v1","compressed_bytes":%s,"free_bytes":%s,"images":[' "$total_bytes" "$free_bytes"
    sep=''
    while IFS='|' read -r name ref config index bytes layers state; do
        printf '%s{"name":"%s","ref":"%s","config":"%s","index":"%s","compressed_bytes":%s,"layers":%s,"local":"%s"}' \
            "$sep" "$name" "$ref" "$config" "$index" "$bytes" "$layers" "$state"
        sep=','
    done < "$WORK/plan"
    printf ']}\n'
    exit 0
fi

# --- acquisition: one pull per absent image, no retry --------------------------
OUTCOME='failed'
while IFS='|' read -r name ref config index bytes layers state; do
    [ "$state" = present ] && continue
    left=$(remaining)
    [ "$left" -gt 5 ] || fail 'total wall bound exhausted before every pull'
    printf '%s' "$ref" > "$WORK/marker.pull.$name"
    bounded "$left" "$SMALL" "pull-$name" "$DOCKER" pull --quiet --platform linux/amd64 "$ref" \
        || fail "$name pull did not complete"
done < "$WORK/plan"

# --- verification: local identity is the exact manifest -----------------------
while IFS='|' read -r name ref config index bytes layers state; do
    digest=${ref#*@}
    bounded 30 "$SMALL" "local-$name" "$DOCKER" image inspect --format \
        '{{.Id}} {{.Descriptor.digest}} {{.Architecture}} {{.Os}} {{len .RootFS.Layers}}' "$ref" \
        || fail "$name local identity unreadable"
    [ "$(cat "$WORK/local-$name.out")" = "$digest $digest amd64 linux $layers" ] \
        || fail "$name local identity differs: $(cat "$WORK/local-$name.out")"
done < "$WORK/plan"

OUTCOME='success'
printf '{"schema":"durable-workflow.sdk-php-phase-a-receipt/v1","compressed_bytes":%s,"images":[' "$total_bytes"
sep=''
while IFS='|' read -r name ref config index bytes layers state; do
    if [ "$state" = present ]; then how=pre-existing; else how=pulled; fi
    printf '%s{"name":"%s","ref":"%s","manifest":"%s","config":"%s","index":"%s","layers":%s,"acquired":"%s"}' \
        "$sep" "$name" "$ref" "${ref#*@}" "$config" "$index" "$layers" "$how"
    sep=','
done < "$WORK/plan"
printf ']}\n'
