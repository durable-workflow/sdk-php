#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
run=${LOAD_DRAIN_RUN:?Set a unique load-driver test name}
[[ "$run" =~ ^[a-z0-9-]{1,60}$ ]]
node=$(jq -er '.tools.node' inputs.json)
k6=$(jq -er '.tools.k6' inputs.json)
cleanup() {
    docker rm --force "$run-k6" "$run-http" >/dev/null 2>&1 || true
    docker network rm "$run" >/dev/null 2>&1 || true
}
if [[ "${1:-}" == cleanup ]]; then cleanup; exit; fi
report=$(mktemp -d)
trap 'cleanup; rm -r -- "$report"' EXIT
docker network create "$run" >/dev/null
for mode in clean failure; do
    fail=false
    expected_exit=0
    if [[ "$mode" == failure ]]; then fail=true; expected_exit=99; fi
    docker run --detach --name "$run-http" --network "$run" --network-alias mock \
        --user 1000:1000 --cpus .2 --memory 128m --memory-swap 128m \
        --mount "type=bind,source=$PWD,target=/fixture,readonly" \
        -e "FAIL_LAST_CONTEXT=$fail" "$node" node /fixture/load-drain-fixture.mjs >/dev/null
    ready=false
    for attempt in $(seq 1 20); do
        if docker exec "$run-http" node -e 'fetch("http://127.0.0.1:8080/test-state").then(r=>{if(!r.ok)process.exit(1)}).catch(()=>process.exit(1))'; then
            ready=true; break
        fi
        sleep .25
    done
    [[ "$ready" == true ]]
    set +e
    docker run --name "$run-k6" --network "$run" --user 1000:1000 \
        --cpus .3 --memory 256m --memory-swap 256m \
        --mount "type=bind,source=$PWD,target=/fixture,readonly" \
        -e K6_NO_USAGE_REPORT=true -e EXAMPLE_ORIGIN=http://mock:8080 \
        -e DURABLE_WORKFLOW_NAMESPACE=drain-test -e "EXAMPLE_RUN_PREFIX=drain-$mode" \
        -e EXAMPLE_DURATION_SECONDS=5 -e EXAMPLE_MIXED=false -e EXAMPLE_SUMMARY_PATH=/tmp/drain-summary.json \
        --entrypoint k6 "$k6" run --quiet /fixture/load.js > "$report/$mode.log" 2>&1
    result=$?
    set -e
    docker cp "$run-k6:/tmp/drain-summary.json" "$report/$mode.json"
    docker exec "$run-http" node -e 'fetch("http://127.0.0.1:8080/test-state").then(r=>r.text()).then(t=>console.log(t))' > "$report/$mode-state.json"
    if [[ "$result" != "$expected_exit" ]]; then tail -n 12 "$report/$mode.log"; exit 1; fi
    jq -c '{admitted:.metrics.workflow_admitted.values.count,terminal:.metrics.workflow_terminal.values.count,errors:.metrics.run_errors.values.count}' "$report/$mode.json"
    cat "$report/$mode-state.json"
    jq -e --arg mode "$mode" '.metrics.workflow_admitted.values.count==5 and
        .metrics.workflow_terminal.values.count==5 and .metrics.context_completed.values.count==(if $mode=="clean" then 30 else 29 end) and
        .metrics.dropped_iterations.values.count==0 and .metrics.run_errors.values.count==(if $mode=="clean" then 0 else 1 end) and
        .example.original_observer_seconds==35 and .example.outer_drain_seconds==36' "$report/$mode.json" >/dev/null
    jq -e '.admitted==5 and .observed==5 and .contexts==30 and .lateAdmissions==0' "$report/$mode-state.json" >/dev/null
    printf '%s: all five originals observed through the declared drain; k6 exit %s\n' "$mode" "$result"
    docker rm "$run-k6" >/dev/null
    docker stop --time 2 "$run-http" >/dev/null
    docker rm "$run-http" >/dev/null
done
