"""Modern installed PHP command-key cases on one already-owned runtime."""

import argparse
import concurrent.futures
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import stat
import subprocess
import threading
import time
import types
import urllib.request


HERE = Path(__file__).resolve().parent
INPUTS = ("command-key-client.php", "command-key-supply.json", "verify_command_key_supply.py", "command_key_cohort.py")
DOCKER_CONFIG = None


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def capture_inputs(commit):
    require(re.fullmatch("[0-9a-f]{40}", commit or "") is not None, "An exact reviewed runner commit is required")
    repository = HERE.parents[1]
    result = {}
    for name in INPUTS:
        path = HERE / name
        relative = path.relative_to(repository).as_posix()
        original = subprocess.check_output(["git", "--no-replace-objects", "-C", str(repository),
                                            "show", commit + ":" + relative], timeout=10)
        require(path.read_bytes() == original, "Runner source differs from the selected commit")
        result[name] = original
    return result


def module_from_bytes(name, raw):
    module = types.ModuleType(name)
    module.__file__ = str(HERE / (name + ".py"))
    exec(compile(raw, module.__file__, "exec"), module.__dict__)
    return module


def docker_environment():
    require(DOCKER_CONFIG is not None, "An explicit empty Docker configuration is required")
    private_state(DOCKER_CONFIG)
    require(not list(DOCKER_CONFIG.iterdir()), "Docker configuration must remain empty")
    return {"PATH": "/usr/bin:/bin", "DOCKER_CONFIG": str(DOCKER_CONFIG),
            "DOCKER_HOST": "unix:///var/run/docker.sock"}


def exact_missing(process, identity):
    return (process.returncode == 1 and process.stdout == "[]\n"
            and process.stderr == "Error: No such object: " + identity + "\n")


def inspect_runtime(identity, allow_missing=False):
    process = subprocess.run(["docker", "inspect", identity], capture_output=True, text=True, timeout=15,
                             env=docker_environment())
    if allow_missing and exact_missing(process, identity):
        return None
    require(process.returncode == 0, "The captured runtime cannot be inspected")
    records = json.loads(process.stdout)
    require(isinstance(records, list) and len(records) == 1, "The captured runtime is ambiguous")
    return records[0]


def runtime_origin(identity, owner, supply):
    require(re.fullmatch("[0-9a-f]{64}", identity) is not None, "A captured full runtime ID is required")
    require(re.fullmatch("[0-9]+-[0-9]+", owner) is not None, "The workflow run owner is required")
    record = inspect_runtime(identity)
    require(record["Id"] == identity and record["State"]["Running"], "The captured runtime is not running")
    require(record["Config"]["Labels"].get("durable-workflow.php-command-keys") == owner
            and record["Config"]["Labels"].get("org.opencontainers.image.revision") == supply["server_source"],
            "The runtime belongs to another run")
    require(record["Image"] == supply["server_config"], "The runtime image config differs")
    host = record["HostConfig"]
    require(host["ReadonlyRootfs"] and not host["Privileged"] and not host.get("Binds")
            and host["Init"] and host["CapDrop"] == ["ALL"] and not host.get("CapAdd")
            and "no-new-privileges" in host["SecurityOpt"] and not host.get("Devices")
            and host["NetworkMode"] == "bridge",
            "The runtime filesystem is outside the owned fixture profile")
    require(0 < host["Memory"] <= 1024**3 and 0 < host["NanoCpus"] <= 10**9
            and 0 < host["PidsLimit"] <= 256, "The runtime resource caps differ")
    require(all(mount["Type"] == "tmpfs" for mount in record["Mounts"]), "Only private tmpfs mounts are allowed")
    require(set(host["Tmpfs"]) == {"/tmp", "/app/database", "/app/storage"}, "Unexpected runtime writable paths")
    require(set(record["NetworkSettings"]["Ports"]) == {"8080/tcp"}, "Unexpected published runtime ports")
    ports = record["NetworkSettings"]["Ports"].get("8080/tcp")
    require(isinstance(ports, list) and len(ports) == 1 and ports[0]["HostIp"] == "127.0.0.1",
            "The runtime must have one numeric loopback binding")
    port = ports[0]["HostPort"]
    require(re.fullmatch("[1-9][0-9]{0,4}", port) is not None and int(port) <= 65535,
            "The runtime port is invalid")
    return "http://127.0.0.1:" + port


def remove_runtime(identity, owner):
    record = inspect_runtime(identity, allow_missing=True)
    if record is None:
        return
    require(record["Id"] == identity
            and record["Config"]["Labels"].get("durable-workflow.php-command-keys") == owner,
            "Runtime ownership changed; preserve the replacement")
    subprocess.run(["docker", "rm", "--force", "--volumes", identity], check=True,
                   stdout=subprocess.DEVNULL, timeout=30, env=docker_environment())
    missing = subprocess.run(["docker", "inspect", identity], capture_output=True, text=True, timeout=15,
                             env=docker_environment())
    require(exact_missing(missing, identity),
            "Exact runtime absence was not established")


def private_state(directory):
    metadata = directory.lstat()
    require(directory.resolve() == directory and stat.S_ISDIR(metadata.st_mode)
            and metadata.st_uid == os.getuid() and stat.S_IMODE(metadata.st_mode) == 0o700,
            "Runtime state must be a canonical private owned directory")
    return directory / "runtime.cid"


def cleanup_state(directory, owner):
    cidfile = private_state(directory)
    if not cidfile.exists() and not cidfile.is_symlink():
        return
    metadata = cidfile.lstat()
    require(stat.S_ISREG(metadata.st_mode) and metadata.st_uid == os.getuid()
            and metadata.st_nlink == 1 and metadata.st_size == 64,
            "Runtime cleanup needs the captured single-link CID file")
    identity = cidfile.read_text()
    require(re.fullmatch("[0-9a-f]{64}", identity) is not None, "Invalid cleanup runtime ID")
    remove_runtime(identity, owner)


@contextmanager
def owned_runtime(owner, supply, token, directory):
    require(re.fullmatch("[0-9]+-[0-9]+", owner) is not None, "The workflow run owner is required")
    image = "durableworkflow/server@" + supply["server_index"]
    subprocess.run(["docker", "pull", "--platform", "linux/amd64", image], check=True,
                   stdout=subprocess.DEVNULL, timeout=180, env=docker_environment())
    raw = subprocess.check_output(["docker", "image", "inspect", image], timeout=15, env=docker_environment())
    selected = json.loads(raw)[0]
    require(selected["Id"] == supply["server_config"] and selected["Os"] == "linux"
            and selected["Architecture"] == "amd64"
            and selected["Config"]["Labels"].get("org.opencontainers.image.revision") == supply["server_source"],
            "The pulled immutable runtime identity differs")
    cidfile = private_state(directory)
    require(not list(directory.iterdir()), "Runtime state must start empty")
    try:
        subprocess.run(["docker", "create", "--cidfile", str(cidfile), "--init", "--read-only",
                            "--name", "sdk-php-command-keys-" + owner + "-" + secrets.token_hex(4),
                            "--label", "durable-workflow.php-command-keys=" + owner,
                            "--cpus", "1", "--memory", "1g", "--pids-limit", "256", "--network", "bridge",
                            "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
                            "--tmpfs", "/tmp:mode=1777,size=64m",
                            "--tmpfs", "/app/storage:uid=1000,gid=1000,mode=0700,size=64m",
                            "--tmpfs", "/app/database:uid=1000,gid=1000,mode=0700,size=32m",
                            "--publish", "127.0.0.1::8080", "--env", "DB_CONNECTION=sqlite",
                            "--env", "DW_AUTH_DRIVER=token", "--env", "DW_AUTH_TOKEN=" + token, image],
                           check=True, stdout=subprocess.DEVNULL, timeout=30, env=docker_environment())
        identity = cidfile.read_text()
        require(re.fullmatch("[0-9a-f]{64}", identity) is not None, "Docker did not capture a full runtime ID")
        created = inspect_runtime(identity)
        require(created["Id"] == identity
                and created["Config"]["Labels"].get("durable-workflow.php-command-keys") == owner,
                "The created runtime identity differs")
        subprocess.run(["docker", "start", identity], check=True, stdout=subprocess.DEVNULL, timeout=30,
                       env=docker_environment())
        origin = runtime_origin(identity, owner, supply)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        for _ in range(30):
            try:
                with opener.open(origin + "/api/ready", timeout=2) as response:
                    if response.status == 200:
                        break
            except OSError:
                pass
            time.sleep(1)
        else:
            raise RuntimeError("The selected runtime did not become ready")
        yield identity, origin
    finally:
        # The workflow repeats this cleanup if its outer timeout interrupts us.
        cleanup_state(directory, owner)


class Consumer:
    def __init__(self, consumer, archive, supply, sources, verifier, origin, token, runtime_id, owner):
        self.consumer, self.archive, self.supply = consumer, archive, supply
        self.sources, self.verifier, self.origin, self.token = sources, verifier, origin, token
        self.runtime_id, self.owner = runtime_id, owner
        self.php = shutil.which("php")
        require(self.php is not None, "The qualified PHP executable is required")
        require(isinstance(token, str) and token.startswith("sdk-php-216-"), "A synthetic fixture token is required")
        self.package = verifier.verify(consumer, archive, supply)

    def call(self, packet, origin=None):
        require(runtime_origin(self.runtime_id, self.owner, self.supply) == self.origin,
                "The captured runtime endpoint changed")
        for name, raw in self.sources.items():
            require((HERE / name).read_bytes() == raw, "The captured runner source changed")
        require(self.verifier.verify(self.consumer, self.archive, self.supply) == self.package,
                "The installed package changed before invocation")
        environment = {"PATH": "/usr/bin:/bin", "COMMAND_KEY_RUNTIME_URL": origin or self.origin,
                       "COMMAND_KEY_TOKEN": self.token}
        process = subprocess.run([self.php, str(HERE / "command-key-client.php"),
                                  str(self.consumer / "vendor/autoload.php")], input=json.dumps(packet),
                                 text=True, capture_output=True, env=environment, timeout=20)
        require(process.returncode == 0, "The PHP fixture operation failed")
        require(0 < len(process.stdout) <= 1024**2, "The PHP fixture evidence is empty or oversized")
        result = json.loads(process.stdout)
        require(result.get("sdk_version") == self.supply["sdk_version"]
                and result.get("sdk_reference") == self.supply["sdk_reference"]
                and result.get("server_version") == self.supply["server_version"]
                and result.get("source_fallback") is False, "The API operation used another supply")
        return result

    def answered(self, packet, origin=None):
        response = self.call(packet, origin)
        require(response.get("outcome") == "answered", "The API operation was not answered")
        return response["result"]


def identity(receipt, kind):
    fields = ["command_id", "run_id", "request_id"]
    if kind == "update":
        fields += ["update_id", "accepted_at"]
    require(all(isinstance(receipt.get(field), str) and receipt[field] for field in fields),
            "The admission identity is incomplete")
    require(type(receipt.get("command_sequence")) is int and receipt["command_sequence"] > 0,
            "The command sequence is missing")
    if kind == "signal":
        require(isinstance(receipt.get("signal_id"), str) and receipt["signal_id"], "The signal identity is missing")
        fields += ["signal_id"]
    return {field: receipt[field] for field in fields + ["command_sequence"]}


def retained(receipt, original, kind):
    require(identity(receipt, kind) == identity(original, kind), "A retry changed the original admission identity")
    require(receipt.get("retained") is True and receipt.get("intent_verified") is True,
            "The retry did not retain verified intent")


def four_retries(client, packet, original, kind, origin=None):
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        replies = list(pool.map(lambda _: client.answered(packet, origin), range(4)))
    for reply in replies:
        retained(reply, original, kind)


def conflict(client, packet, original):
    response = client.call(packet)
    require(response.get("outcome") == "error" and response.get("status") == 409
            and response.get("is_request_id_conflict") is True
            and response.get("retained_command_id") == original["command_id"]
            and response.get("transient_connection_failure") is False
            and response.get("transient_upstream_failure") is False, "The changed intent was not a typed conflict")


def bootstrap(client, mode, label):
    workflow = "sdk-php-216-" + label + "-" + secrets.token_hex(5)
    queue = workflow + "-queue"
    client.answered({"operation": "bootstrap", "workflow_id": workflow, "task_queue": queue, "mode": mode})
    description = client.answered({"operation": "describe", "workflow_id": workflow})
    require(isinstance(description.get("run_id"), str) and description["run_id"], "The fixture has no original run")
    return workflow, queue, description["run_id"]


def history(client, workflow, run):
    return client.answered({"operation": "history", "workflow_id": workflow, "run_id": run})


def settle(client, workflow, queue, original_run, mode):
    client.answered({"operation": "signal", "workflow_id": workflow, "name": "release",
                     "request_id": "release", "arguments": []})
    for _ in range(6):
        client.answered({"operation": "drive", "workflow_id": workflow, "task_queue": queue})
        description = client.answered({"operation": "describe", "workflow_id": workflow})
        if mode == "close" and description.get("status") == "completed":
            return description
        if mode == "continue" and description.get("run_id") != original_run:
            return description
    raise RuntimeError("The fixture did not complete its declared transition")


def direct_cell(client, mode):
    workflow, queue, run = bootstrap(client, mode, mode)
    admissions = {}
    for kind in ("signal", "update"):
        packet = {"operation": kind, "workflow_id": workflow, "name": "signal", "arguments": [1],
                  "request_id": "explicit-" + kind}
        original = client.answered(packet)
        identity(original, kind)
        require(original["run_id"] == run and original["request_id"] == packet["request_id"]
                and original.get("retained") is False and original.get("intent_verified") is True,
                "The fresh admission identity or intent marker differs")
        four_retries(client, packet, original, kind)
        before = history(client, workflow, run)
        conflict(client, packet | {"arguments": [True]}, original)
        conflict(client, packet | {"name": "other"}, original)
        conflict(client, packet | {"run_id": "00000000000000000000000000"}, original)
        require(history(client, workflow, run) == before, "A conflict changed durable history")
        admissions[kind] = (packet, original)
    pending = client.call({"operation": "value", "workflow_id": workflow,
                           "update_id": admissions["update"][1]["update_id"]})
    require(pending.get("outcome") == "pending", "The queued update did not preserve typed pending")
    if mode != "open":
        settled = settle(client, workflow, queue, run, mode)
        for kind, (packet, original) in admissions.items():
            four_retries(client, packet, original, kind)
        value = client.answered({"operation": "value", "workflow_id": workflow,
                                 "update_id": admissions["update"][1]["update_id"]})
        require(value.get("completed_null") is True, "The completed update did not preserve null")
        if mode == "continue":
            require(settled["run_id"] != run, "The successor reused the old run")
            for packet, original in admissions.values():
                conflict(client, packet | {"run_id": settled["run_id"]}, original)
    counts = history(client, workflow, run)["event_counts"]
    require(counts.get("UpdateAccepted") == 1 and counts.get("SignalReceived") == (1 if mode == "open" else 2),
            "Retries admitted an extra durable command")
    return {"mode": mode, "four_way_retries": True, "typed_conflicts": True,
            "retained_after_transition": mode != "open", "typed_pending": True,
            "completed_null": mode != "open", "event_counts": counts,
            "workflow_id": workflow, "original_run_id": run,
            "admissions": {kind: identity(receipt, kind) for kind, (_, receipt) in admissions.items()}}


def unicode_cell(client):
    workflow, _, run = bootstrap(client, "open", "unicode")
    for kind in ("signal", "update"):
        packet = {"operation": kind, "workflow_id": workflow, "name": "signal", "arguments": [1], "request_id": "é" * 255}
        original = client.answered(packet)
        require(original.get("request_id") == packet["request_id"], "The Unicode key changed")
        four_retries(client, packet, original, kind)
        before = history(client, workflow, run)
        refused = client.call(packet | {"request_id": "é" * 256})
        require(refused.get("outcome") == "local_invalid_argument", "The oversized character key was submitted")
        require(history(client, workflow, run) == before, "The local refusal changed history")
    return {"unicode_255_characters": True, "unicode_256_local_refusal": True}


def loss_cell(client, module, kind):
    workflow, _, run = bootstrap(client, "open", "loss-" + kind)
    proxy = module.CommittedResponseLoss(client.origin)
    thread = threading.Thread(target=proxy.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    path = "/api/workflows/" + workflow + "/" + kind + "/signal"
    packet = {"operation": kind, "workflow_id": workflow, "name": "signal", "arguments": [1]}
    try:
        proxy.arm("POST", path)
        response = client.call(packet, proxy.url)
        if response.get("outcome") == "error":
            require(response.get("transient_connection_failure") is True, "The lost reply was not a typed transport error")
            key = response.get("request_id")
        else:
            require(response.get("outcome") == "answered", "The lost reply has no recoverable identity")
            key = response["result"].get("request_id")
        require(isinstance(key, str) and re.fullmatch("sdk-php-" + kind + "-[0-9a-f]{32}", key),
                "The generated key was not recoverable")
        first = history(client, workflow, run)
        event_type = "SignalReceived" if kind == "signal" else "UpdateAccepted"
        original = [event for event in first["admissions"] if event["event_type"] == event_type]
        require(len(original) == 1 and isinstance(original[0].get("command_id"), str),
                "The committed original admission identity is unavailable")
        packet["request_id"] = key
        recovered = client.answered(packet, proxy.url)
        require(recovered["command_id"] == original[0]["command_id"] and recovered["run_id"] == run,
                "Recovery did not return the committed original command")
        require(recovered.get("retained") is True and recovered.get("intent_verified") is True
                and recovered.get("request_id") == key, "Recovery did not retain the generated key and intent")
        four_retries(client, packet, recovered, kind, proxy.url)
        require(history(client, workflow, run) == first, "Response-loss retries changed durable history")
        records = [record for record in proxy.records if record["method"] == "POST" and record["path"] == path]
        require(6 <= len(records) <= 9 and sum(record["dropped"] for record in records) == 1,
                "The response-loss observation is incomplete")
        require(all(record["upstream_complete"] and 200 <= record["upstream_status"] < 300 for record in records),
                "An upstream response was not completely consumed")
        require(len({record["request_body_sha256"] for record in records}) == 1, "A retry changed the generated key or command body")
        return {"command_type": kind, "committed_before_loss": True, "original_identity_retained": True,
                "four_way_retries": True, "no_second_admission": True, "requests": records,
                "workflow_id": workflow, "original_run_id": run, "recovered": identity(recovered, kind)}
    finally:
        proxy.shutdown()
        proxy.server_close()
        thread.join(timeout=2)
        require(not thread.is_alive(), "The response-loss fixture did not stop")


def main():
    global DOCKER_CONFIG
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--consumer", type=Path)
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--proxy-source", type=Path)
    parser.add_argument("--runtime-state", type=Path, required=True)
    parser.add_argument("--docker-config", type=Path, required=True)
    parser.add_argument("--runtime-owner", required=True)
    parser.add_argument("--runner-source", required=True)
    parser.add_argument("--cleanup-only", action="store_true")
    options = parser.parse_args()
    DOCKER_CONFIG = options.docker_config
    docker_environment()
    sources = capture_inputs(options.runner_source)
    require(re.fullmatch("[0-9]+-[0-9]+", options.runtime_owner) is not None, "Invalid runtime owner")
    if options.cleanup_only:
        cleanup_state(options.runtime_state, options.runtime_owner)
        print(json.dumps({"runtime_removed": True}))
        return
    require(options.consumer is not None and options.archive is not None and options.proxy_source is not None,
            "The registry consumer, archive and proxy source are required")
    supply = json.loads(sources["command-key-supply.json"])
    proxy_raw = options.proxy_source.read_bytes()
    require(hashlib.sha256(proxy_raw).hexdigest() == supply["loss_proxy_sha256"], "The shared loss fixture source differs")
    verifier = module_from_bytes("verify_command_key_supply", sources["verify_command_key_supply.py"])
    verifier.verify(options.consumer, options.archive, supply)
    proxy = module_from_bytes("committed_response_loss", proxy_raw)
    token = "sdk-php-216-" + secrets.token_hex(24)
    with owned_runtime(options.runtime_owner, supply, token, options.runtime_state) as (runtime_id, origin):
        client = Consumer(options.consumer, options.archive, supply, sources, verifier, origin,
                          token, runtime_id, options.runtime_owner)
        direct = [direct_cell(client, mode) for mode in ("open", "close", "continue")]
        unicode = unicode_cell(client)
        loss = [loss_cell(client, proxy, kind) for kind in ("signal", "update")]
    print(json.dumps({"outcome": "pass", "scope": "modern-direct-and-postcommit-loss-only",
                      "runner_source": options.runner_source,
                      "runner_inputs": {name: hashlib.sha256(raw).hexdigest() for name, raw in sources.items()},
                      "package": client.package, "server_index": supply["server_index"],
                      "server_platform": "linux/amd64", "server_config": supply["server_config"],
                      "direct": direct, "unicode": unicode, "loss": loss, "runtime_removed": True}))


if __name__ == "__main__":
    main()
