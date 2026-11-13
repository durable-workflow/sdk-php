"""Modern installed PHP command-key cases on one already-owned runtime."""

import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import stat
import subprocess
import threading
import time
import types
import urllib.request


HERE = Path(__file__).resolve().parent
INPUTS = ("command-key-client.php", "command-key-supply.json", "verify_command_key_supply.py",
          "command_key_cohort.py", "command_key_containers.py", "command_key_include_closure.py")


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


def fixture_authority(sources):
    return {name: {"mode": 0o644, "bytes": len(sources[name]), "sha256": hashlib.sha256(sources[name]).hexdigest()}
            for name in ("command-key-client.php", "command-key-supply.json")}


def stage_inventory(root, expected, root_identity, closure, supply, approved_fixture):
    require(root.parent == Path("/run") and re.fullmatch(r"sdk-php-command-keys\.[A-Za-z0-9]{6}", root.name),
            "The include closure must use its root-controlled staging location")
    metadata = root.lstat()
    require(root.resolve() == root and stat.S_ISDIR(metadata.st_mode) and metadata.st_uid == 0,
            "The include staging root is not root controlled")
    require(root_identity == str(metadata.st_dev) + ":" + str(metadata.st_ino), "The include staging inode differs")
    result = closure.inventory(root, 0, sealed=True)
    closure.verify_sealed_sdk(result, supply["sdk_archive_members_sha256"])
    closure.bind_members(result, "fixture/", approved_fixture, sealed=True)
    require(result["sha256"] == expected, "The complete include inventory differs")
    return result


def confined(record, configuration):
    require(record["State"]["Running"] and record["Image"] == configuration,
            "The fixture is not running the selected image configuration")
    host = record["HostConfig"]
    require(host["ReadonlyRootfs"] and not host["Privileged"] and not host.get("Binds")
            and host["Init"] and host["CapDrop"] == ["ALL"] and not host.get("CapAdd")
            and "no-new-privileges" in host["SecurityOpt"] and not host.get("Devices"),
            "The fixture privileges differ")
    require(0 < host["Memory"] <= 1024**3 and 0 < host["NanoCpus"] <= 10**9
            and 0 < host["PidsLimit"] <= 256, "The fixture resource caps differ")


def runtime_origin(resources, identity, supply):
    record = resources.owned(identity, "server")
    confined(record, supply["server_config"])
    require(record["Config"]["Labels"].get("org.opencontainers.image.revision") == supply["server_source"],
            "The native source identity differs")
    require(record["HostConfig"]["NetworkMode"] == "bridge", "The native network profile differs")
    require(all(mount["Type"] == "tmpfs" for mount in record["Mounts"]), "Unexpected native bind or volume")
    require(set(record["HostConfig"]["Tmpfs"]) == {"/tmp", "/app/database", "/app/storage"},
            "Unexpected native writable paths")
    require({mount["Destination"] for mount in record["Mounts"]} == {"/tmp", "/app/database", "/app/storage"},
            "The native writable mount destinations differ")
    require(set(record["NetworkSettings"]["Ports"]) == {"8080/tcp"}, "Unexpected native published ports")
    ports = record["NetworkSettings"]["Ports"]["8080/tcp"]
    require(isinstance(ports, list) and len(ports) == 1 and ports[0]["HostIp"] == "127.0.0.1",
            "The native fixture must have one numeric loopback binding")
    port = ports[0]["HostPort"]
    require(re.fullmatch(r"[1-9][0-9]{0,4}", port) and int(port) <= 65535, "Invalid fixture port")
    return "http://127.0.0.1:" + port


def consumer_record(resources, identity, supply, root):
    record = resources.owned(identity, "consumer")
    confined(record, supply["php_config"])
    host = record["HostConfig"]
    require(host["NetworkMode"] == "host" and not record["NetworkSettings"]["Ports"],
            "The PHP fixture network profile differs")
    require(record["Config"]["User"] == str(os.getuid()) + ":" + str(os.getgid()) and os.getuid() != 0,
            "The PHP fixture must use the unprivileged caller identity")
    require(set(host["Tmpfs"]) == {"/tmp"}, "Unexpected PHP writable paths")
    binds = [mount for mount in record["Mounts"] if mount["Type"] == "bind"]
    require(len(binds) == 1 and binds[0]["Source"] == str(root)
            and binds[0]["Destination"] == "/closure" and binds[0]["RW"] is False,
            "The complete include closure must be mounted read-only")
    require(all(mount["Type"] == "tmpfs" or mount in binds for mount in record["Mounts"]),
            "Unexpected PHP volume or mount")
    require({mount["Destination"] for mount in record["Mounts"]} == {"/closure", "/tmp"},
            "Unexpected PHP writable mount destinations")
    return record


def publish_outputs(values):
    output = os.environ.get("GITHUB_OUTPUT")
    require(isinstance(output, str) and output, "The trusted workflow output file is required")
    descriptor = os.open(output, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(descriptor)
        require(stat.S_ISREG(metadata.st_mode) and metadata.st_uid == os.getuid() and metadata.st_nlink == 1,
                "The workflow output file identity differs")
        raw = "".join(key + "=" + value + "\n" for key, value in values.items()).encode()
        offset = 0
        while offset < len(raw):
            written = os.write(descriptor, raw[offset:])
            require(written > 0, "The workflow outputs could not be written")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def prepare_containers(resources, supply, token, root):
    try:
        server_image = "durableworkflow/server@" + supply["server_index"]
        native = resources.pull(server_image, supply["server_config"])
        require(native["Config"]["Labels"].get("org.opencontainers.image.revision") == supply["server_source"],
                "The selected native source differs")
        server = resources.create("server", ["--network", "bridge", "--tmpfs", "/tmp:mode=1777,size=64m",
                                  "--tmpfs", "/app/storage:uid=1000,gid=1000,mode=0700,size=64m",
                                  "--tmpfs", "/app/database:uid=1000,gid=1000,mode=0700,size=32m",
                                  "--publish", "127.0.0.1::8080", "--env", "DB_CONNECTION=sqlite",
                                  "--env", "DW_AUTH_DRIVER=token", "--env", "DW_AUTH_TOKEN=" + token, server_image])
        resources.start(server, "server")
        origin = runtime_origin(resources, server, supply)
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
            raise RuntimeError("The selected native fixture did not become ready")
        php_image = "php@" + supply["php_index"]
        php = resources.pull(php_image, supply["php_config"])
        require("PHP_VERSION=" + supply["php_version"] in php["Config"]["Env"], "The PHP image version differs")
        user = str(os.getuid()) + ":" + str(os.getgid())
        require(os.getuid() > 0, "The PHP fixture cannot run as root")
        consumer = resources.create("consumer", ["--network", "host", "--user", user,
                                    "--workdir", "/closure/consumer", "--tmpfs", "/tmp:mode=1777,size=128m",
                                    "--mount", "type=bind,src=" + str(root) + ",dst=/closure,readonly", php_image,
                                    "php", "-c", "/usr/local/etc/php/php.ini-production", "-r", "sleep(900);"])
        resources.start(consumer, "consumer")
        consumer_record(resources, consumer, supply, root)
        probe = resources.command(["exec", consumer, "php", "-c", "/usr/local/etc/php/php.ini-production", "-r",
                                   'echo json_encode(["version"=>PHP_VERSION,"ini"=>php_ini_loaded_file(),'
                                   '"coverage"=>extension_loaded("xdebug")||extension_loaded("pcov")]);'])
        require(probe.returncode == 0, "The PHP profile probe failed")
        observed = json.loads(probe.stdout)
        require(observed == {"version": supply["php_version"], "ini": "/usr/local/etc/php/php.ini-production",
                             "coverage": False}, "The PHP production profile differs")
        return server, consumer
    except BaseException:
        resources.remove_all(reversed(list(resources.identities.items())))
        raise


class Consumer:
    def __init__(self, resources, container, runtime_id, supply, sources, origin, token, root, expected, root_identity, closure):
        self.resources, self.container, self.runtime_id = resources, container, runtime_id
        self.supply, self.sources, self.origin, self.token = supply, sources, origin, token
        self.root, self.expected, self.root_identity, self.closure = root, expected, root_identity, closure
        self.approved_fixture = fixture_authority(sources)
        self.include_inventory = stage_inventory(root, expected, root_identity, closure, supply, self.approved_fixture)

    def call(self, packet, origin=None):
        require(runtime_origin(self.resources, self.runtime_id, self.supply) == self.origin,
                "The captured native endpoint changed")
        consumer_record(self.resources, self.container, self.supply, self.root)
        require(stage_inventory(self.root, self.expected, self.root_identity, self.closure,
                                self.supply, self.approved_fixture) == self.include_inventory,
                "The complete include tree changed before invocation")
        for name, raw in self.sources.items():
            require((HERE / name).read_bytes() == raw, "The captured runner source changed")
        result = self.resources.command(["exec", "--interactive", "--env", "COMMAND_KEY_RUNTIME_URL=" + (origin or self.origin),
                                         "--env", "COMMAND_KEY_TOKEN=" + self.token, self.container, "php", "-c",
                                         "/usr/local/etc/php/php.ini-production", "/closure/fixture/command-key-client.php",
                                         "/closure/consumer/vendor/autoload.php"], input=json.dumps(packet), timeout=20)
        require(result.returncode == 0, "The isolated PHP fixture operation failed")
        require(0 < len(result.stdout) <= 1024**2, "The PHP fixture evidence is empty or oversized")
        require(stage_inventory(self.root, self.expected, self.root_identity, self.closure,
                                self.supply, self.approved_fixture) == self.include_inventory,
                "The complete include tree changed during invocation")
        consumer_record(self.resources, self.container, self.supply, self.root)
        response = json.loads(result.stdout)
        require(response.get("sdk_version") == self.supply["sdk_version"]
                and response.get("sdk_reference") == self.supply["sdk_reference"]
                and response.get("server_version") == self.supply["server_version"]
                and response.get("php_version") == self.supply["php_version"]
                and response.get("source_fallback") is False, "The API operation used another supply")
        return response

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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("stage", "prepare", "cohort"), required=True)
    parser.add_argument("--consumer", type=Path)
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--proxy-source", type=Path)
    parser.add_argument("--stage-root", type=Path, required=True)
    parser.add_argument("--include-sha256")
    parser.add_argument("--root-identity")
    parser.add_argument("--runtime-state", type=Path, required=True)
    parser.add_argument("--docker-config", type=Path, required=True)
    parser.add_argument("--runtime-owner", required=True)
    parser.add_argument("--runner-source", required=True)
    parser.add_argument("--server-id")
    parser.add_argument("--consumer-id")
    parser.add_argument("--nonce")
    options = parser.parse_args()
    require(re.fullmatch(r"[0-9]+-[0-9]+", options.runtime_owner), "Invalid fixture run owner")
    sources = capture_inputs(options.runner_source)
    supply = json.loads(sources["command-key-supply.json"])
    approved_fixture = fixture_authority(sources)
    closure = module_from_bytes("command_key_include_closure", sources["command_key_include_closure.py"])
    if options.phase == "stage":
        require(options.consumer is not None and options.archive is not None,
                "The registry consumer and approved archive are required")
        verifier = module_from_bytes("verify_command_key_supply", sources["verify_command_key_supply.py"])
        package, approved_sdk = verifier.verify_with_members(options.consumer, options.archive, supply)
        fixture = options.stage_root.parent / "captured-fixture"
        fixture.mkdir(mode=0o700)
        for name in ("command-key-client.php", "command-key-supply.json"):
            descriptor = os.open(fixture / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o400)
            with os.fdopen(descriptor, "wb") as output:
                output.write(sources[name])
        include = closure.stage(options.consumer, fixture, options.stage_root, approved_sdk,
                                supply["sdk_archive_members_sha256"], approved_fixture)
        print(json.dumps({"include_sha256": include["sha256"], "members": len(include["entries"]),
                          "bytes": include["bytes"], "package": package, "inspection_only": True}))
        return
    require(isinstance(options.include_sha256, str) and re.fullmatch(r"[0-9a-f]{64}", options.include_sha256),
            "The captured include digest is required")
    include = stage_inventory(options.stage_root, options.include_sha256, options.root_identity,
                              closure, supply, approved_fixture)
    containers = module_from_bytes("command_key_containers", sources["command_key_containers.py"])
    nonce = secrets.token_hex(16) if options.phase == "prepare" else options.nonce
    created = (lambda role, identity: publish_outputs({role + "_id": identity})) if options.phase == "prepare" else None
    resources = containers.Containers(options.docker_config, options.runtime_state, options.runtime_owner, nonce, created)
    try:
        if options.phase == "prepare":
            try:
                token = "sdk-php-216-" + secrets.token_hex(24)
                publish_outputs({"nonce": nonce, "token": token})
                server, consumer = prepare_containers(resources, supply, token, options.stage_root)
                stage_inventory(options.stage_root, options.include_sha256, options.root_identity,
                                closure, supply, approved_fixture)
                # This step ends before any package code executes. GitHub retains
                # these outputs independently of the mutable checkout/state.
                print(json.dumps({"prepared": True, "package_executed": False,
                                  "server_config": supply["server_config"], "php_config": supply["php_config"]}))
                return
            except BaseException:
                resources.remove_all(reversed(list(resources.identities.items())))
                raise
        token = os.environ.get("COMMAND_KEY_TOKEN")
        require(isinstance(token, str) and re.fullmatch(r"sdk-php-216-[0-9a-f]{48}", token),
                "The prepared synthetic credential is required")
        require(options.proxy_source is not None, "The accepted response-loss fixture is required")
        proxy_raw = options.proxy_source.read_bytes()
        require(hashlib.sha256(proxy_raw).hexdigest() == supply["loss_proxy_sha256"], "The accepted loss fixture differs")
        proxy = module_from_bytes("committed_response_loss", proxy_raw)
        try:
            origin = runtime_origin(resources, options.server_id, supply)
            consumer_record(resources, options.consumer_id, supply, options.stage_root)
            client = Consumer(resources, options.consumer_id, options.server_id, supply, sources, origin, token,
                              options.stage_root, options.include_sha256, options.root_identity, closure)
            direct = [direct_cell(client, mode) for mode in ("open", "close", "continue")]
            unicode = unicode_cell(client)
            loss = [loss_cell(client, proxy, kind) for kind in ("signal", "update")]
        finally:
            resources.remove_all([("consumer", options.consumer_id), ("server", options.server_id)])
        print(json.dumps({"outcome": "pass", "scope": "modern-direct-and-postcommit-loss-only",
                          "runner_source": options.runner_source,
                          "runner_inputs": {name: hashlib.sha256(raw).hexdigest() for name, raw in sources.items()},
                          "sdk_version": supply["sdk_version"], "sdk_reference": supply["sdk_reference"],
                          "include_sha256": include["sha256"], "include_members": len(include["entries"]),
                          "server_index": supply["server_index"], "server_platform": "linux/amd64",
                          "server_config": supply["server_config"], "php_index": supply["php_index"],
                          "php_config": supply["php_config"], "php_version": supply["php_version"],
                          "direct": direct, "unicode": unicode, "loss": loss, "runtime_removed": True}))
    finally:
        resources.close()


if __name__ == "__main__":
    main()
