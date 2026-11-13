"""Inert closure, workflow-bootstrap and receipt controls; no runtime calls."""

from copy import deepcopy
import importlib.util
import json
import os
from pathlib import Path
import stat
import textwrap
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch


SOURCE = Path(__file__).resolve().parents[1] / "smoke/command_key_cohort.py"
SPEC = importlib.util.spec_from_file_location("command_key_cohort", SOURCE)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
SUPPLY = json.loads(SOURCE.with_name("command-key-supply.json").read_text())
WORKFLOW = SOURCE.parents[2] / ".github/workflows/published-command-keys.yml"
IDENTITY = "a" * 64
ROOT = Path("/run/sdk-php-command-keys.abcdef")


def runtime():
    return {"Id": IDENTITY, "Image": SUPPLY["server_config"], "State": {"Running": True},
            "Config": {"Labels": {"org.opencontainers.image.revision": SUPPLY["server_source"]}},
            "HostConfig": {"ReadonlyRootfs": True, "Privileged": False, "Binds": None,
                           "Init": True, "CapDrop": ["ALL"], "CapAdd": None,
                           "SecurityOpt": ["no-new-privileges"], "Devices": [], "NetworkMode": "bridge",
                           "Memory": 1024**3, "NanoCpus": 10**9, "PidsLimit": 256,
                           "Tmpfs": {"/tmp": "size=64m", "/app/database": "size=32m", "/app/storage": "size=64m"}},
            "Mounts": [{"Type": "tmpfs", "Destination": path} for path in ("/tmp", "/app/database", "/app/storage")],
            "NetworkSettings": {"Ports": {"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": "43123"}]}}}


def consumer():
    record = runtime()
    record["Image"] = SUPPLY["php_config"]
    record["Config"]["User"] = "1001:1001"
    record["HostConfig"].update(NetworkMode="host", Tmpfs={"/tmp": "size=128m"})
    record["Mounts"] = [{"Type": "bind", "Source": str(ROOT), "Destination": "/closure", "RW": False},
                        {"Type": "tmpfs", "Destination": "/tmp"}]
    record["NetworkSettings"]["Ports"] = None
    return record


class ProvenanceControls(unittest.TestCase):
    def origin(self, record):
        resources = Mock()
        resources.owned.return_value = record
        result = MODULE.runtime_origin(resources, IDENTITY, SUPPLY)
        resources.owned.assert_called_once_with(IDENTITY, "server")
        return result

    def php(self, record):
        resources = Mock()
        resources.owned.return_value = record
        with patch.object(MODULE.os, "getuid", return_value=1001), patch.object(MODULE.os, "getgid", return_value=1001):
            result = MODULE.consumer_record(resources, IDENTITY, SUPPLY, ROOT)
        resources.owned.assert_called_once_with(IDENTITY, "consumer")
        return result

    def test_complete_native_profile_selects_numeric_loopback(self):
        self.assertEqual(self.origin(runtime()), "http://127.0.0.1:43123")

    def test_native_source_configuration_network_and_ports_are_closed(self):
        mutations = [lambda r: r.update(Image="sha256:" + "0" * 64),
                     lambda r: r["Config"]["Labels"].update({"org.opencontainers.image.revision": "0" * 40}),
                     lambda r: r["State"].update(Running=False),
                     lambda r: r["HostConfig"].update(NetworkMode="host"),
                     lambda r: r["NetworkSettings"]["Ports"]["8080/tcp"][0].update(HostIp="0.0.0.0"),
                     lambda r: r["NetworkSettings"]["Ports"]["8080/tcp"][0].update(HostPort="65536"),
                     lambda r: r["NetworkSettings"]["Ports"].update({"9000/tcp": None})]
        for index, mutation in enumerate(mutations):
            record = runtime()
            mutation(record)
            with self.subTest(index=index), self.assertRaises(RuntimeError):
                self.origin(record)

    def test_broader_caps_and_privileges_refuse_for_both_roles(self):
        changes = [("Memory", 0), ("Memory", 2 * 1024**3), ("NanoCpus", 0), ("NanoCpus", 2 * 10**9),
                   ("PidsLimit", 0), ("PidsLimit", 257), ("ReadonlyRootfs", False), ("Privileged", True),
                   ("Init", False), ("CapDrop", []), ("CapAdd", ["SYS_ADMIN"]),
                   ("SecurityOpt", []), ("Devices", [{}]), ("Binds", ["unexpected"])]
        for factory, check in ((runtime, self.origin), (consumer, self.php)):
            for key, value in changes:
                record = factory()
                record["HostConfig"][key] = value
                with self.subTest(role=factory.__name__, key=key, value=value), self.assertRaises(RuntimeError):
                    check(record)

    def test_native_writable_paths_and_mounts_cannot_expand(self):
        for mutation in (lambda r: r["Mounts"].append({"Type": "bind", "Destination": "/closure"}),
                         lambda r: r["Mounts"].append({"Type": "tmpfs", "Destination": "/other"}),
                         lambda r: r["HostConfig"]["Tmpfs"].update({"/other": "size=1m"})):
            record = runtime()
            mutation(record)
            with self.assertRaises(RuntimeError):
                self.origin(record)

    def test_complete_php_include_closure_is_readonly_and_unprivileged(self):
        self.assertEqual(self.php(consumer())["Mounts"][0]["RW"], False)

    def test_writable_partial_replaced_or_shadowed_include_mount_refuses(self):
        mutations = [lambda r: r["Mounts"][0].update(RW=True),
                     lambda r: r["Mounts"][0].update(Source="/other"),
                     lambda r: r["Mounts"][0].update(Destination="/closure/consumer/vendor"),
                     lambda r: r["Mounts"].append({"Type": "tmpfs", "Destination": "/closure/fixture"}),
                     lambda r: r["Mounts"].append({"Type": "volume", "Destination": "/cache"}),
                     lambda r: r["HostConfig"]["Tmpfs"].update({"/closure": "size=1m"}),
                     lambda r: r["Config"].update(User="0:0")]
        for index, mutation in enumerate(mutations):
            record = consumer()
            mutation(record)
            with self.subTest(index=index), self.assertRaises(RuntimeError):
                self.php(record)

    def test_stage_requires_root_owner_inode_sealed_inventory_and_digest(self):
        metadata = SimpleNamespace(st_mode=stat.S_IFDIR | 0o555, st_uid=0, st_dev=3, st_ino=4)
        closure = Mock()
        closure.inventory.return_value = {"sha256": "d" * 64}
        with patch.object(Path, "lstat", return_value=metadata), patch.object(Path, "resolve", lambda path: path):
            self.assertEqual(MODULE.stage_inventory(ROOT, "d" * 64, "3:4", closure), closure.inventory.return_value)
            closure.inventory.assert_called_with(ROOT, 0, sealed=True)
            for root, digest, identity in ((Path("/tmp") / ROOT.name, "d" * 64, "3:4"),
                                           (ROOT, "e" * 64, "3:4"), (ROOT, "d" * 64, "3:5")):
                with self.assertRaises(RuntimeError):
                    MODULE.stage_inventory(root, digest, identity, closure)
            metadata.st_uid = 1001
            with self.assertRaises(RuntimeError):
                MODULE.stage_inventory(ROOT, "d" * 64, "3:4", closure)

    def bootstraps(self):
        bodies = WORKFLOW.read_text().split("python3 -I -S -B -c '\n")[1:]
        return [compile(textwrap.dedent(body.split("\n          ' \\", 1)[0]), "literal-controller-bootstrap", "exec")
                for body in bodies]

    def test_all_three_phase_bootstraps_bind_git_bytes_before_execution(self):
        bootstraps = self.bootstraps()
        self.assertEqual(len(bootstraps), 3)
        for bootstrap in bootstraps:
            with patch.dict(os.environ, {"GITHUB_SHA": "a" * 40}), \
                    patch.object(MODULE.subprocess, "check_output", return_value=SOURCE.read_bytes()) as read, \
                    patch("sys.argv", ["controller", "--help"]), patch("sys.stdout"), \
                    self.assertRaises(SystemExit) as result:
                exec(bootstrap, {})
            self.assertEqual(result.exception.code, 0)
            self.assertEqual(read.call_args.args[0], ["/usr/bin/git", "--no-replace-objects", "show",
                                                    "a" * 40 + ":tests/smoke/command_key_cohort.py"])

    def test_preopen_controller_substitution_refuses_before_compile(self):
        for bootstrap in self.bootstraps():
            with patch.dict(os.environ, {"GITHUB_SHA": "a" * 40}), \
                    patch.object(MODULE.subprocess, "check_output", return_value=b"inert substituted bytes"), \
                    patch("builtins.compile") as compile_source, \
                    self.assertRaisesRegex(RuntimeError, "independently fixed digest"):
                exec(bootstrap, {})
            compile_source.assert_not_called()

    def test_independent_cleanup_survives_checkout_state_and_docker_config_refusal(self):
        workflow = WORKFLOW.read_text()
        body = workflow.split("<<'PYCLEAN' | tee command-key-cleanup.json\n", 1)[1].split("\n          PYCLEAN", 1)[0]
        cleanup = compile(textwrap.dedent(body), "literal-always-cleanup", "exec")
        for foreign_consumer, mixed_error in ((False, False), (True, False), (False, True)):
            calls = []
            removed = set()

            def invoke(arguments, **options):
                self.assertEqual(options["env"]["DOCKER_CONFIG"], "/inert-independent-configuration")
                calls.append(arguments)
                identity = arguments[-1]
                if arguments[1] == "rm":
                    removed.add(identity)
                    return SimpleNamespace(returncode=0, stdout="", stderr="")
                if identity in removed:
                    extra = "engine unavailable\n" if mixed_error and identity != IDENTITY else ""
                    return SimpleNamespace(returncode=1, stdout="[]\n", stderr="error: no such object: " + identity + "\n" + extra)
                role = "server" if identity == IDENTITY else "consumer"
                labels = {"durable-workflow.php-command-keys": "foreign" if foreign_consumer and role == "consumer" else "123-1",
                          "durable-workflow.fixture-role": role, "durable-workflow.fixture-nonce": "b" * 32}
                return SimpleNamespace(returncode=0, stderr="", stdout=json.dumps([{"Id": identity, "Config": {"Labels": labels}}]))

            environment = {"GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "1", "FIXTURE_NONCE": "b" * 32,
                           "SERVER_ID": IDENTITY, "CONSUMER_ID": "c" * 64, "INCLUDE_ROOT": "", "INCLUDE_IDENTITY": "",
                           "GITHUB_SHA": "invalid", "COMMAND_KEY_STATE": "/missing-state", "TMPDIR": "/missing-temp",
                           "DOCKER_CONFIG": "/foreign-configuration", "DOCKER_HOST": "tcp://example.invalid:2375"}
            with patch.dict(os.environ, environment), patch("tempfile.TemporaryDirectory") as temporary, \
                    patch("subprocess.run", side_effect=invoke), patch("builtins.print"):
                temporary.return_value.__enter__.return_value = "/inert-independent-configuration"
                if foreign_consumer or mixed_error:
                    with self.assertRaises(SystemExit):
                        exec(cleanup, {})
                else:
                    exec(cleanup, {})
            self.assertIn(IDENTITY, removed)
            self.assertEqual("c" * 64 in removed, not foreign_consumer)
            self.assertTrue(all(call[0] == "/usr/bin/docker" for call in calls))
            temporary.assert_called_once_with(prefix="sdk-php-command-key-cleanup-", dir="/tmp")

    def test_receipt_comparison_preserves_original_identity_and_verified_intent(self):
        for kind in ("signal", "update"):
            original = {"command_id": "command", "run_id": "run", "request_id": "key", "signal_id": "signal",
                        "update_id": "update", "accepted_at": "date", "command_sequence": 3}
            retained = original | {"retained": True, "intent_verified": True}
            MODULE.retained(retained, original, kind)
            for field in MODULE.identity(original, kind):
                changed = deepcopy(retained)
                changed[field] = 4 if field == "command_sequence" else "changed"
                with self.subTest(kind=kind, field=field), self.assertRaises(RuntimeError):
                    MODULE.retained(changed, original, kind)
            for field in ("retained", "intent_verified"):
                with self.assertRaises(RuntimeError):
                    MODULE.retained(retained | {field: False}, original, kind)


if __name__ == "__main__":
    unittest.main()
