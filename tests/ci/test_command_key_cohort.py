"""Inert closure, workflow-bootstrap and receipt controls; no runtime calls."""

from copy import deepcopy
import hashlib
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
            "Config": {"Image": "durableworkflow/server@" + SUPPLY["server_index"],
                       "Labels": {"org.opencontainers.image.revision": SUPPLY["server_source"]}},
            "HostConfig": {"ReadonlyRootfs": True, "Privileged": False, "Binds": None,
                           "Init": True, "CapDrop": ["ALL"], "CapAdd": None,
                           "SecurityOpt": ["no-new-privileges"], "Devices": [], "NetworkMode": "bridge",
                           "Memory": 1024**3, "NanoCpus": 10**9, "PidsLimit": 256,
                           "Tmpfs": {"/tmp": "mode=1777,size=64m",
                                     "/app/database": "uid=1000,gid=1000,mode=0700,size=32m",
                                     "/app/storage": "uid=1000,gid=1000,mode=0700,size=64m"}},
            "Mounts": [{"Type": "tmpfs", "Destination": path} for path in ("/tmp", "/app/database", "/app/storage")],
            "NetworkSettings": {"Ports": {"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": "43123"}]}}}


def consumer():
    record = runtime()
    record["Image"] = SUPPLY["php_config"]
    record["Config"]["Image"] = "php@" + SUPPLY["php_index"]
    record["Config"]["User"] = "1001:1001"
    record["HostConfig"].update(NetworkMode="host", Tmpfs={"/tmp": "mode=1777,size=128m"})
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

    def test_legacy_tmpfs_configuration_and_complete_reported_form_both_pass(self):
        for factory, check in ((runtime, self.origin), (consumer, self.php)):
            complete = factory()
            check(complete)
            legacy = deepcopy(complete)
            legacy["Mounts"] = [mount for mount in legacy["Mounts"] if mount["Type"] != "tmpfs"]
            check(legacy)

    def test_duplicate_reported_destinations_refuse_for_both_roles(self):
        for factory, check in ((runtime, self.origin), (consumer, self.php)):
            record = factory()
            record["Mounts"].append(deepcopy(record["Mounts"][-1]))
            with self.subTest(role=factory.__name__), self.assertRaisesRegex(RuntimeError, "mount destinations"):
                check(record)

    def test_partial_reported_native_tmpfs_set_refuses(self):
        record = runtime()
        record["Mounts"].pop()
        with self.assertRaisesRegex(RuntimeError, "mount destinations"):
            self.origin(record)

    def test_tmpfs_size_permissions_and_declared_path_set_are_exact(self):
        for factory, check in ((runtime, self.origin), (consumer, self.php)):
            for options in ("mode=1777,size=1g", "mode=0777,size=128m", "", None):
                record = factory()
                record["HostConfig"]["Tmpfs"]["/tmp"] = options
                with self.subTest(role=factory.__name__, options=options), self.assertRaisesRegex(RuntimeError, "tmpfs configuration"):
                    check(record)
            for declared in ({}, {"/tmp": "mode=1777,size=128m", "/closure": "size=1m"}):
                record = factory()
                record["Mounts"] = [mount for mount in record["Mounts"] if mount["Type"] != "tmpfs"]
                record["HostConfig"]["Tmpfs"] = declared
                with self.subTest(role=factory.__name__, declared=declared), self.assertRaisesRegex(RuntimeError, "tmpfs configuration"):
                    check(record)

    def test_containerd_index_identity_requires_the_exact_platform_manifest(self):
        for factory, role, check in ((runtime, "server", self.origin), (consumer, "php", self.php)):
            record = factory()
            record["Image"] = SUPPLY[role + "_index"]
            record["ImageManifestDescriptor"] = {
                "digest": SUPPLY[role + "_amd64"], "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "platform": {"os": "linux", "architecture": "amd64"}}
            check(record)
            for mutation in (lambda r: r.update(ImageManifestDescriptor=None),
                             lambda r: r["ImageManifestDescriptor"].update(digest="sha256:" + "0" * 64),
                             lambda r: r["ImageManifestDescriptor"]["platform"].update(architecture="arm64"),
                             lambda r: r["ImageManifestDescriptor"]["platform"].update(os="windows"),
                             lambda r: r["ImageManifestDescriptor"].update(mediaType="application/vnd.oci.image.index.v1+json"),
                             lambda r: r["Config"].update(Image="php:latest"),
                             lambda r: r.update(Image="sha256:" + "0" * 64)):
                changed = deepcopy(record)
                mutation(changed)
                with self.subTest(role=role), self.assertRaises(RuntimeError):
                    check(changed)

    def test_present_wrong_manifest_refuses_even_with_matching_classic_config(self):
        record = runtime()
        record["ImageManifestDescriptor"] = {"digest": "sha256:" + "0" * 64,
                                              "mediaType": "application/vnd.oci.image.manifest.v1+json",
                                              "platform": {"os": "linux", "architecture": "amd64"}}
        with self.assertRaisesRegex(RuntimeError, "platform manifest differs"):
            self.origin(record)

    def test_wrong_selected_manifest_refuses_before_native_process_start(self):
        resources = Mock()
        resources.identities = {"server": IDENTITY}
        resources.pull.return_value = {"Config": {"Labels": {"org.opencontainers.image.revision": SUPPLY["server_source"]}}}
        resources.create.return_value = IDENTITY
        record = runtime()
        record["State"]["Running"] = False
        record["Image"] = SUPPLY["server_index"]
        record["ImageManifestDescriptor"] = {"digest": "sha256:" + "0" * 64,
                                              "mediaType": "application/vnd.oci.image.manifest.v1+json",
                                              "platform": {"os": "linux", "architecture": "amd64"}}
        resources.owned.return_value = record
        with self.assertRaisesRegex(RuntimeError, "platform manifest differs"):
            MODULE.prepare_containers(resources, SUPPLY, "synthetic", ROOT)
        resources.start.assert_not_called()
        resources.remove_all.assert_called_once()

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
        self.assertFalse(self.php(consumer())["Mounts"][0]["RW"])

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
            self.assertEqual(MODULE.stage_inventory(ROOT, "d" * 64, "3:4", closure, SUPPLY, {}), closure.inventory.return_value)
            closure.inventory.assert_called_with(ROOT, 0, sealed=True)
            closure.verify_sealed_sdk.assert_called_once_with(closure.inventory.return_value, SUPPLY["sdk_archive_members_sha256"])
            closure.bind_members.assert_called_once_with(closure.inventory.return_value, "fixture/", {}, sealed=True)
            for root, digest, identity in ((Path("/tmp") / ROOT.name, "d" * 64, "3:4"),
                                           (ROOT, "e" * 64, "3:4"), (ROOT, "d" * 64, "3:5")):
                with self.assertRaises(RuntimeError):
                    MODULE.stage_inventory(root, digest, identity, closure, SUPPLY, {})
            metadata.st_uid = 1001
            with self.assertRaises(RuntimeError):
                MODULE.stage_inventory(ROOT, "d" * 64, "3:4", closure, SUPPLY, {})

    def bootstraps(self):
        bodies = WORKFLOW.read_text().split("python3 -I -S -B -c '\n")[1:]
        return [compile(textwrap.dedent(body.split("\n          ' \\", 1)[0]), "literal-controller-bootstrap", "exec")
                for body in bodies]

    def test_matching_dynamic_root_hash_cannot_authorize_a_replaced_sdk_leaf(self):
        path = SOURCE.with_name("command_key_include_closure.py")
        spec = importlib.util.spec_from_file_location("root_closure", path)
        closure = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(closure)
        approved = {"src/Client.php": {"mode": 0o644, "bytes": 5, "sha256": hashlib.sha256(b"safe!").hexdigest()}}
        supply = {"sdk_archive_members_sha256": closure.members_digest(approved)}
        prefix = "consumer/vendor/durable-workflow/sdk"
        snapshot = {"sha256": "d" * 64, "entries": {
            prefix: {"kind": "directory", "mode": 0o555},
            prefix + "/src": {"kind": "directory", "mode": 0o555},
            prefix + "/src/Client.php": {"kind": "file", "mode": 0o444, "bytes": 5,
                                         "sha256": hashlib.sha256(b"evil!").hexdigest()}}}
        metadata = SimpleNamespace(st_mode=stat.S_IFDIR | 0o555, st_uid=0, st_dev=3, st_ino=4)
        with patch.object(Path, "lstat", return_value=metadata), patch.object(Path, "resolve", lambda value: value), \
                patch.object(closure, "inventory", return_value=snapshot), \
                self.assertRaisesRegex(ValueError, "approved archive member authority"):
            MODULE.stage_inventory(ROOT, snapshot["sha256"], "3:4", closure, supply, {})

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
