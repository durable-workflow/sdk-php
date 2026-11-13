"""Inert provenance/receipt controls. No Docker, PHP, runtime or network calls."""

from copy import deepcopy
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import textwrap
import unittest
from unittest.mock import patch


SOURCE = Path(__file__).resolve().parents[1] / "smoke/command_key_cohort.py"
SPEC = importlib.util.spec_from_file_location("command_key_cohort", SOURCE)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
SUPPLY = json.loads(SOURCE.with_name("command-key-supply.json").read_text())
IDENTITY = "a" * 64


def runtime():
    return {"Id": IDENTITY, "Image": SUPPLY["server_config"], "State": {"Running": True},
            "Config": {"Labels": {"durable-workflow.php-command-keys": "123-1",
                                   "org.opencontainers.image.revision": SUPPLY["server_source"]}},
            "HostConfig": {"ReadonlyRootfs": True, "Privileged": False, "Binds": None,
                           "Init": True, "CapDrop": ["ALL"], "CapAdd": None,
                           "SecurityOpt": ["no-new-privileges"], "Devices": [], "NetworkMode": "bridge",
                           "Memory": 1024**3, "NanoCpus": 10**9, "PidsLimit": 256,
                           "Tmpfs": {"/tmp": "size=64m", "/app/database": "size=32m", "/app/storage": "size=64m"}},
            "Mounts": [], "NetworkSettings": {"Ports": {"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": "43123"}]}}}


class ProvenanceControls(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        configuration = patch.object(MODULE, "DOCKER_CONFIG", Path(directory.name))
        configuration.start()
        self.addCleanup(configuration.stop)

    def origin(self, record):
        with patch.object(MODULE, "inspect_runtime", return_value=record):
            return MODULE.runtime_origin(IDENTITY, "123-1", SUPPLY)

    def test_complete_owned_runtime_selects_only_numeric_loopback(self):
        self.assertEqual(self.origin(runtime()), "http://127.0.0.1:43123")

    def test_foreign_id_owner_source_and_config_refuse(self):
        for mutation in (lambda r: r.update(Id="b" * 64),
                         lambda r: r.update(Image="sha256:" + "0" * 64),
                         lambda r: r["Config"]["Labels"].update({"durable-workflow.php-command-keys": "other"}),
                         lambda r: r["Config"]["Labels"].update({"org.opencontainers.image.revision": "0" * 40}),
                         lambda r: r["State"].update(Running=False)):
            record = runtime()
            mutation(record)
            with self.subTest(record=record), self.assertRaises(RuntimeError):
                self.origin(record)

    def test_broader_caps_filesystems_and_ports_refuse(self):
        mutations = [lambda r: r["Mounts"].append({"Type": "bind"}),
                     lambda r: r["HostConfig"]["Tmpfs"].update({"/other": "size=1m"}),
                     lambda r: r["NetworkSettings"]["Ports"]["8080/tcp"][0].update(HostIp="0.0.0.0"),
                     lambda r: r["NetworkSettings"]["Ports"].update({"9000/tcp": None})]
        mutations += [lambda r, key=key, value=value: r["HostConfig"].update({key: value})
                      for key, value in [("Memory", 0), ("Memory", 2 * 1024**3), ("NanoCpus", 0),
                                         ("PidsLimit", 257), ("ReadonlyRootfs", False), ("Privileged", True),
                                         ("Init", False), ("CapDrop", []), ("CapAdd", ["SYS_ADMIN"]),
                                         ("SecurityOpt", []), ("NetworkMode", "host"), ("Devices", [{}])]]
        for index, mutation in enumerate(mutations):
            record = runtime()
            mutation(record)
            with self.subTest(index=index), self.assertRaises(RuntimeError):
                self.origin(record)

    def test_cleanup_preserves_foreign_id_replacements(self):
        record = runtime()
        record["Config"]["Labels"]["durable-workflow.php-command-keys"] = "foreign"
        with patch.object(MODULE, "inspect_runtime", return_value=record), \
                patch.object(MODULE.subprocess, "run") as execute:
            with self.assertRaisesRegex(RuntimeError, "preserve the replacement"):
                MODULE.remove_runtime(IDENTITY, "123-1")
            execute.assert_not_called()

    def test_cleanup_uses_only_captured_id_and_requires_missing_object(self):
        calls = []

        def execute(arguments, **_):
            calls.append(arguments)
            return type("Result", (), {"returncode": 1, "stdout": "[]\n",
                                       "stderr": "Error: No such object: " + IDENTITY + "\n"})()

        with patch.object(MODULE, "inspect_runtime", return_value=runtime()), \
                patch.object(MODULE.subprocess, "run", side_effect=execute):
            MODULE.remove_runtime(IDENTITY, "123-1")
        self.assertEqual(calls, [["docker", "rm", "--force", "--volumes", IDENTITY], ["docker", "inspect", IDENTITY]])

    def test_inspection_error_category_is_not_an_absence_receipt(self):
        result = type("Result", (), {"returncode": 1, "stdout": "", "stderr": "engine unavailable"})()
        with patch.object(MODULE.subprocess, "run", return_value=result), self.assertRaises(RuntimeError):
            MODULE.inspect_runtime(IDENTITY, allow_missing=True)

    def test_missing_object_requires_exact_exit_stdout_and_stderr(self):
        expected = {"returncode": 1, "stdout": "[]\n", "stderr": "Error: No such object: " + IDENTITY + "\n"}
        self.assertTrue(MODULE.exact_missing(type("Result", (), expected)(), IDENTITY))
        for changed in ({"returncode": 125}, {"stdout": ""}, {"stdout": "[{}]\n"},
                        {"stderr": expected["stderr"] + "permission denied\n"},
                        {"stderr": "daemon error\n" + expected["stderr"]}):
            with self.subTest(changed=changed):
                process = type("Result", (), expected | changed)()
                self.assertFalse(MODULE.exact_missing(process, IDENTITY))
                with patch.object(MODULE.subprocess, "run", return_value=process), self.assertRaises(RuntimeError):
                    MODULE.inspect_runtime(IDENTITY, allow_missing=True)

    def test_docker_environment_excludes_ambient_credentials_and_remote_context(self):
        with patch.dict(os.environ, {"DOCKER_AUTH_CONFIG": "untrusted", "DOCKER_CONTEXT": "foreign",
                                     "DOCKER_HOST": "tcp://remote:2375"}):
            environment = MODULE.docker_environment()
        self.assertEqual(environment, {"PATH": "/usr/bin:/bin", "DOCKER_CONFIG": str(MODULE.DOCKER_CONFIG),
                                       "DOCKER_HOST": "unix:///var/run/docker.sock"})
        (MODULE.DOCKER_CONFIG / "config.json").write_text('{}')
        with self.assertRaisesRegex(RuntimeError, "remain empty"):
            MODULE.docker_environment()

    def controller_bootstrap(self):
        workflow = (SOURCE.parents[2] / ".github/workflows/published-command-keys.yml").read_text()
        literal = workflow.split("python3 -I -S -B -c '\n", 1)[1].split("\n          ' \\", 1)[0]
        return compile(textwrap.dedent(literal), "literal-controller-bootstrap", "exec")

    def test_literal_bootstrap_loads_only_the_digest_bound_git_blob(self):
        bootstrap = self.controller_bootstrap()
        with patch.dict(os.environ, {"GITHUB_SHA": "a" * 40}), \
                patch.object(MODULE.subprocess, "check_output", return_value=SOURCE.read_bytes()) as read, \
                patch("sys.argv", ["controller", "--help"]), patch("sys.stdout"), \
                self.assertRaises(SystemExit) as result:
            exec(bootstrap, {})
        self.assertEqual(result.exception.code, 0)
        self.assertEqual(read.call_args.args[0], ["/usr/bin/git", "--no-replace-objects", "show",
                                                "a" * 40 + ":tests/smoke/command_key_cohort.py"])

    def test_literal_bootstrap_rejects_other_bytes_before_compile(self):
        bootstrap = self.controller_bootstrap()
        with patch.dict(os.environ, {"GITHUB_SHA": "a" * 40}), \
                patch.object(MODULE.subprocess, "check_output", return_value=b"print('inert marker')"), \
                patch("builtins.compile") as compile_source, \
                self.assertRaisesRegex(RuntimeError, "independently fixed digest"):
            exec(bootstrap, {})
        compile_source.assert_not_called()

    def test_only_owned_private_runtime_state_and_regular_cid_are_accepted(self):
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            self.assertEqual(MODULE.private_state(state), state / "runtime.cid")
            (state / "runtime.cid").write_text(IDENTITY)
            with patch.object(MODULE, "remove_runtime") as remove:
                MODULE.cleanup_state(state, "123-1")
                remove.assert_called_once_with(IDENTITY, "123-1")
            os.chmod(state, 0o755)
            with self.assertRaises(RuntimeError):
                MODULE.private_state(state)
            os.chmod(state, 0o700)
            (state / "runtime.cid").write_text("name-instead-of-id")
            with self.assertRaises(RuntimeError):
                MODULE.cleanup_state(state, "123-1")

    def test_receipt_comparison_preserves_every_original_identity(self):
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
