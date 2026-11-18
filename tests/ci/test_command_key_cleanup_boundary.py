"""Opt-in idle-container proof of the literal cleanup after control refusal."""

import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import stat
import subprocess
import tempfile
import textwrap
import time
import types
import unittest
from unittest.mock import patch


REPOSITORY = Path(__file__).resolve().parents[2]
IMAGE = os.environ.get("SDK_PHP_COMMAND_KEY_CLEANUP_IMAGE", "")
SOURCE = os.environ.get("SDK_PHP_COMMAND_KEY_CLEANUP_SOURCE", "")
CLEANUP_SHA256 = "8b30e60e4de5349c99521e0e258a5fa99be5831f8a6defed3db007ed4527f15f"
OBSERVATIONS = []


def captured_bytes(relative):
    path = REPOSITORY / relative
    if not IMAGE:
        return path.read_bytes()
    if re.fullmatch(r"[0-9a-f]{40}", SOURCE) is None:
        raise RuntimeError("The cleanup control needs its exact reviewed source")
    raw = subprocess.check_output(["/usr/bin/git", "--no-replace-objects", "-C", str(REPOSITORY),
                                   "show", SOURCE + ":" + relative], timeout=10)
    if path.read_bytes() != raw:
        raise RuntimeError("Cleanup control inputs differ from the reviewed source")
    return raw


def load(name):
    relative = "tests/smoke/" + name + ".py"
    module = types.ModuleType(name)
    module.__file__ = str(REPOSITORY / relative)
    exec(compile(captured_bytes(relative), module.__file__, "exec"), module.__dict__)
    return module


def cleanup_script(raw):
    body = raw.decode().split("<<'PYCLEAN' | tee command-key-cleanup.json\n", 1)[1].split("\n          PYCLEAN", 1)[0]
    script = textwrap.dedent(body)
    if hashlib.sha256(script.encode()).hexdigest() != CLEANUP_SHA256:
        raise RuntimeError("The independently fixed literal cleanup digest differs")
    return script


WORKFLOW = captured_bytes(".github/workflows/published-command-keys.yml")
CONTAINERS = load("command_key_containers")
COHORT = load("command_key_cohort")
SUPPLY = json.loads(captured_bytes("tests/smoke/command-key-supply.json"))


class CleanupSourceAuthority(unittest.TestCase):
    def test_exact_literal_cleanup_is_bound(self):
        script = cleanup_script(WORKFLOW)
        compile(script, "literal-independent-cleanup", "exec")

    def test_changed_cleanup_refuses_before_compilation(self):
        changed = WORKFLOW.replace(b"/usr/bin/docker", b"/bin/false", 1)
        self.assertNotEqual(changed, WORKFLOW)
        with self.assertRaisesRegex(RuntimeError, "fixed literal cleanup digest"):
            cleanup_script(changed)

    def simulate(self, create_output="", fault=None, script=None):
        server, consumer, extra = "a" * 64, "c" * 64, "e" * 64
        live = {consumer: {CONTAINERS.OWNER: "123-1", CONTAINERS.ROLE: "consumer", CONTAINERS.NONCE: "b" * 32}}
        captured, removed = {}, set()
        with tempfile.TemporaryDirectory(prefix="sdk-php-lost-create-inert-") as temporary:
            root = Path(temporary)
            configuration, state = root / "docker", root / "state"
            configuration.mkdir(mode=0o700)
            state.mkdir(mode=0o700)
            resources = CONTAINERS.Containers(configuration, state, "123-1", "b" * 32,
                                               lambda role, identity: captured.update({role: identity}))

            def create(arguments, **_):
                self.assertEqual(arguments[0], "create")
                live[server] = {CONTAINERS.OWNER: "123-1", CONTAINERS.ROLE: "server", CONTAINERS.NONCE: "b" * 32}
                if isinstance(create_output, BaseException):
                    raise create_output
                return types.SimpleNamespace(returncode=0, stdout=create_output, stderr="")

            try:
                with patch.object(resources, "command", side_effect=create), self.assertRaisesRegex(RuntimeError, "one full ID"):
                    resources.create("server", [])
                self.assertEqual(captured, {})
            finally:
                resources.close()
        if fault == "duplicate":
            live[extra] = dict(live[server])

        def invoke(arguments, **options):
            self.assertEqual(arguments[0], "/usr/bin/docker")
            self.assertEqual(options["env"], {"PATH": "/usr/bin:/bin", "DOCKER_CONFIG": "/inert-cleanup-config",
                                             "DOCKER_HOST": "unix:///var/run/docker.sock"})
            if arguments[1] == "ps":
                self.assertEqual(arguments[2:6], ["--all", "--no-trunc", "--last", "2"])
                self.assertIn("label=" + CONTAINERS.OWNER + "=123-1", arguments)
                self.assertIn("label=" + CONTAINERS.NONCE + "=" + "b" * 32, arguments)
                role = "server" if "label=" + CONTAINERS.ROLE + "=server" in arguments else "consumer"
                if role == "consumer" and fault in {"listing-failed", "listing-stderr", "listing-partial", "listing-overflow"}:
                    outputs = {"listing-failed": (1, "", "engine unavailable\n"),
                               "listing-stderr": (0, consumer + "\n", "mixed error\n"),
                               "listing-partial": (0, "abc\n", ""),
                               "listing-overflow": (0, (consumer + "\n") * 3, "")}
                    code, stdout, stderr = outputs[fault]
                    return types.SimpleNamespace(returncode=code, stdout=stdout, stderr=stderr)
                identities = [identity for identity, labels in live.items() if labels[CONTAINERS.ROLE] == role]
                return types.SimpleNamespace(returncode=0, stdout="".join(identity + "\n" for identity in identities[:2]), stderr="")
            identity = arguments[-1]
            if arguments[1] == "rm":
                self.assertIn(identity, live)
                labels = live.pop(identity)
                removed.add(identity)
                if fault == "replacement" and identity == consumer:
                    live[extra] = labels
                return types.SimpleNamespace(returncode=0, stdout=identity + "\n", stderr="")
            self.assertEqual(arguments[1], "inspect")
            if fault == "consumer-inspect" and identity == consumer:
                return types.SimpleNamespace(returncode=1, stdout="[]\n", stderr="engine unavailable\n")
            if identity not in live:
                return types.SimpleNamespace(returncode=1, stdout="[]\n", stderr="error: no such object: " + identity + "\n")
            labels = dict(live[identity])
            reported_identity = identity
            if identity == server:
                for label, selected_fault in ((CONTAINERS.OWNER, "foreign-owner"), (CONTAINERS.ROLE, "foreign-role"),
                                               (CONTAINERS.NONCE, "foreign-nonce")):
                    if fault == selected_fault:
                        labels[label] = "foreign"
                if fault == "foreign-id":
                    reported_identity = extra
            return types.SimpleNamespace(returncode=0, stdout=json.dumps([{"Id": reported_identity, "Config": {"Labels": labels}}]), stderr="")

        environment = {"GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "1", "FIXTURE_NONCE": "b" * 32,
                       "SERVER_ID": "", "CONSUMER_ID": consumer, "INCLUDE_ROOT": "", "INCLUDE_IDENTITY": "",
                       "GITHUB_SHA": "invalid", "COMMAND_KEY_STATE": "/missing-state", "TMPDIR": "/missing-temp",
                       "DOCKER_CONFIG": "/refused-config", "DOCKER_HOST": "tcp://example.invalid:2375"}
        code = 0
        with patch.dict(os.environ, environment, clear=True), patch("tempfile.TemporaryDirectory") as temporary, \
                patch("subprocess.run", side_effect=invoke), patch("builtins.print") as printed:
            temporary.return_value.__enter__.return_value = "/inert-cleanup-config"
            try:
                exec(compile(script if script is not None else cleanup_script(WORKFLOW), "inert-independent-cleanup", "exec"), {})
            except SystemExit as error:
                code = error.code
        return types.SimpleNamespace(code=code, outcome=json.loads(printed.call_args.args[0]), live=live, removed=removed)

    def test_lost_and_ambiguous_create_outputs_are_reconciled(self):
        for output in ("", "ambiguous\n", CONTAINERS.FixtureTimeout("")):
            result = self.simulate(output)
            self.assertEqual(result.live, {})
            self.assertEqual(result.code, 0)
            self.assertEqual(result.outcome["reconciled_ids"], {"server": "a" * 64})
            self.assertEqual(result.outcome["role_absence"], {"server": True, "consumer": True})

    def test_duplicate_and_foreign_candidates_refuse(self):
        for fault in ("duplicate", "foreign-owner", "foreign-role", "foreign-nonce", "foreign-id"):
            result = self.simulate(fault=fault)
            self.assertEqual(result.code, 1)
            self.assertFalse(result.outcome["runtime_removed"])
            self.assertIn("a" * 64, result.live)
            self.assertIn("c" * 64, result.removed)

    def test_one_role_error_does_not_skip_cleanup_of_the_other(self):
        result = self.simulate(fault="consumer-inspect")
        self.assertEqual(result.code, 1)
        self.assertIn("c" * 64, result.live)
        self.assertIn("a" * 64, result.removed)
        self.assertEqual(result.outcome["role_absence"], {"consumer": False, "server": True})

    def test_postdelete_role_replacement_cannot_be_reported_absent(self):
        result = self.simulate(fault="replacement")
        self.assertEqual(result.code, 1)
        self.assertIn("e" * 64, result.live)
        self.assertEqual(result.removed, {"a" * 64, "c" * 64})
        self.assertFalse(result.outcome["role_absence"]["consumer"])

    def test_malformed_or_failed_role_listing_refuses(self):
        for fault in ("listing-failed", "listing-stderr", "listing-partial", "listing-overflow"):
            result = self.simulate(fault=fault)
            self.assertEqual(result.code, 1)
            self.assertIn("c" * 64, result.live)
            self.assertIn("a" * 64, result.removed)


@unittest.skipUnless(IMAGE, "Cleanup control requires exact reviewed source and the cached selected PHP image.")
class SyntheticCleanupBoundary(unittest.TestCase):
    def setUp(self):
        self.assertEqual(IMAGE, "php@" + SUPPLY["php_index"])
        self.assertGreater(os.getuid(), 0)
        captured_bytes("tests/ci/test_command_key_cleanup_boundary.py")
        self.script = cleanup_script(WORKFLOW)
        self.work = Path(tempfile.mkdtemp(prefix="sdk-php-cleanup-control-"))
        self.active = set()
        self.captured = {}
        self.cleanup_proved = False
        self.addCleanup(self.remove_owned_data)

    def remove_owned_data(self):
        self.assertTrue(self.cleanup_proved, "Preserve evidence until both labelled roles are independently absent")
        self.assertFalse(self.active, "Preserve control data until exact container absence is established")
        metadata = self.work.lstat()
        self.assertEqual(self.work.resolve(), self.work)
        self.assertTrue(stat.S_ISDIR(metadata.st_mode))
        self.assertEqual(metadata.st_uid, os.getuid())
        self.assertTrue(shutil.rmtree.avoids_symlink_attacks)
        shutil.rmtree(self.work)

    def record_created(self, role, identity):
        self.assertNotIn(role, self.captured)
        self.captured[role] = identity
        self.active.add(identity)

    def test_literal_cleanup_removes_both_roles_after_checkout_state_and_config_refuse(self):
        configuration, state, observer = (self.work / name for name in ("docker", "records", "observer"))
        for path in (configuration, state, observer):
            path.mkdir(mode=0o700)
        run_id, attempt = str(os.getpid()), str(time.time_ns())
        nonce = secrets.token_hex(16)
        resources = CONTAINERS.Containers(configuration, state, run_id + "-" + attempt, nonce, self.record_created)
        observation = {"source": SOURCE, "cleanup_sha256": CLEANUP_SHA256,
                       "fixture": "idle-php-roles-only", "captured_ids": self.captured,
                       "lost_create_refused": False,
                       "checkout_refused": False, "state_refused": False, "config_refused": False,
                       "container_removed": False}
        OBSERVATIONS.append(observation)
        observation["ownership"] = {"run_id": run_id, "attempt": attempt, "owner": run_id + "-" + attempt, "nonce": nonce}
        # Flush the reconciliation tuple before create can lose its stdout.
        print(json.dumps({"scope": "synthetic-cleanup-ownership", "source": SOURCE,
                          "ownership": observation["ownership"]}), flush=True)
        try:
            for role in ("server", "consumer"):
                arguments = ["--pull", "never", "--network", "none", "--user", str(os.getuid()) + ":" + str(os.getgid()),
                             IMAGE, "php", "-n", "-r", "sleep(90);"]
                if role == "server":
                    command = resources.command

                    def lose_create_stdout(arguments, **options):
                        result = command(arguments, **options)
                        if arguments[0] == "create":
                            self.assertEqual(result.returncode, 0)
                            self.assertRegex(result.stdout, r"\A[0-9a-f]{64}\n\Z")
                            observation["lost_create_id_sha256"] = hashlib.sha256(result.stdout.strip().encode()).hexdigest()
                            return types.SimpleNamespace(returncode=0, stdout="", stderr=result.stderr)
                        return result

                    with patch.object(resources, "command", side_effect=lose_create_stdout), \
                            self.assertRaisesRegex(RuntimeError, "one full ID"):
                        resources.create(role, arguments)
                    self.assertNotIn(role, self.captured)
                    observation["lost_create_refused"] = True
                    continue
                identity = resources.create(role, arguments)
                COHORT.selected_image(resources.owned(identity, role), SUPPLY, "php")
                resources.start(identity, role)
                record = resources.owned(identity, role)
                COHORT.confined(record, SUPPLY, "php")
                self.assertEqual(record["HostConfig"]["NetworkMode"], "none")
                self.assertEqual(record["Config"]["User"], str(os.getuid()) + ":" + str(os.getgid()))
                self.assertEqual(record["Mounts"], [])
                self.assertFalse(record["HostConfig"].get("Tmpfs"))
                self.assertFalse(record["NetworkSettings"]["Ports"])
            with self.assertRaisesRegex(RuntimeError, "exact reviewed runner commit"):
                COHORT.capture_inputs("invalid")
            observation["checkout_refused"] = True
            os.fchmod(resources.state_descriptor, 0o755)
            with self.assertRaisesRegex(RuntimeError, "canonical, private and owned"):
                CONTAINERS.Containers(configuration, state, run_id + "-" + attempt, nonce)
            observation["state_refused"] = True
            descriptor = os.open(configuration / "config.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(descriptor, "w") as output:
                output.write("{}\n")
            with self.assertRaisesRegex(RuntimeError, "configuration must remain empty"):
                resources.environment()
            observation["config_refused"] = True
        finally:
            try:
                resources.close()
            finally:
                self.independent_cleanup(configuration, observer, run_id, attempt, nonce, observation)
        self.assertEqual(set(self.captured), {"consumer"})
        recovered = observation["cleanup"]["reconciled_ids"]
        self.assertEqual(set(recovered), {"server"})
        self.assertEqual(hashlib.sha256(recovered["server"].encode()).hexdigest(), observation["lost_create_id_sha256"])

    def independent_cleanup(self, configuration, observer, run_id, attempt, nonce, observation):
        environment = {"PATH": "/usr/bin:/bin", "GITHUB_RUN_ID": run_id, "GITHUB_RUN_ATTEMPT": attempt,
                       "FIXTURE_NONCE": nonce, "CONSUMER_ID": self.captured.get("consumer", ""),
                       "SERVER_ID": self.captured.get("server", ""), "INCLUDE_ROOT": "", "INCLUDE_IDENTITY": "",
                       "GITHUB_SHA": "invalid", "COMMAND_KEY_STATE": str(self.work / "missing-state"),
                       "TMPDIR": str(self.work / "missing-temp"), "DOCKER_CONFIG": str(configuration),
                       "DOCKER_HOST": "tcp://example.invalid:2375"}
        cleanup = subprocess.run(["/usr/bin/python3", "-I", "-S", "-B", "-c", self.script],
                                 env=environment, cwd=self.work, capture_output=True, text=True, timeout=120)
        observation["cleanup_exit"] = cleanup.returncode
        self.assertEqual(cleanup.returncode, 0, cleanup.stderr)
        outcome = json.loads(cleanup.stdout)
        observation["cleanup"] = outcome
        self.assertTrue(outcome["runtime_removed"])
        self.assertEqual(outcome["errors"], [])
        self.assertEqual(outcome["role_absence"], {"server": True, "consumer": True})
        found = dict(self.captured)
        for role, identity in outcome["reconciled_ids"].items():
            self.assertIn(role, {"server", "consumer"})
            self.assertRegex(identity, r"\A[0-9a-f]{64}\Z")
            self.assertNotIn(role, found)
            found[role] = identity
        self.assertEqual(len(set(found.values())), len(found))
        observation["observed_ids"] = found
        self.active.update(found.values())
        empty_environment = {"PATH": "/usr/bin:/bin", "DOCKER_CONFIG": str(observer),
                             "DOCKER_HOST": "unix:///var/run/docker.sock"}
        for identity in found.values():
            absent = subprocess.run(["/usr/bin/docker", "inspect", identity], env=empty_environment,
                                    capture_output=True, text=True, timeout=15)
            self.assertTrue(CONTAINERS.exact_missing(absent, identity))
            self.active.remove(identity)
        for role in ("server", "consumer"):
            listed = subprocess.run(["/usr/bin/docker", "ps", "--all", "--no-trunc", "--last", "2",
                "--filter", "label=" + CONTAINERS.OWNER + "=" + run_id + "-" + attempt,
                "--filter", "label=" + CONTAINERS.ROLE + "=" + role,
                "--filter", "label=" + CONTAINERS.NONCE + "=" + nonce, "--format", "{{.ID}}"],
                env=empty_environment, capture_output=True, text=True, timeout=15)
            self.assertEqual((listed.returncode, listed.stdout, listed.stderr), (0, "", ""))
        self.cleanup_proved = True
        observation["container_removed"] = not self.active


if __name__ == "__main__":
    result = unittest.main(exit=False, failfast=True).result
    if IMAGE:
        print(json.dumps({"scope": "synthetic-state-refusal-cleanup-only", "observations": OBSERVATIONS}))
    raise SystemExit(not result.wasSuccessful())
