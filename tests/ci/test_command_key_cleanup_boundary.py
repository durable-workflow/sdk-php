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


REPOSITORY = Path(__file__).resolve().parents[2]
IMAGE = os.environ.get("SDK_PHP_COMMAND_KEY_CLEANUP_IMAGE", "")
SOURCE = os.environ.get("SDK_PHP_COMMAND_KEY_CLEANUP_SOURCE", "")
CLEANUP_SHA256 = "5cbfb06cd9ef5d6c73c006b48584f89e9c335d27d608c6616c133291316b0f83"
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
        self.addCleanup(self.remove_owned_data)

    def remove_owned_data(self):
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
                       "fixture": "two-idle-php-roles-only", "captured_ids": self.captured,
                       "checkout_refused": False, "state_refused": False, "config_refused": False,
                       "container_removed": False}
        OBSERVATIONS.append(observation)
        try:
            for role in ("server", "consumer"):
                identity = resources.create(role, ["--pull", "never", "--network", "none", "--user",
                    str(os.getuid()) + ":" + str(os.getgid()), IMAGE, "php", "-n", "-r", "sleep(90);"])
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
            resources.close()
            environment = {"PATH": "/usr/bin:/bin", "GITHUB_RUN_ID": run_id, "GITHUB_RUN_ATTEMPT": attempt,
                           "FIXTURE_NONCE": nonce, "CONSUMER_ID": self.captured.get("consumer", ""),
                           "SERVER_ID": self.captured.get("server", ""), "INCLUDE_ROOT": "", "INCLUDE_IDENTITY": "",
                           "GITHUB_SHA": "invalid", "COMMAND_KEY_STATE": str(self.work / "missing-state"),
                           "TMPDIR": str(self.work / "missing-temp"), "DOCKER_CONFIG": str(configuration),
                           "DOCKER_HOST": "tcp://example.invalid:2375"}
            cleanup = subprocess.run(["/usr/bin/python3", "-I", "-S", "-B", "-c", self.script],
                                     env=environment, cwd=self.work, capture_output=True, text=True, timeout=120)
            observation["cleanup_exit"] = cleanup.returncode
            observation["cleanup_stdout"] = cleanup.stdout
            self.assertEqual(cleanup.returncode, 0, cleanup.stderr)
            self.assertEqual(json.loads(cleanup.stdout), {"runtime_removed": True, "errors": []})
            empty_environment = {"PATH": "/usr/bin:/bin", "DOCKER_CONFIG": str(observer),
                                 "DOCKER_HOST": "unix:///var/run/docker.sock"}
            for identity in self.captured.values():
                absent = subprocess.run(["/usr/bin/docker", "inspect", identity], env=empty_environment,
                                        capture_output=True, text=True, timeout=15)
                self.assertTrue(CONTAINERS.exact_missing(absent, identity))
                self.active.remove(identity)
            observation["container_removed"] = not self.active
        self.assertEqual(set(self.captured), {"server", "consumer"})


if __name__ == "__main__":
    result = unittest.main(exit=False, failfast=True).result
    if IMAGE:
        print(json.dumps({"scope": "synthetic-state-refusal-cleanup-only", "observations": OBSERVATIONS}))
    raise SystemExit(not result.wasSuccessful())
