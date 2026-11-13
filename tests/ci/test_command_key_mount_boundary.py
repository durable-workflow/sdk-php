"""Opt-in mount controls using generated include files, never package code."""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import stat
import subprocess
import tempfile
import time
import types
import unittest


HERE = Path(__file__).resolve().parent
SMOKE = HERE.parent / "smoke"
IMAGE = os.environ.get("SDK_PHP_COMMAND_KEY_MOUNT_IMAGE", "")
OBSERVATIONS = []
TARGETS = ("consumer/vendor/autoload.php", "consumer/vendor/composer/nested.php",
           "fixture/command-key-client.php", "fixture/nested/leaf.php")
PROBE = r'''
$targets=['consumer/vendor/autoload.php','consumer/vendor/composer/nested.php',
          'fixture/command-key-client.php','fixture/nested/leaf.php'];
$results=[];
foreach($targets as $index=>$relative){
    $target='/closure/'.$relative;
    $results[]=@file_put_contents($target,"synthetic replacement\n")!==false;
    $results[]=@rename(dirname($target).'/candidate-'.$index,$target);
}
echo json_encode($results,JSON_THROW_ON_ERROR),"\n";
'''


def selected_bytes(path):
    if not IMAGE:
        return path.read_bytes()
    source = os.environ.get("SDK_PHP_COMMAND_KEY_BOUNDARY_SOURCE", "")
    if re.fullmatch(r"[0-9a-f]{40}", source) is None:
        raise RuntimeError("The boundary test needs its exact reviewed Git source")
    repository = HERE.parents[1]
    raw = subprocess.check_output(["/usr/bin/git", "--no-replace-objects", "-C", str(repository),
                                   "show", source + ":" + path.relative_to(repository).as_posix()], timeout=10)
    if path.read_bytes() != raw:
        raise RuntimeError("Boundary test inputs differ from the reviewed source")
    return raw


def load(name):
    path = SMOKE / (name + ".py")
    module = types.ModuleType(name)
    module.__file__ = str(path)
    exec(compile(selected_bytes(path), str(path), "exec"), module.__dict__)
    return module


COHORT = load("command_key_cohort")
CONTAINERS = load("command_key_containers")
CLOSURE = load("command_key_include_closure")
SUPPLY = json.loads(selected_bytes(SMOKE / "command-key-supply.json"))


def boundary_arguments(root, *, writable=False):
    arguments = ["--pull", "never", *COHORT.php_consumer_arguments(root, SUPPLY)]
    if writable:
        position = arguments.index("--mount") + 1
        if not arguments[position].endswith(",readonly"):
            raise RuntimeError("The countercontrol may remove only the literal read-only mount flag")
        arguments[position] = arguments[position].removesuffix(",readonly")
    return arguments


class ConsumerArguments(unittest.TestCase):
    def test_unprivileged_complete_closure_factory_has_no_writable_alias(self):
        from unittest.mock import patch
        root = Path("/run/sdk-php-command-keys.abcdef")
        with patch.object(COHORT.os, "getuid", return_value=1001), patch.object(COHORT.os, "getgid", return_value=1001):
            arguments = boundary_arguments(root)
            countercontrol = boundary_arguments(root, writable=True)
        self.assertEqual(arguments[:2], ["--pull", "never"])
        self.assertEqual(arguments[arguments.index("--mount") + 1], "type=bind,src=" + str(root) + ",dst=/closure,readonly")
        self.assertEqual(arguments[arguments.index("--user") + 1], "1001:1001")
        self.assertEqual(arguments[arguments.index("--tmpfs") + 1], "/tmp:mode=1777,size=128m")
        self.assertEqual(arguments.count("--mount"), 1)
        self.assertEqual([index for index, values in enumerate(zip(arguments, countercontrol)) if values[0] != values[1]],
                         [arguments.index("--mount") + 1])
        with patch.object(COHORT.os, "getuid", return_value=0), self.assertRaises(RuntimeError):
            COHORT.php_consumer_arguments(root, SUPPLY)


@unittest.skipUnless(IMAGE, "Boundary controls require an exact reviewed source and cached selected PHP image.")
class SyntheticMountBoundary(unittest.TestCase):
    def setUp(self):
        self.assertEqual(IMAGE, "php@" + SUPPLY["php_index"])
        self.assertGreater(os.getuid(), 0)
        source = os.environ.get("SDK_PHP_COMMAND_KEY_BOUNDARY_SOURCE", "")
        self.assertRegex(source, r"\A[0-9a-f]{40}\Z")
        repository = HERE.parents[1]
        for path in (Path(__file__), *(SMOKE / (name + ".py") for name in
                     ("command_key_cohort", "command_key_containers", "command_key_include_closure")),
                     SMOKE / "command-key-supply.json"):
            relative = path.relative_to(repository).as_posix()
            approved = subprocess.check_output(["/usr/bin/git", "--no-replace-objects", "-C", str(repository),
                                                "show", source + ":" + relative], timeout=10)
            self.assertEqual(path.read_bytes(), approved)
        self.work = Path(tempfile.mkdtemp(prefix="sdk-php-boundary-data-"))
        self.active = set()
        self.addCleanup(self.remove_owned_data)

    def remove_owned_data(self):
        self.assertFalse(self.active, "Preserve fixture data until exact container absence is established")
        self.assertEqual(self.work.resolve(), self.work)
        self.assertEqual(self.work.lstat().st_uid, os.getuid())
        for path in [self.work, *self.work.rglob("*")]:
            if path.is_dir() and not path.is_symlink():
                path.chmod(0o700)
        self.assertTrue(shutil.rmtree.avoids_symlink_attacks)
        shutil.rmtree(self.work)

    def synthetic_tree(self, name):
        root = self.work / name
        root.mkdir(mode=0o700)
        for index, relative in enumerate(TARGETS):
            path = root / relative
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            path.write_text("<?php return 'original';\n")
            path.chmod(0o600)
            candidate = path.parent / ("candidate-" + str(index))
            candidate.write_text("synthetic replacement\n")
            candidate.chmod(0o600)
        return root

    def sealed_root(self):
        source = self.synthetic_tree("source")
        staged = self.work / "sealed"
        CLOSURE.copy_tree(source, staged, CLOSURE.inventory(source, os.getuid(), sealed=False))
        result = subprocess.run(["/usr/bin/sudo", "-n", "/usr/bin/mktemp", "-d",
                                 "/run/sdk-php-command-keys.XXXXXX"], capture_output=True, text=True,
                                check=True, timeout=15)
        root = Path(result.stdout.strip())
        self.assertEqual(root.parent, Path("/run"))
        self.assertRegex(root.name, r"\Asdk-php-command-keys\.[A-Za-z0-9]{6}\Z")
        metadata = root.lstat()
        self.assertEqual(metadata.st_uid, 0)
        self.assertTrue(stat.S_ISDIR(metadata.st_mode))
        self.assertEqual(stat.S_IMODE(metadata.st_mode), 0o700)
        self.assertEqual(root.resolve(), root)
        identity = metadata.st_dev, metadata.st_ino
        self.addCleanup(self.remove_root, root, identity)
        subprocess.run(["/usr/bin/sudo", "-n", "/usr/bin/cp", "-a", "--no-preserve=ownership", "--",
                        str(staged) + "/.", str(root) + "/"], check=True, capture_output=True, timeout=15)
        subprocess.run(["/usr/bin/sudo", "-n", "/usr/bin/chmod", "0555", "--", str(root)],
                       check=True, capture_output=True, timeout=15)
        self.assertEqual(CLOSURE.inventory(root, 0, sealed=True)["sha256"],
                         CLOSURE.inventory(staged, os.getuid(), sealed=True)["sha256"])
        return root

    def remove_root(self, root, identity):
        self.assertFalse(self.active, "Preserve the sealed root until exact container absence is established")
        metadata = root.lstat()
        self.assertEqual(root.resolve(), root)
        self.assertTrue(stat.S_ISDIR(metadata.st_mode))
        self.assertEqual(metadata.st_uid, 0)
        self.assertEqual((metadata.st_dev, metadata.st_ino), identity)
        subprocess.run(["/usr/bin/sudo", "-n", "/usr/bin/rm", "-rf", "--", str(root)],
                       check=True, capture_output=True, timeout=15)
        self.assertFalse(root.exists())

    @contextmanager
    def consumer(self, root, *, writable=False):
        state = self.work / ("state-" + secrets.token_hex(8))
        state.mkdir(mode=0o700)
        for name in ("docker", "records"):
            (state / name).mkdir(mode=0o700)
        resources = CONTAINERS.Containers(state / "docker", state / "records",
                                           str(os.getpid()) + "-" + str(time.time_ns()), secrets.token_hex(16),
                                           lambda role, identity: self.active.add(identity))
        observation = {"source": os.environ["SDK_PHP_COMMAND_KEY_BOUNDARY_SOURCE"],
                       "image_index": SUPPLY["php_index"], "writable_countercontrol": writable,
                       "container_removed": False}
        OBSERVATIONS.append(observation)
        try:
            arguments = boundary_arguments(root, writable=writable)
            identity = resources.create("consumer", arguments)
            observation["container_id"] = identity
            COHORT.selected_image(resources.owned(identity, "consumer"), SUPPLY, "php")
            resources.start(identity, "consumer")
            yield resources, identity
        finally:
            try:
                resources.remove_all(reversed(list(resources.identities.items())))
                self.active.difference_update(resources.identities.values())
                observation["container_removed"] = True
            finally:
                resources.close()

    def probe(self, resources, identity):
        result = resources.command(["exec", identity, "php", "-c", "/usr/local/etc/php/php.ini-production", "-r", PROBE], timeout=20)
        self.assertEqual(result.returncode, 0)
        self.assertLess(len(result.stdout), 4096)
        outcomes = json.loads(result.stdout)
        OBSERVATIONS[-1]["replacement_outcomes"] = outcomes
        return outcomes

    def genuine_php(self, resources, identity):
        result = resources.command(["exec", identity, "php", "-c", "/usr/local/etc/php/php.ini-production", "-r",
                                    "echo json_encode(require '/closure/fixture/nested/leaf.php');"], timeout=10)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(json.loads(result.stdout), "original")
        OBSERVATIONS[-1]["genuine_include_unchanged"] = True

    def test_root_owned_sealed_closure_refuses_all_replacements_and_keeps_genuine_php(self):
        root = self.sealed_root()
        before = CLOSURE.inventory(root, 0, sealed=True)
        with self.consumer(root) as (resources, identity):
            COHORT.consumer_record(resources, identity, SUPPLY, root)
            self.assertEqual(self.probe(resources, identity), [False] * (2 * len(TARGETS)))
            self.genuine_php(resources, identity)
            COHORT.consumer_record(resources, identity, SUPPLY, root)
        self.assertEqual(CLOSURE.inventory(root, 0, sealed=True), before)

    def test_writable_permissions_still_refuse_on_readonly_mount_and_countercontrol_detects_rw(self):
        root = self.synthetic_tree("writable-data")
        before = CLOSURE.inventory(root, os.getuid(), sealed=False)
        with self.consumer(root) as (resources, identity):
            COHORT.consumer_record(resources, identity, SUPPLY, root)
            self.assertEqual(self.probe(resources, identity), [False] * (2 * len(TARGETS)))
            self.genuine_php(resources, identity)
        self.assertEqual(CLOSURE.inventory(root, os.getuid(), sealed=False), before)
        # This negative primitive receives only generated marker data. It is
        # deliberately rejected by the production guard and never admitted.
        with self.consumer(root, writable=True) as (resources, identity):
            with self.assertRaisesRegex(RuntimeError, "mounted read-only"):
                COHORT.consumer_record(resources, identity, SUPPLY, root)
            OBSERVATIONS[-1]["production_guard_refused"] = True
            self.assertEqual(self.probe(resources, identity), [True] * (2 * len(TARGETS)))
        self.assertNotEqual(CLOSURE.inventory(root, os.getuid(), sealed=False), before)


if __name__ == "__main__":
    result = unittest.main(exit=False).result
    if IMAGE:
        print(json.dumps({"scope": "synthetic-mount-controls-only", "observations": OBSERVATIONS}))
    raise SystemExit(not result.wasSuccessful())
