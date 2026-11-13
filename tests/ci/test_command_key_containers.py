"""Inert captured-ID lifecycle controls; Docker is always replaced by a stub."""

import importlib.util
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


SOURCE = Path(__file__).resolve().parents[1] / "smoke/command_key_containers.py"
SPEC = importlib.util.spec_from_file_location("command_key_containers", SOURCE)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
IDENTITY = "a" * 64
NONCE = "b" * 32


def result(code=0, output="", error=""):
    return SimpleNamespace(returncode=code, stdout=output, stderr=error)


def owned_record(role="server"):
    return {"Id": IDENTITY, "Config": {"Labels": {
        MODULE.OWNER: "123-1", MODULE.ROLE: role, MODULE.NONCE: NONCE}}}


class ContainerControls(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.configuration, self.state = [self.root / name for name in ("docker", "state")]
        self.configuration.mkdir(mode=0o700)
        self.state.mkdir(mode=0o700)
        self.resources = MODULE.Containers(self.configuration, self.state, "123-1", NONCE)
        self.addCleanup(self.resources.close)

    def test_only_exact_missing_object_forms_are_absence(self):
        for prefix in ("Error: No such object: ", "error: no such object: "):
            missing = result(1, "[]\n", prefix + IDENTITY + "\n")
            self.assertTrue(MODULE.exact_missing(missing, IDENTITY))
            with patch.object(self.resources, "command", return_value=missing):
                self.assertIsNone(self.resources.inspect(IDENTITY, allow_missing=True))
            for changed in (result(125, missing.stdout, missing.stderr),
                            result(1, "", missing.stderr), result(1, "[{}]\n", missing.stderr),
                            result(1, missing.stdout, missing.stderr + "permission denied\n"),
                            result(1, missing.stdout, "engine unavailable\n" + missing.stderr)):
                with self.subTest(changed=vars(changed)):
                    self.assertFalse(MODULE.exact_missing(changed, IDENTITY))
                    with patch.object(self.resources, "command", return_value=changed), self.assertRaises(RuntimeError):
                        self.resources.inspect(IDENTITY, allow_missing=True)

    def test_empty_local_docker_configuration_excludes_ambient_authority(self):
        with patch.dict(os.environ, {"DOCKER_AUTH_CONFIG": "inert", "DOCKER_CONTEXT": "foreign",
                                     "DOCKER_HOST": "tcp://example.invalid:2375"}):
            self.assertEqual(self.resources.environment(), {
                "PATH": "/usr/bin:/bin", "DOCKER_CONFIG": str(self.configuration),
                "DOCKER_HOST": "unix:///var/run/docker.sock"})
        (self.configuration / "config.json").write_text('{}')
        with self.assertRaisesRegex(RuntimeError, "remain empty"):
            self.resources.environment()

    def test_inspect_requires_full_exact_returned_identity(self):
        for identity in ("short", "name", "A" * 64):
            with self.assertRaises(RuntimeError):
                self.resources.inspect(identity)
        for rows in ([], [owned_record(), owned_record()], [owned_record() | {"Id": "c" * 64}]):
            with patch.object(self.resources, "command", return_value=result(output=json.dumps(rows))), \
                    self.assertRaises(RuntimeError):
                self.resources.inspect(IDENTITY)

    def test_pulled_image_accepts_config_id_or_explicit_index_descriptor(self):
        configuration = "sha256:" + "c" * 64
        index = "sha256:" + "d" * 64
        image = "php@" + index
        for record in ({"Id": configuration, "Os": "linux", "Architecture": "amd64"},
                       {"Id": index, "Os": "linux", "Architecture": "amd64", "Descriptor": {
                           "digest": index, "mediaType": "application/vnd.oci.image.index.v1+json"}}):
            with patch.object(self.resources, "command", side_effect=[result(), result(output=json.dumps([record]))]) as invoke:
                self.assertEqual(self.resources.pull(image, configuration), record)
            self.assertEqual(invoke.call_args_list[0].args[0], ["pull", "--platform", "linux/amd64", image])

    def test_index_image_refuses_wrong_digest_type_or_platform(self):
        configuration = "sha256:" + "c" * 64
        index = "sha256:" + "d" * 64
        valid = {"Id": index, "Os": "linux", "Architecture": "amd64", "Descriptor": {
            "digest": index, "mediaType": "application/vnd.oci.image.index.v1+json"}}
        changes = [{"Id": "sha256:" + "0" * 64}, {"Os": "windows"}, {"Architecture": "arm64"},
                   {"Descriptor": None}, {"Descriptor": {"digest": "sha256:" + "0" * 64,
                    "mediaType": "application/vnd.oci.image.index.v1+json"}},
                   {"Id": configuration, "Descriptor": {"digest": "sha256:" + "0" * 64,
                    "mediaType": "application/vnd.oci.image.index.v1+json"}},
                   {"Descriptor": {"digest": index, "mediaType": "application/vnd.oci.image.manifest.v1+json"}}]
        for change in changes:
            record = valid | change
            with self.subTest(change=change), patch.object(self.resources, "command", side_effect=[
                    result(), result(output=json.dumps([record]))]), self.assertRaisesRegex(RuntimeError, "configuration differs"):
                self.resources.pull("php@" + index, configuration)

    def test_image_tags_and_unselected_repositories_refuse_before_pull(self):
        for image in ("php:latest", "php:8.3", "other/image@sha256:" + "d" * 64):
            with self.subTest(image=image), patch.object(self.resources, "command") as invoke, self.assertRaises(RuntimeError):
                self.resources.pull(image, "sha256:" + "c" * 64)
            invoke.assert_not_called()

    def test_foreign_owner_role_and_nonce_are_preserved(self):
        for field in (MODULE.OWNER, MODULE.ROLE, MODULE.NONCE):
            record = owned_record()
            record["Config"]["Labels"][field] = "foreign"
            with patch.object(self.resources, "inspect", return_value=record), \
                    patch.object(self.resources, "command") as invoke, \
                    self.assertRaisesRegex(RuntimeError, "preserve the replacement"):
                self.resources.remove(IDENTITY, "server")
            invoke.assert_not_called()

    def test_create_captures_stdout_to_held_record_and_workflow_output(self):
        captured = []
        self.resources.record_created = lambda role, identity: captured.append((role, identity))
        with patch.object(self.resources, "command", return_value=result(output=IDENTITY + "\n")) as invoke, \
                patch.object(self.resources, "owned", return_value=owned_record()):
            self.assertEqual(self.resources.create("server", ["inert-image"]), IDENTITY)
        self.assertEqual(captured, [("server", IDENTITY)])
        self.assertEqual(json.loads((self.state / "server.json").read_text()), {
            "id": IDENTITY, "owner": "123-1", "role": "server", "nonce": NONCE})
        flags = invoke.call_args.args[0]
        self.assertIn("--read-only", flags)
        self.assertEqual(flags[flags.index("--memory") + 1], "1g")
        self.assertEqual(flags[flags.index("--cpus") + 1], "1")
        self.assertEqual(flags[flags.index("--pids-limit") + 1], "256")
        with self.assertRaises(RuntimeError):
            self.resources.create("server", ["inert-image"])

    def test_record_path_substitution_does_not_select_or_modify_replacement(self):
        original = self.root / "held-state"
        self.state.rename(original)
        self.state.mkdir(mode=0o700)
        replacement = self.state / "server.json"
        replacement.write_text('foreign sentinel')
        with patch.object(self.resources, "command", return_value=result(output=IDENTITY + "\n")), \
                patch.object(self.resources, "owned", return_value=owned_record()):
            self.resources.create("server", ["inert-image"])
        self.assertEqual(replacement.read_text(), 'foreign sentinel')
        self.assertEqual(json.loads((original / "server.json").read_text())["id"], IDENTITY)

    def test_preexisting_record_and_symlink_refuse_before_create(self):
        path = self.state / "server.json"
        target = self.root / "sentinel"
        target.write_text('unchanged')
        for symlink in (False, True):
            if symlink:
                path.symlink_to(target)
            else:
                path.write_text('existing')
            with patch.object(self.resources, "command") as invoke, self.assertRaises(OSError):
                self.resources.create("server", ["inert-image"])
            invoke.assert_not_called()
            path.unlink()
        self.assertEqual(target.read_text(), 'unchanged')

    def test_state_replacement_before_directory_open_refuses(self):
        original_open = MODULE.os.open
        displaced = self.root / "displaced-state"

        def replace_before_open(path, flags, *arguments, **keywords):
            if path == self.state:
                self.state.rename(displaced)
                self.state.mkdir(mode=0o700)
            return original_open(path, flags, *arguments, **keywords)

        with patch.object(MODULE.os, "open", side_effect=replace_before_open), \
                self.assertRaisesRegex(RuntimeError, "changed before it was held"):
            MODULE.Containers(self.configuration, self.state, "123-1", NONCE)

    def test_nonzero_create_with_full_id_remains_available_for_cleanup(self):
        captured = []
        self.resources.record_created = lambda *value: captured.append(value)
        with patch.object(self.resources, "command", return_value=result(124, IDENTITY + "\n")), \
                self.assertRaisesRegex(RuntimeError, "after returning"):
            self.resources.create("server", ["inert-image"])
        self.assertEqual(captured, [("server", IDENTITY)])
        self.assertEqual(self.resources.identities, {"server": IDENTITY})

    def test_timed_out_create_still_records_a_complete_stdout_id(self):
        captured = []
        self.resources.record_created = lambda *value: captured.append(value)
        with patch.object(self.resources, "command", side_effect=MODULE.FixtureTimeout((IDENTITY + "\n").encode())), \
                self.assertRaisesRegex(RuntimeError, "after returning"):
            self.resources.create("server", ["inert-image"])
        self.assertEqual(captured, [("server", IDENTITY)])

    def test_ambiguous_create_stdout_is_never_interpreted_as_a_name_or_id(self):
        for output in ("name\n", IDENTITY, IDENTITY + "\nother\n"):
            with self.subTest(output=output), tempfile.TemporaryDirectory() as temporary:
                state = Path(temporary)
                resources = MODULE.Containers(self.configuration, state, "123-1", NONCE)
                try:
                    with patch.object(resources, "command", return_value=result(output=output)), self.assertRaises(RuntimeError):
                        resources.create("server", ["inert-image"])
                    self.assertEqual(resources.identities, {})
                finally:
                    resources.close()

    def test_cleanup_uses_captured_id_and_observes_exact_absence(self):
        absent = result(1, "[]\n", "error: no such object: " + IDENTITY + "\n")
        with patch.object(self.resources, "command", side_effect=[
                result(output=json.dumps([owned_record()])), result(), absent]) as invoke:
            self.resources.remove(IDENTITY, "server")
        self.assertEqual([call.args[0] for call in invoke.call_args_list], [
            ["inspect", IDENTITY], ["rm", "--force", "--volumes", IDENTITY], ["inspect", IDENTITY]])

    def test_one_cleanup_refusal_does_not_skip_the_other_role(self):
        pairs = [("consumer", "c" * 64), ("server", IDENTITY)]
        with patch.object(self.resources, "remove", side_effect=[RuntimeError("preserve"), None]) as remove, \
                self.assertRaisesRegex(RuntimeError, "could not be removed"):
            self.resources.remove_all(pairs)
        self.assertEqual([call.args for call in remove.call_args_list], [(identity, role) for role, identity in pairs])


if __name__ == "__main__":
    unittest.main()
