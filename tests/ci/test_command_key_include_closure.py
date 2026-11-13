"""Inert include-tree inventory controls; no package code or runtime loading."""

import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile


SOURCE = Path(__file__).resolve().parents[1] / "smoke/command_key_include_closure.py"
SPEC = importlib.util.spec_from_file_location("command_key_include_closure", SOURCE)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
SUPPLY_SPEC = importlib.util.spec_from_file_location("command_key_supply", SOURCE.with_name("verify_command_key_supply.py"))
SUPPLY = importlib.util.module_from_spec(SUPPLY_SPEC)
SUPPLY_SPEC.loader.exec_module(SUPPLY)


class IncludeClosureControls(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.consumer, self.fixture, self.output = [self.root / name for name in ("consumer", "fixture", "output")]
        for path in (self.consumer, self.fixture, self.output):
            path.mkdir(mode=0o700)
        (self.consumer / "composer.json").write_text('{}')
        (self.consumer / "composer.lock").write_text('{}')
        (self.consumer / "vendor/composer").mkdir(parents=True)
        (self.consumer / "vendor/autoload.php").write_text('inert autoload bytes')
        (self.consumer / "vendor/composer/installed.json").write_text('{}')
        (self.consumer / "vendor/composer/nested.php").write_text('inert nested include bytes')
        self.sdk = self.consumer / "vendor/durable-workflow/sdk"
        (self.sdk / "src").mkdir(parents=True)
        (self.sdk / "src/Client.php").write_bytes(b"safe!")
        self.approved_sdk = SUPPLY.member_inventory({"src/Client.php": (b"safe!", 0o644)})
        self.approved_digest = MODULE.members_digest(self.approved_sdk)
        self.supply = {"sdk_version": "1.2.3", "sdk_reference": "a" * 40,
                       "sdk_source_url": "https://example.invalid/sdk.git", "sdk_dist_url": "https://example.invalid/sdk.zip",
                       "sdk_archive_members_sha256": self.approved_digest}
        package = {"name": "durable-workflow/sdk", "version": "1.2.3",
                   "source": {"url": self.supply["sdk_source_url"], "reference": self.supply["sdk_reference"]},
                   "dist": {"url": self.supply["sdk_dist_url"], "reference": self.supply["sdk_reference"]}}
        (self.consumer / "composer.json").write_text(json.dumps({"require": {"durable-workflow/sdk": "1.2.3"}}))
        (self.consumer / "composer.lock").write_text(json.dumps({"packages": [package]}))
        (self.consumer / "vendor/composer/installed.json").write_text(json.dumps({"packages": [package | {
            "installation-source": "dist", "install-path": "../durable-workflow/sdk"}]}))
        self.archive = self.root / "approved.zip"
        member = zipfile.ZipInfo("sdk/src/Client.php")
        member.external_attr = 0o100644 << 16
        with zipfile.ZipFile(self.archive, "w") as archive:
            archive.writestr(member, b"safe!")
        (self.fixture / "command-key-client.php").write_text('inert fixture bytes')
        (self.fixture / "command-key-supply.json").write_text('{}')
        self.approved_fixture = SUPPLY.member_inventory({path.name: (path.read_bytes(), 0o644) for path in self.fixture.iterdir()})
        self.addCleanup(self.unseal)

    def unseal(self):
        for path in [self.root, *self.root.rglob("*")]:
            if path.is_dir() and not path.is_symlink():
                path.chmod(0o700)

    def stage(self):
        return MODULE.stage(self.consumer, self.fixture, self.output, self.approved_sdk,
                            self.approved_digest, self.approved_fixture)

    def test_complete_nested_include_inventory_and_readonly_copy(self):
        result = self.stage()
        self.assertEqual(MODULE.inventory(self.output, os.getuid(), sealed=True), result)
        member = result["entries"]["consumer/vendor/composer/nested.php"]
        self.assertEqual(member["bytes"], len(b'inert nested include bytes'))
        self.assertFalse(member["mode"] & 0o222)
        self.assertIn("fixture/command-key-client.php", result["entries"])

    def test_alias_and_hardlink_leaves_refuse(self):
        target = self.consumer / "vendor/composer/alias.php"
        original = self.consumer / "vendor/autoload.php"
        target.symlink_to(original)
        with self.assertRaises(ValueError):
            self.stage()
        target.unlink()
        os.link(original, target)
        with self.assertRaises(ValueError):
            self.stage()

    def test_extra_root_member_refuses(self):
        (self.consumer / "unreviewed.php").write_text('inert')
        with self.assertRaisesRegex(ValueError, "Unexpected consumer"):
            self.stage()

    def test_extra_fixture_and_missing_autoloader_refuse(self):
        extra = self.fixture / "unreviewed.php"
        extra.write_text('inert')
        with self.assertRaisesRegex(ValueError, "Unexpected captured fixture"):
            self.stage()
        extra.unlink()
        (self.consumer / "vendor/autoload.php").unlink()
        with self.assertRaisesRegex(ValueError, "complete installed autoloader"):
            self.stage()

    def test_repository_metadata_inside_a_dependency_refuses(self):
        (self.consumer / "vendor/composer/.git").write_text('inert metadata')
        with self.assertRaisesRegex(ValueError, "Repository metadata"):
            self.stage()

    def test_nested_byte_change_changes_complete_inventory(self):
        before = MODULE.inventory(self.consumer, os.getuid(), sealed=False)
        (self.consumer / "vendor/composer/nested.php").write_text('changed inert bytes')
        after = MODULE.inventory(self.consumer, os.getuid(), sealed=False)
        self.assertNotEqual(before["sha256"], after["sha256"])

    def test_writable_sealed_member_and_wrong_owner_refuse(self):
        self.stage()
        member = self.output / "consumer/vendor/autoload.php"
        member.chmod(0o600)
        owner = os.getuid()
        with self.assertRaisesRegex(ValueError, "write permissions"):
            MODULE.inventory(self.output, owner, sealed=True)
        member.chmod(0o400)
        with self.assertRaisesRegex(ValueError, "ownership"):
            MODULE.inventory(self.output, owner + 1, sealed=True)

    def test_post_verification_same_size_sdk_replacement_is_not_reapproved(self):
        evidence, approved = SUPPLY.verify_with_members(self.consumer, self.archive, self.supply)
        self.assertEqual(evidence["archive_members_sha256"], self.approved_digest)
        (self.sdk / "src/Client.php").write_bytes(b"evil!")
        with self.assertRaisesRegex(ValueError, "approved authority"):
            MODULE.stage(self.consumer, self.fixture, self.output, approved,
                         evidence["archive_members_sha256"], self.approved_fixture)
        self.assertEqual(list(self.output.iterdir()), [])

    def test_sdk_replacement_after_inventory_is_checked_through_source_descriptor(self):
        original_copy = MODULE.copy_tree

        def replace_then_copy(source, destination, expected):
            if source == self.consumer:
                (self.sdk / "src/Client.php").write_bytes(b"evil!")
            return original_copy(source, destination, expected)

        with patch.object(MODULE, "copy_tree", side_effect=replace_then_copy), \
                self.assertRaisesRegex(ValueError, "changed before copying"):
            self.stage()

    def test_captured_fixture_replacement_is_not_reapproved(self):
        (self.fixture / "command-key-client.php").write_text('changed fixture bytes')
        with self.assertRaisesRegex(ValueError, "approved authority"):
            self.stage()

    def test_sealed_sdk_recheck_uses_archive_authority_even_with_new_include_hash(self):
        before = self.stage()
        leaf = self.output / "consumer/vendor/durable-workflow/sdk/src/Client.php"
        leaf.chmod(0o600)
        leaf.write_bytes(b"evil!")
        leaf.chmod(0o444)
        changed = MODULE.inventory(self.output, os.getuid(), sealed=True)
        self.assertNotEqual(before["sha256"], changed["sha256"])
        with self.assertRaisesRegex(ValueError, "approved archive member authority"):
            MODULE.verify_sealed_sdk(changed, self.approved_digest)

    def test_approved_executable_mode_is_deterministic_after_private_umask(self):
        leaf = self.sdk / "src/Client.php"
        leaf.chmod(0o600)
        self.approved_sdk["src/Client.php"]["mode"] = 0o755
        self.approved_digest = MODULE.members_digest(self.approved_sdk)
        result = self.stage()
        self.assertEqual(result["entries"]["consumer/vendor/durable-workflow/sdk/src/Client.php"]["mode"], 0o555)
        MODULE.verify_sealed_sdk(result, self.approved_digest)

    def test_extra_sdk_empty_directory_and_file_refuse(self):
        for directory in (False, True):
            extra = self.sdk / "extra"
            if directory:
                extra.mkdir()
            else:
                extra.write_bytes(b"inert")
            with self.subTest(directory=directory), self.assertRaisesRegex(ValueError, "member set|directory set"):
                self.stage()
            if directory:
                extra.rmdir()
            else:
                extra.unlink()

    def test_changed_approved_map_does_not_change_the_selected_archive_authority(self):
        self.approved_sdk["src/Client.php"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "staging SDK authority"):
            self.stage()


if __name__ == "__main__":
    unittest.main()
