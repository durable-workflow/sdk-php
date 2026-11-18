"""Inert archive/installation provenance controls; no PHP or network."""

from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import stat
import tempfile
import unittest
import zipfile


SOURCE = Path(__file__).resolve().parents[1] / "smoke/verify_command_key_supply.py"
SPEC = importlib.util.spec_from_file_location("command_key_supply", SOURCE)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class CommandKeySupplyTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="sdk-php-supply-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.consumer = self.root / "consumer"
        self.package = self.consumer / "vendor/durable-workflow/sdk"
        (self.package / "src").mkdir(parents=True)
        (self.consumer / "vendor/composer").mkdir(parents=True)
        self.data = b"<?php // inert fixture, never loaded\n"
        (self.package / "src/Client.php").write_bytes(self.data)
        (self.package / "src/Client.php").chmod(0o600)
        self.archive = self.root / "sdk.zip"
        member = zipfile.ZipInfo("sdk/src/Client.php")
        member.external_attr = 0o100644 << 16
        with zipfile.ZipFile(self.archive, "w") as archive:
            archive.writestr(member, self.data)
        self.supply = {"sdk_version": "1.2.3", "sdk_reference": "a" * 40,
                       "sdk_source_url": "https://example.invalid/sdk.git",
                       "sdk_dist_url": "https://example.invalid/sdk.zip",
                       "sdk_archive_members_sha256": MODULE.member_digest({"src/Client.php": (self.data, 0o644)})}
        entry = {"name": "durable-workflow/sdk", "version": "1.2.3",
                 "source": {"url": self.supply["sdk_source_url"], "reference": self.supply["sdk_reference"]},
                 "dist": {"url": self.supply["sdk_dist_url"], "reference": self.supply["sdk_reference"]}}
        self.lock = {"packages": [entry]}
        self.installed = {"packages": [deepcopy(entry) | {
            "installation-source": "dist", "install-path": "../durable-workflow/sdk"}]}
        self.manifest = {"require": {"durable-workflow/sdk": "1.2.3"}}
        self.write_metadata()

    def write_metadata(self):
        for name, document in (("composer.json", self.manifest), ("composer.lock", self.lock),
                               ("vendor/composer/installed.json", self.installed)):
            (self.consumer / name).write_text(json.dumps(document))

    def verify(self):
        return MODULE.verify(self.consumer, self.archive, self.supply)

    def test_exact_dist_and_private_umask_pass(self):
        result = self.verify()
        self.assertEqual(1, result["installed_files"])
        self.assertEqual(len(self.data), result["installed_bytes"])
        self.assertFalse(result["source_fallback"])

    def test_github_dos_members_have_nonexecutable_default_permissions(self):
        member = zipfile.ZipInfo("sdk/ordinary.php")
        member.create_system = 0
        member.external_attr = 0
        self.assertEqual(("ordinary.php", 0o644), MODULE.archive_member(member))

    def test_archive_members_must_equal_the_approved_inventory(self):
        self.supply["sdk_archive_members_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "approved SDK supply"):
            self.verify()

    def test_regenerated_wrapper_is_allowed_only_with_identical_members(self):
        original = self.verify()
        member = zipfile.ZipInfo("different-generated-root/src/Client.php", date_time=(2026, 1, 1, 0, 0, 0))
        member.external_attr = 0o100644 << 16
        with zipfile.ZipFile(self.archive, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.comment = b"inert wrapper change"
            archive.writestr(member, self.data)
        regenerated = self.verify()
        self.assertNotEqual(original["archive_sha256"], regenerated["archive_sha256"])
        self.assertEqual(original["archive_members_sha256"], regenerated["archive_members_sha256"])

    def test_changed_member_bytes_modes_and_inventory_refuse(self):
        for raw, mode, extra in ((b"changed", 0o644, False), (self.data, 0o755, False), (self.data, 0o644, True)):
            member = zipfile.ZipInfo("sdk/src/Client.php")
            member.external_attr = (stat.S_IFREG | mode) << 16
            with zipfile.ZipFile(self.archive, "w") as archive:
                archive.writestr(member, raw)
                if extra:
                    archive.writestr("sdk/extra.php", b"inert")
            with self.subTest(mode=mode, extra=extra), self.assertRaisesRegex(ValueError, "approved SDK supply"):
                self.verify()

    def test_changed_installed_module_refuses(self):
        (self.package / "src/Client.php").write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "bytes differ"):
            self.verify()

    def test_extra_and_missing_installed_files_refuse(self):
        extra = self.package / "unapproved.php"
        extra.write_bytes(b"inert")
        with self.assertRaisesRegex(ValueError, "unapproved file"):
            self.verify()
        extra.unlink()
        (self.package / "src/Client.php").unlink()
        with self.assertRaisesRegex(ValueError, "inventory is incomplete"):
            self.verify()

    def test_wrong_lock_source_or_dist_identity_refuses(self):
        for field in ("source", "dist"):
            original = deepcopy(self.lock)
            self.lock["packages"][0][field]["reference"] = "b" * 40
            self.write_metadata()
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "identities differ"):
                self.verify()
            self.lock = original

    def test_local_dist_url_and_source_install_refuse(self):
        self.installed["packages"][0]["dist"]["url"] = "file:///fixture.zip"
        self.write_metadata()
        with self.assertRaisesRegex(ValueError, "identities differ"):
            self.verify()
        self.installed["packages"][0]["dist"]["url"] = self.supply["sdk_dist_url"]
        self.installed["packages"][0]["installation-source"] = "source"
        self.write_metadata()
        with self.assertRaisesRegex(ValueError, "registry dist installation"):
            self.verify()

    def test_repository_override_refuses(self):
        self.manifest["repositories"] = [{"type": "path", "url": "./source"}]
        self.write_metadata()
        with self.assertRaisesRegex(ValueError, "exact registry SDK"):
            self.verify()

    def test_extra_file_permissions_refuse(self):
        (self.package / "src/Client.php").chmod(0o666)
        with self.assertRaisesRegex(ValueError, "file mode exceeds"):
            self.verify()


if __name__ == "__main__":
    unittest.main()
