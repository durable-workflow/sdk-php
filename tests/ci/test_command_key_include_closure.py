"""Inert include-tree inventory controls; no package code or runtime loading."""

import importlib.util
import os
from pathlib import Path
import tempfile
import unittest


SOURCE = Path(__file__).resolve().parents[1] / "smoke/command_key_include_closure.py"
SPEC = importlib.util.spec_from_file_location("command_key_include_closure", SOURCE)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


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
        (self.fixture / "command-key-client.php").write_text('inert fixture bytes')
        (self.fixture / "command-key-supply.json").write_text('{}')
        self.addCleanup(self.unseal)

    def unseal(self):
        for path in [self.root, *self.root.rglob("*")]:
            if path.is_dir() and not path.is_symlink():
                path.chmod(0o700)

    def stage(self):
        return MODULE.stage(self.consumer, self.fixture, self.output)

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
        with self.assertRaisesRegex(ValueError, "write permissions"):
            MODULE.inventory(self.output, os.getuid(), sealed=True)
        member.chmod(0o400)
        with self.assertRaisesRegex(ValueError, "ownership"):
            MODULE.inventory(self.output, os.getuid() + 1, sealed=True)


if __name__ == "__main__":
    unittest.main()
