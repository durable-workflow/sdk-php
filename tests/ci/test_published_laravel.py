import copy
import json
from pathlib import Path
import tempfile
import unittest

from check_published_laravel import check, FRAMEWORKS


class PublishedLaravelTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.version = "3.18.3"
        self.reference = "a" * 40
        self.manifest = {"require": {"durable-workflow/sdk": self.version, "laravel/framework": "12.72.0"}}
        self.rows = []
        for name, version, reference in [("durable-workflow/sdk", self.version, self.reference),
                                         ("laravel/framework", "v12.72.0", FRAMEWORKS["12.72.0"])]:
            self.rows.append({"name": name, "version": version, "source": {"reference": reference},
                              "dist": {"reference": reference, "type": "zip"}, "installation-source": "dist"})

    def write(self, rows=None):
        documents = {"composer.json": self.manifest, "composer.lock": {"packages": self.rows},
                     "vendor/composer/installed.json": {"packages": rows if rows is not None else self.rows},
                     "vendor/durable-workflow/sdk/composer.json": {"extra": {"durable-workflow": {"product-train": self.version}}}}
        for name, document in documents.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(document))

    def test_matching_registry_install(self):
        self.write()
        self.assertFalse(check(self.root, self.version, self.reference, "12.72.0")["source_fallback"])

    def test_installed_metadata_cannot_borrow_the_lock_identity(self):
        for name in ["durable-workflow/sdk", "laravel/framework"]:
            for mutation in ["version", "source", "dist", "installation-source"]:
                with self.subTest(name=name, mutation=mutation):
                    rows = copy.deepcopy(self.rows)
                    row = next(row for row in rows if row["name"] == name)
                    if mutation in ["source", "dist"]:
                        row[mutation]["reference"] = "b" * 40
                    else:
                        row[mutation] = "source" if mutation == "installation-source" else "99.0.0"
                    self.write(rows)
                    with self.assertRaises(ValueError):
                        check(self.root, self.version, self.reference, "12.72.0")

    def test_duplicate_and_repository_override_refuse(self):
        self.write(self.rows + [self.rows[0]])
        with self.assertRaises(ValueError):
            check(self.root, self.version, self.reference, "12.72.0")
        self.manifest["repositories"] = [{"type": "path", "url": "/synthetic"}]
        self.write()
        with self.assertRaises(ValueError):
            check(self.root, self.version, self.reference, "12.72.0")


if __name__ == "__main__":
    unittest.main()
