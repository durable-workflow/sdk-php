#!/usr/bin/env python3
"""Exercise channel selection against real Git histories and an offline release API."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


GUARD = Path(__file__).with_name("check-docs-release-channel.py").resolve()
PREPARE = Path(__file__).with_name("prepare-docs-tooling.py").resolve()
BIND = Path(__file__).with_name("bind-docs-artifact.py").resolve()


class DocumentationChannelTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "source"
        self.root.mkdir()
        self.bin = Path(self.temporary.name) / "bin"
        self.bin.mkdir()
        self.output = Path(self.temporary.name) / "outputs"
        self.environment = {
            "PATH": str(self.bin) + os.pathsep + os.environ["PATH"],
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GITHUB_REPOSITORY": "fixture/sdk-php",
            "GITHUB_OUTPUT": str(self.output),
            "FAKE_RELEASE": json.dumps({"isDraft": False, "isPrerelease": False}),
        }
        gh = self.bin / "gh"
        gh.write_text(
            "#!/usr/bin/env python3\n"
            "import os,sys\n"
            "expected=['release','view','3.9.4','--repo','fixture/sdk-php','--json','isDraft,isPrerelease']\n"
            "if sys.argv[1:]!=expected:sys.exit(9)\n"
            "print(os.environ['FAKE_RELEASE'])\n"
            "sys.exit(int(os.environ.get('FAKE_RELEASE_STATUS','0')))\n"
        )
        gh.chmod(0o700)
        self.git("init", "--object-format=sha1", "-b", "main")
        self.git("config", "user.name", "Documentation test")
        self.git("config", "user.email", "docs@example.test")
        self.manifest = {"require": {"php": "^8.1"}, "extra": {"durable-workflow": {"product-train": "3.9.4"}}}
        self.write_manifest()
        for path in ["src/Client.php", "examples/worker.php", "resources/quickstart.json", "config/sdk.php", "package.json", "package-lock.json", "composer.lock", "docs/index.md"]:
            self.write(path, "published\n")
        self.published = self.commit()
        self.git("tag", "-a", "3.9.4", "-m", "Published SDK")
        self.git("update-ref", "refs/remotes/origin/main", self.published)

    def git(self, *arguments: str) -> str:
        result = subprocess.run(["git", *arguments], cwd=self.root, env=self.environment, text=True, capture_output=True, check=True)
        return result.stdout.strip()

    def write(self, name: str, content: str) -> None:
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    def write_manifest(self) -> None:
        self.write("composer.json", json.dumps(self.manifest))

    def commit(self) -> str:
        self.git("add", ".")
        self.git("commit", "-m", "Fixture source")
        return self.git("rev-parse", "HEAD")

    def changed(self, path: str) -> str:
        self.write(path, "changed\n")
        return self.commit()

    def channel(self, stable: bool | None, **environment: str) -> None:
        before = self.git("rev-parse", "HEAD")
        self.output.unlink(missing_ok=True)
        result = subprocess.run([sys.executable, str(GUARD)], cwd=self.root, env=self.environment | environment, text=True, capture_output=True)
        if stable is None:
            self.assertNotEqual(0, result.returncode, result.stdout)
            self.assertFalse(self.output.exists())
        else:
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertEqual(f"stable={'true' if stable else 'false'}\nsource_revision={before}\n", self.output.read_text())
        self.assertEqual(before, self.git("rev-parse", "HEAD"), "Channel selection must not change the audited source.")

    def testPublishedStableTagIsStable(self) -> None:
        self.channel(True)
        self.channel(True, STABLE_TAG="3.9.4")

    def test_tooling_is_exact_and_separate_from_an_old_selected_tag(self) -> None:
        self.write("scripts/check-docs-release-channel.py", GUARD.read_text())
        self.write("package.json", json.dumps({"scripts": {"test:docs-portal-links": "workflow-owned"}}))
        workflow = self.commit()
        self.git("checkout", "--detach", self.published)
        self.assertFalse((self.root / "scripts/check-docs-release-channel.py").exists())
        selected_manifest = (self.root / "package.json").read_bytes()
        tooling = Path(self.temporary.name) / "tooling"
        prepared = subprocess.run(
            [sys.executable, str(PREPARE), workflow, str(tooling)],
            cwd=self.root, env=self.environment, text=True, capture_output=True,
        )
        self.assertEqual(0, prepared.returncode, prepared.stderr)
        self.assertEqual(workflow, subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=tooling, env=self.environment, text=True,
        ).strip())
        self.assertIn("test:docs-portal-links", (tooling / "package.json").read_text())
        result = subprocess.run(
            [sys.executable, str(tooling / "scripts/check-docs-release-channel.py")],
            cwd=self.root, env=self.environment | {"STABLE_TAG": "3.9.4"}, text=True, capture_output=True,
        )
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(f"stable=true\nsource_revision={self.published}\n", self.output.read_text())
        self.assertEqual(selected_manifest, (self.root / "package.json").read_bytes())
        self.assertEqual("", self.git("diff", "HEAD"))
        self.git("worktree", "remove", str(tooling))

    def test_tooling_refuses_a_mutable_revision_or_existing_directory(self) -> None:
        for revision, destination in [("main", Path(self.temporary.name) / "new"), (self.published, self.root)]:
            with self.subTest(revision=revision):
                result = subprocess.run(
                    [sys.executable, str(PREPARE), revision, str(destination)],
                    cwd=self.root, env=self.environment, text=True, capture_output=True,
                )
                self.assertNotEqual(0, result.returncode)
                self.assertEqual(self.published, self.git("rev-parse", "HEAD"))

    def test_artifact_audit_uses_exact_selected_source(self) -> None:
        for source in [self.published, self.changed("docs/index.md")]:
            with self.subTest(source=source):
                self.git("checkout", "--detach", source)
                (self.root / "build/site").mkdir(parents=True, exist_ok=True)
                result = subprocess.run([sys.executable, str(BIND), source], cwd=self.root, env=self.environment, capture_output=True)
                self.assertEqual(0, result.returncode, result.stderr)
                self.assertEqual({"schema": "durable-workflow.sdk-php.docs-deployment/v1", "source_revision": source},
                                 json.loads((self.root / "build/site/deployment-audit.json").read_text()))

    def test_artifact_refuses_wrong_identity_and_modified_selected_source(self) -> None:
        (self.root / "build/site").mkdir(parents=True)
        for expected, dirty in [("main", False), ("a" * 40, False), (self.published, True)]:
            with self.subTest(expected=expected, dirty=dirty):
                if dirty:
                    self.write("docs/index.md", "uncommitted documentation")
                result = subprocess.run([sys.executable, str(BIND), expected], cwd=self.root, env=self.environment, capture_output=True)
                self.assertNotEqual(0, result.returncode)
                self.assertFalse((self.root / "build/site/deployment-audit.json").exists())

    def testDocumentationOnlyMainChangesRemainStable(self) -> None:
        self.changed("docs/index.md")
        self.channel(True)

    def testStableLookingTrainDoesNotPublishChangedProductPaths(self) -> None:
        for path in ["src/Client.php", "examples/worker.php", "resources/quickstart.json", "config/sdk.php", "package.json", "package-lock.json", "composer.lock"]:
            with self.subTest(path=path):
                self.git("reset", "--hard", self.published)
                self.changed(path)
                self.channel(False)

    def testManifestChangesRemainPreview(self) -> None:
        self.manifest["require"]["php"] = "^8.2"
        self.write_manifest()
        self.commit()
        self.channel(False)

    def testWorkingProductChangesCannotPassAsTheTag(self) -> None:
        self.write("src/Client.php", "uncommitted API\n")
        self.channel(False)
        self.channel(None, STABLE_TAG="3.9.4")

    def testUntrackedProductFilesRemainPreview(self) -> None:
        self.write("src/FutureApi.php", "future API\n")
        self.channel(False)

    def testMissingPublishedTagRemainsPreview(self) -> None:
        self.git("tag", "-d", "3.9.4")
        self.channel(False)

    def testUnpublishedDraftPrereleaseAndMalformedReleasesRemainPreview(self) -> None:
        for release, status in [("{}", "1"), ('{"isDraft":true,"isPrerelease":false}', "0"), ('{"isDraft":false,"isPrerelease":true}', "0"), ("{}", "0"), ('{"isDraft":0,"isPrerelease":false}', "0"), ("not JSON", "0")]:
            with self.subTest(release=release, status=status):
                self.channel(False, FAKE_RELEASE=release, FAKE_RELEASE_STATUS=status)
                self.channel(None, STABLE_TAG="3.9.4", FAKE_RELEASE=release, FAKE_RELEASE_STATUS=status)

    def testPrereleaseTrainRemainsPreview(self) -> None:
        self.manifest["extra"]["durable-workflow"]["product-train"] = "3.10.0-rc.1"
        self.write_manifest()
        self.commit()
        self.channel(False)

    def testExplicitTagRejectsADifferentSelectedCommit(self) -> None:
        self.changed("docs/index.md")
        self.channel(None, STABLE_TAG="3.9.4")

    def testReviewedDocumentationOverrideWorksWithoutAGuardInTheOldTag(self) -> None:
        self.assertFalse((self.root / "scripts/check-docs-release-channel.py").exists())
        source = self.changed("docs/index.md")
        self.git("update-ref", "refs/remotes/origin/main", source)
        self.channel(True, STABLE_TAG="3.9.4", STABLE_SOURCE=source)

    def testExplicitSourceRequiresExactMainAncestryAndIdentity(self) -> None:
        source = self.changed("docs/index.md")
        self.channel(None, STABLE_TAG="3.9.4", STABLE_SOURCE=source)
        self.git("update-ref", "refs/remotes/origin/main", source)
        self.channel(None, STABLE_TAG="3.9.4", STABLE_SOURCE=self.published)

    def testExplicitSourceRejectsProductAndManifestChanges(self) -> None:
        for path in ["src/Client.php", "examples/worker.php", "resources/quickstart.json", "config/sdk.php"]:
            with self.subTest(path=path):
                self.git("reset", "--hard", self.published)
                source = self.changed(path)
                self.git("update-ref", "refs/remotes/origin/main", source)
                self.channel(None, STABLE_TAG="3.9.4", STABLE_SOURCE=source)
        self.git("reset", "--hard", self.published)
        self.manifest["require"]["php"] = "^8.2"
        self.write_manifest()
        source = self.commit()
        self.git("update-ref", "refs/remotes/origin/main", source)
        self.channel(None, STABLE_TAG="3.9.4", STABLE_SOURCE=source)

    def testExplicitSourceRetainsStrictManifestTypesAndOrder(self) -> None:
        # The prior PHP !== comparison rejects changed JSON value types/order.
        self.manifest["require"]["php"] = 81
        self.write_manifest()
        source = self.commit()
        self.git("update-ref", "refs/remotes/origin/main", source)
        self.channel(None, STABLE_TAG="3.9.4", STABLE_SOURCE=source)
        self.git("reset", "--hard", self.published)
        self.manifest = {"extra": self.manifest["extra"], "require": {"php": "^8.1"}}
        self.write_manifest()
        source = self.commit()
        self.git("update-ref", "refs/remotes/origin/main", source)
        self.channel(None, STABLE_TAG="3.9.4", STABLE_SOURCE=source)

    def testInvalidExplicitInputsFailClosed(self) -> None:
        for inputs in [{"STABLE_TAG": "3.9.4-rc.1"}, {"STABLE_TAG": "../main"}, {"STABLE_SOURCE": self.published}, {"STABLE_TAG": "3.9.4", "STABLE_SOURCE": "HEAD"}, {"STABLE_TAG": "3.9.4", "STABLE_SOURCE": self.published.upper()}, {"STABLE_TAG": "3.9.3"}]:
            with self.subTest(inputs=inputs):
                self.channel(None, **inputs)


if __name__ == "__main__":
    unittest.main()
