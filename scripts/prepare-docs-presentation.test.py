#!/usr/bin/env python3
"""Presentation staging must preserve published SDK and guide authority."""
import importlib.util
import json
import os
import sys
from pathlib import Path
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('presentation', Path(__file__).with_name('prepare-docs-presentation.py'))
presentation = importlib.util.module_from_spec(spec)
spec.loader.exec_module(presentation)


class PresentationTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / 'repo'
        self.root.mkdir()
        self.git('init', '-q')
        self.git('config', 'user.email', 'fixture@example.invalid')
        self.git('config', 'user.name', 'Source fixture')
        for name, value in {
            'composer.json': '{"published_version":"3.9.4"}', 'src/Client.php': 'published implementation',
            'docs/quickstart-contract.json': '{"published_version":"3.9.4"}',
            'docs/portal/guide.md': 'published support and examples',
            'docs/portal/_data/release.js': 'original data loader',
            '.phpdoc/template/index.html.twig': 'published API index',
        }.items(): self.write(name, value)
        for name in presentation.PRESENTATION_FILES: self.write(name, 'old presentation')
        self.write('docs/portal/assets/identity.css', 'old palette')
        self.git('add', '.')
        self.git('commit', '-qm', 'published SDK')
        self.sdk = self.git('rev-parse', 'HEAD')
        self.write('src/Client.php', 'unpublished future API')
        self.write('composer.json', '{"published_version":"3.10.0"}')
        self.write('docs/quickstart-contract.json', '{"published_version":"3.10.0"}')
        self.write('docs/portal/guide.md', 'unpublished future support floor')
        self.write('.phpdoc/template/index.html.twig', 'future static API index')
        self.write('docs/portal/assets/identity.css', 'new shared palette')
        self.write('.phpdoc/template/layout.html.twig', 'new shared layout')
        self.write('docs/portal/index.md', 'new landing page with release variables')
        self.git('add', '.')
        self.git('commit', '-qm', 'future SDK and shared presentation')
        self.design = self.git('rev-parse', 'HEAD')
        self.git('update-ref', 'refs/remotes/origin/main', self.design)
        self.git('checkout', '-q', '--detach', self.sdk)
        self.adapter = Path(self.temporary.name) / 'release.js'
        self.adapter.write_text('read release facts from the selected SDK directory')
        self.output = Path(self.temporary.name) / 'presentation'

    def git(self, *args):
        return subprocess.check_output(['git', '-C', str(self.root), *args], stderr=subprocess.PIPE).decode().strip()

    def write(self, name, value):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value)

    def prepare(self):
        return presentation.prepare(self.root, self.design, self.output, self.adapter)

    def testOnlyPresentationCanChangeAndBothExactSourcesAreRecorded(self):
        packet = self.prepare()
        self.assertEqual(packet['sdk_source_revision'], self.sdk)
        self.assertEqual(packet['presentation_source_revision'], self.design)
        self.assertEqual((self.output / 'portal/assets/identity.css').read_text(), 'new shared palette')
        self.assertEqual((self.output / 'portal/guide.md').read_text(), 'published support and examples')
        self.assertEqual((self.output / '.phpdoc/template/index.html.twig').read_text(), 'published API index')
        self.assertEqual((self.output / '.phpdoc/template/layout.html.twig').read_text(), 'new shared layout')
        self.assertEqual((self.root / 'src/Client.php').read_text(), 'published implementation')
        self.assertEqual(json.loads((self.root / 'composer.json').read_text())['published_version'], '3.9.4')
        self.assertEqual(self.git('status', '--porcelain'), '')
        presentation.verify(self.output, self.sdk, self.design)

    def testArtifactBindsPublishedPackageAndPresentationSeparately(self):
        self.prepare()
        self.git('tag', '3.9.4', self.sdk)
        (self.root / 'build/site').mkdir(parents=True)
        environment = os.environ | {'DOCS_PRESENTATION_DIRECTORY': str(self.output),
            'DOCS_PRESENTATION_REVISION': self.design, 'STABLE_TAG': '3.9.4'}
        binder = Path(__file__).with_name('bind-docs-artifact.py')
        result = subprocess.run([sys.executable, str(binder), self.sdk], cwd=self.root,
                                env=environment, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        audit = json.loads((self.root / 'build/site/deployment-audit.json').read_text())
        self.assertEqual(audit['source_revision'], self.sdk)
        self.assertEqual(audit['published_sdk_source_revision'], self.sdk)
        self.assertEqual(audit['published_sdk_version'], '3.9.4')
        self.assertEqual(audit['presentation_source_revision'], self.design)
        (self.output / 'portal/assets/identity.css').write_text('changed after build')
        result = subprocess.run([sys.executable, str(binder), self.sdk], cwd=self.root,
                                env=environment, capture_output=True)
        self.assertNotEqual(result.returncode, 0)

    def testAnUnacceptedPresentationRefuses(self):
        self.git('update-ref', 'refs/remotes/origin/main', self.sdk)
        with self.assertRaises(presentation.PresentationError): self.prepare()
        self.assertFalse(self.output.exists())

    def testMutableOrWrongReferencesRefuse(self):
        for revision in ['main', self.design.upper(), self.design[:12], '0' * 40]:
            with self.subTest(revision=revision), self.assertRaises((presentation.PresentationError, subprocess.CalledProcessError)):
                presentation.prepare(self.root, revision, self.output, self.adapter)

    def testChangedPublishedSourceRefuses(self):
        self.write('src/Client.php', 'changed source')
        with self.assertRaises(presentation.PresentationError): self.prepare()

    def testChangedOrAddedStagedFilesRefuseAtArtifactBinding(self):
        self.prepare()
        (self.output / 'portal/guide.md').write_text('future support')
        with self.assertRaises(presentation.PresentationError): presentation.verify(self.output, self.sdk, self.design)

    def testSymlinkedPresentationRefuses(self):
        self.git('checkout', '-q', '--detach', self.design)
        path = self.root / 'docs/portal/assets/identity.css'
        path.unlink()
        path.symlink_to('../../guide.md')
        self.git('add', '.')
        self.git('commit', '-qm', 'symlink refusal fixture')
        self.design = self.git('rev-parse', 'HEAD')
        self.git('update-ref', 'refs/remotes/origin/main', self.design)
        self.git('checkout', '-q', '--detach', self.sdk)
        with self.assertRaises(presentation.PresentationError): self.prepare()
        self.assertFalse(self.output.exists())


if __name__ == '__main__': unittest.main()
