#!/usr/bin/env python3
"""Rendered assets follow the selected presentation; analytics stays SDK-owned."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
CHECKER = ROOT / 'scripts/check-docs-analytics.php'
ANALYTICS = ROOT / '.phpdoc/template/analytics/analytics.js'
STYLES = '.dw-cloud-promotion__eyebrow { letter-spacing: 0; }'
PROMOTION = ('data-promotion-source="sdk-php-reference" '
             'href="https://cloud.durable-workflow.com/early-access#source=sdk-php-reference"')


class RenderedAssetsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name)
        self.source = root / 'sdk'
        self.template = self.source / '.phpdoc/template'
        self.presentation = root / 'presentation/template'
        self.site = root / 'site'
        self.write(self.template / 'analytics/analytics.js', ANALYTICS.read_text())
        self.write(self.site / 'analytics/analytics.js', ANALYTICS.read_text())
        self.write(self.template / 'assets/api-reference.css', STYLES + '/* SDK */')
        self.write(self.template / 'assets/api-reference.js', '// SDK runtime')
        self.write(self.presentation / 'assets/api-reference.css', STYLES + '/* presentation */')
        self.write(self.presentation / 'assets/api-reference.js', '// presentation runtime')
        for name in ['index.html', 'api/index.html', 'api/classes/Client.html']:
            assets = ('href="/assets/api-reference.css" '
                      'type="module" src="/assets/api-reference.js"') if name.startswith('api/') else 'href="/assets/portal.css"'
            self.write(self.site / name, f'<html>{assets} {PROMOTION} type="module" src="/analytics/analytics.js"</html>')
        self.environment = os.environ | {'DOCS_SOURCE_DIRECTORY': str(self.source)}
        for key in ['DOCS_TEMPLATE_DIRECTORY', 'CLOUDFLARE_WEB_ANALYTICS_TOKEN']:
            self.environment.pop(key, None)
        self.render(self.template)

    @staticmethod
    def write(path, text):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def render(self, template):
        for name in ['api-reference.css', 'api-reference.js']:
            self.write(self.site / 'assets' / name, (template / 'assets' / name).read_text())

    def check(self, expected):
        result = subprocess.run(['php', str(CHECKER), str(self.site)], env=self.environment,
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode == 0, expected, result.stdout + result.stderr)
        return result

    def testUnchangedSdkTemplateRemainsTheDefault(self):
        self.check(True)

    def testDifferentPresentationAssetsRequireTheirExactSelectedTemplate(self):
        self.render(self.presentation)
        refused = self.check(False)
        self.assertIn('styles are missing or stale', refused.stdout + refused.stderr)
        self.environment['DOCS_TEMPLATE_DIRECTORY'] = str(self.presentation)
        self.check(True)

    def testChangedRenderedStylesOrRuntimeStillRefuse(self):
        self.render(self.presentation)
        self.environment['DOCS_TEMPLATE_DIRECTORY'] = str(self.presentation)
        for name in ['api-reference.css', 'api-reference.js']:
            with self.subTest(name=name):
                self.render(self.presentation)
                self.write(self.site / 'assets' / name, 'stale')
                self.check(False)

    def testMissingSelectedTemplateDoesNotFallBackToSdkAssets(self):
        self.environment['DOCS_TEMPLATE_DIRECTORY'] = str(self.presentation / 'absent')
        self.check(False)

    def testAnalyticsStillUsesTheSelectedSdkSource(self):
        self.render(self.presentation)
        self.environment['DOCS_TEMPLATE_DIRECTORY'] = str(self.presentation)
        self.write(self.presentation / 'analytics/analytics.js', 'unselected analytics')
        self.check(True)
        self.write(self.site / 'analytics/analytics.js', 'unselected analytics')
        refused = self.check(False)
        self.assertIn('analytics runtime is missing or stale', refused.stdout + refused.stderr)


if __name__ == '__main__':
    unittest.main()
