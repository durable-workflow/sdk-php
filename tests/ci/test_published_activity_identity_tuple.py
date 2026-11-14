"""Exercise the real pre-install selector guard without SDK or Docker execution."""

import os
from pathlib import Path
import subprocess
import unittest


def guard_source():
    workflow = (Path(__file__).resolve().parents[2] / ".github/workflows/published-worker-metrics.yml").read_text()
    step = workflow.split("      - name: Require the published activity identity tuple\n", 1)[1].split("      - ", 1)[0]
    return "\n".join(line[10:] for line in step.split("        run: |\n", 1)[1].splitlines())


def require_disjoint_jobs(source):
    expected = "    if: github.event_name == 'workflow_dispatch' && inputs.laravel_bridges && !inputs.activity_retry_identity && !inputs.pagination"
    header = source.split("  installed-laravel-bridges:\n", 1)[1].split("    runs-on:", 1)[0]
    if header.strip() != expected.strip():
        raise ValueError("The sibling Laravel job must not start for activity identity qualification")


class PublishedActivityIdentityTupleTest(unittest.TestCase):
    def setUp(self):
        self.env = {
            "PATH": os.defpath, "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_REF": "refs/heads/main",
            "SDK_VERSION": "3.18.2", "SERVER_VERSION": "3.8.0",
            "SERVER_IMAGE": "durableworkflow/server@sha256:7988a23525d9e50e997a9f6f02e801300ed4f626de3cf6977033657ae7e9f58e",
            "CANDIDATE_SOURCE": "", "COMPLETION_LIMITS": "false", "QUERY_RESULTS": "false", "LARAVEL_BRIDGES": "false", "PAGINATION": "false",
        }

    def accepts(self, changes=None):
        result = subprocess.run(["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", guard_source()],
                                env=self.env | (changes or {}), capture_output=True, check=False)
        return result.returncode == 0

    def test_exact_published_tuple(self):
        self.assertTrue(self.accepts())

    def test_source_selector_and_unrelated_qualification_flags_refuse(self):
        for variable, value in [("CANDIDATE_SOURCE", "a" * 40), ("COMPLETION_LIMITS", "true"),
                                ("QUERY_RESULTS", "true"), ("LARAVEL_BRIDGES", "true"), ("PAGINATION", "true")]:
            with self.subTest(variable=variable):
                self.assertFalse(self.accepts({variable: value}))

    def test_wrong_package_producer_and_dispatch_scope_refuse(self):
        for variable, value in [("SDK_VERSION", "3.18.3"), ("SERVER_VERSION", "3.9.1"),
                                ("SERVER_IMAGE", "durableworkflow/server:3.8.0"),
                                ("GITHUB_EVENT_NAME", "pull_request"), ("GITHUB_REF", "refs/heads/topic")]:
            with self.subTest(variable=variable):
                self.assertFalse(self.accepts({variable: value}))

    def test_sibling_job_cannot_start_on_conflicting_flags(self):
        source = (Path(__file__).resolve().parents[2] / ".github/workflows/published-worker-metrics.yml").read_text()
        require_disjoint_jobs(source)
        with self.assertRaises(ValueError):
            require_disjoint_jobs(source.replace(" && !inputs.activity_retry_identity", ""))


if __name__ == "__main__":
    unittest.main()
