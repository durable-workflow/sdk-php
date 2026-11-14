"""Run the local-backoff selectors without installing or executing an SDK."""

import os
from pathlib import Path
import subprocess
import unittest


def source():
    return (Path(__file__).resolve().parents[2] / ".github/workflows/published-worker-metrics.yml").read_text()


def guard():
    step = source().split("      - name: Require the published local backoff tuple\n", 1)[1].split("      - ", 1)[0]
    return "\n".join(line[10:] for line in step.split("        run: |\n", 1)[1].splitlines())


def disjoint_jobs(workflow):
    header = workflow.split("  installed-laravel-bridges:\n", 1)[1].split("    runs-on:", 1)[0]
    if " && !inputs.local_backoff" not in header:
        raise ValueError("Local-backoff qualification must exclude the sibling Laravel install job")


class PublishedLocalBackoffTupleTest(unittest.TestCase):
    def setUp(self):
        self.env = {
            "PATH": os.defpath, "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_REF": "refs/heads/main",
            "SDK_VERSION": "3.18.2", "SERVER_VERSION": "3.8.0",
            "SERVER_IMAGE": "durableworkflow/server@sha256:7988a23525d9e50e997a9f6f02e801300ed4f626de3cf6977033657ae7e9f58e",
            "CANDIDATE_SOURCE": "", "COMPLETION_LIMITS": "false", "QUERY_RESULTS": "false",
            "LARAVEL_BRIDGES": "false", "ACTIVITY_RETRY_IDENTITY": "false", "PAGINATION": "false",
        }

    def accepts(self, changes=None):
        result = subprocess.run(["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", guard()],
                                env=self.env | (changes or {}), capture_output=True, check=False)
        return result.returncode == 0

    def test_exact_published_tuple(self):
        self.assertTrue(self.accepts())

    def test_other_flags_candidates_and_foreign_tuples_refuse(self):
        for key, value in [("CANDIDATE_SOURCE", "a" * 40), ("COMPLETION_LIMITS", "true"),
                           ("QUERY_RESULTS", "true"), ("LARAVEL_BRIDGES", "true"),
                           ("ACTIVITY_RETRY_IDENTITY", "true"), ("PAGINATION", "true"),
                           ("SDK_VERSION", "3.18.3"), ("SERVER_VERSION", "3.9.1"),
                           ("SERVER_IMAGE", "durableworkflow/server:3.8.0"),
                           ("GITHUB_EVENT_NAME", "pull_request"), ("GITHUB_REF", "refs/heads/topic")]:
            with self.subTest(key=key):
                self.assertFalse(self.accepts({key: value}))

    def test_sibling_laravel_job_cannot_start_for_local_backoff(self):
        disjoint_jobs(source())
        with self.assertRaises(ValueError):
            disjoint_jobs(source().replace(" && !inputs.local_backoff", ""))

    def test_typed_nonempty_evidence_required(self):
        self.assertIn('json_decode(file_get_contents("local-backoff.json"),true,flags:JSON_THROW_ON_ERROR)', source())
        self.assertIn('"legacy_replay_callback_count"', source())
        self.assertIn('array_keys($r["cases"]??[])!==["short","empty"]', source())


if __name__ == "__main__":
    unittest.main()
