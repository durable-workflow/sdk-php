"""Keep both installed feature options bound to the qualified native tuple."""

from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest


SELECTOR = "(inputs.completion_limits || inputs.query_results)"
IMAGE = "durableworkflow/server@sha256:946c65eb70e8b4b7c4694be8a181c9669d46c3408b80d4e44d96f2030ea3cf51"
OLD_IMAGE = "durableworkflow/server@sha256:7988a23525d9e50e997a9f6f02e801300ed4f626de3cf6977033657ae7e9f58e"


def require_tuple(source: str) -> None:
    for key, current, previous in [
        ("SERVER_VERSION", "3.9.1", "3.8.0"),
        ("SERVER_IMAGE", IMAGE, OLD_IMAGE),
    ]:
        expected = f"      {key}: ${{{{ {SELECTOR} && '{current}' || '{previous}' }}}}"
        if expected not in source.splitlines():
            raise ValueError(f"{key} must bind either feature option to the qualified 3.9.1 tuple")


class PublishedQueryTupleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.source = (Path(__file__).resolve().parents[2] / ".github/workflows/published-worker-metrics.yml").read_text()

    def test_current_source(self) -> None:
        require_tuple(self.source)

    def test_predecessor_and_independent_selector_mutations_refuse(self) -> None:
        for key in ["SERVER_VERSION", "SERVER_IMAGE"]:
            with self.subTest(key=key):
                changed = "\n".join(
                    line.replace(SELECTOR, "inputs.completion_limits") if f"{key}:" in line else line
                    for line in self.source.splitlines()
                )
                with self.assertRaises(ValueError):
                    require_tuple(changed)

    def test_wrong_image_refuses(self) -> None:
        with self.assertRaises(ValueError):
            require_tuple(self.source.replace(IMAGE, OLD_IMAGE))

    def test_configured_shell_propagates_probe_failure_through_tee(self) -> None:
        shell = "bash --noprofile --norc -e -o pipefail {0}"
        self.assertIn(f"        shell: {shell}", self.source.splitlines())
        with tempfile.TemporaryDirectory() as temporary:
            probe = Path(temporary) / "probe.sh"
            probe.write_text("(exit 7) | cat > /dev/null\n")
            current = [str(probe) if word == "{0}" else word for word in shlex.split(shell)]
            self.assertEqual(7, subprocess.run(current, check=False).returncode)
            self.assertEqual(0, subprocess.run(["bash", "-e", str(probe)], check=False).returncode)

    def test_empty_query_evidence_is_not_accepted(self) -> None:
        self.assertIn('json_decode(file_get_contents("query-results.json"),true,flags:JSON_THROW_ON_ERROR)', self.source)


if __name__ == "__main__":
    unittest.main()
