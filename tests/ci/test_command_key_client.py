"""Offline input controls for the installed PHP command-key adapter."""

import json
import os
from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[2]
IMAGE = os.environ.get("SDK_PHP_COMMAND_KEY_IMAGE", "")


@unittest.skipUnless(IMAGE, "Select the already-built immutable PHP image for the offline controls.")
class CommandKeyClientInputTest(unittest.TestCase):
    def run_packet(self, packet, *, url="http://127.0.0.1:1", syntax=False):
        self.assertRegex(IMAGE, r"\Asha256:[0-9a-f]{64}\Z")
        self.addCleanup(self.assert_container_absent)
        command = [
            "docker", "run", "--rm", "--init", "--interactive", "--name", self.container_name,
            "--cpus", "1", "--memory", "128m", "--pids-limit", "64",
            "--network", "none", "--read-only", "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges", "--user", f"{os.getuid()}:{os.getgid()}",
            "--env", f"COMMAND_KEY_RUNTIME_URL={url}",
            "--mount", f"type=bind,source={ROOT / 'tests/smoke/command-key-client.php'},target=/fixture/client.php,readonly",
            "--mount", f"type=bind,source={ROOT / 'tests/smoke/command-key-supply.json'},target=/fixture/command-key-supply.json,readonly",
            IMAGE, "php",
        ]
        command += ["-l", "/fixture/client.php"] if syntax else ["/fixture/client.php", "/autoload-must-not-be-read.php"]
        return subprocess.run(command, input=packet, capture_output=True, text=True, timeout=20)

    def setUp(self):
        self.container_name = f"sdk-php-command-key-input-{os.getpid()}"

    def assert_container_absent(self):
        result = subprocess.run(["docker", "inspect", self.container_name], capture_output=True, text=True, timeout=10)
        self.assertNotEqual(0, result.returncode)
        self.assertEqual(f"error: no such object: {self.container_name}", result.stderr.strip().lower())

    def refused(self, packet, message, **options):
        result = self.run_packet(packet, **options)
        self.assertNotEqual(0, result.returncode)
        self.assertIn(message, result.stderr)
        self.assertNotIn("autoload-must-not-be-read", result.stderr)

    def test_unknown_operation_stops_before_autoload(self):
        self.refused('{"operation":"unsupported"}', "Unknown command-key fixture operation")

    def test_foreign_workflow_stops_before_autoload(self):
        self.refused(json.dumps({"operation": "signal", "workflow_id": "customer-order"}),
                     "workflow identity is outside its owned prefix")

    def test_foreign_namespace_stops_before_autoload(self):
        self.refused(json.dumps({"operation": "signal", "workflow_id": "sdk-php-216-inert", "namespace": "customer"}),
                     "namespace is outside its owned prefix")

    def test_external_origin_stops_before_autoload(self):
        self.refused(json.dumps({"operation": "signal", "workflow_id": "sdk-php-216-inert"}),
                     "requires a numeric loopback runtime", url="https://example.invalid")

    def test_oversized_packet_stops_before_autoload(self):
        self.refused('x' * 65537, "fixture packet exceeds its limit")

    def test_adapter_syntax(self):
        result = self.run_packet('', syntax=True)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("No syntax errors", result.stdout)


if __name__ == "__main__":
    unittest.main()
