#!/usr/bin/env python3
"""Inert shell/source controls; never run an image, APT operation or package install.

Only the three readback commands and os-release path are replaced in a temporary
copy. The production script has no path, environment or release override input.
"""

import json
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "examples/laravel-http/runtime"
PREFIX = "/usr/bin/env -i PATH=/usr/bin:/bin LC_ALL=C /bin/sh /usr/local/lib/durable-workflow/require-fixed-apt.sh"


def require_source_boundary(dockerfile, proof):
    if (proof["os"], proof["release"], proof["architecture"], proof["minimum_apt"]) != ("debian", "12", "amd64", "2.6.2"):
        raise ValueError("Unsupported parent/floor")
    if proof["packages"] != {"apt": "2.6.2", "libapt-pkg6.0": "2.6.2"}:
        raise ValueError("Package inventory does not match the reviewed fixed pair")
    if "FROM php@" + proof["child"] + "\n" not in dockerfile:
        raise ValueError("Parent/inventory mismatch")
    runs = [line for line in dockerfile.replace("\\\n", " ").splitlines() if line.startswith("RUN ")]
    if not runs or not runs[0].startswith("RUN " + PREFIX + " "):
        raise ValueError("First RUN lacks the closed readback")
    if " && docker-php-ext-install" not in runs[0] or " && apt-get update" not in runs[0]:
        raise ValueError("Original build commands must follow the readback")


class ExampleFixedAptTest(unittest.TestCase):
    def run_guard(self, *, apt="2.6.2", library="2.6.2", status="install ok installed", release='ID=debian\nVERSION_ID="12"\n',
                  architecture="amd64", client=None, query_exit=0, compare_exit=None, args=(), raw_record=None, database_statuses=None, database_mutation=None):
        with tempfile.TemporaryDirectory(prefix="sdk-php-apt-source-") as directory:
            root = Path(directory)
            os_release = root / "os-release"
            os_release.write_text(release)
            environment = root / "child-environment"
            calls = root / "calls"
            query = root / "dpkg-query"
            apt_record = raw_record if raw_record is not None else status + "\t" + apt + "\n"
            query.write_text("#!/bin/sh\n/usr/bin/env > " + shlex.quote(str(environment)) + "\n"
                             + "printf 'query\\n' >> " + shlex.quote(str(calls)) + "\n"
                             + "[ \"$1\" = --show ] || exit 91\ncase \"$3\" in\n"
                             + "apt) printf '%s' " + shlex.quote(apt_record) + ";;\n"
                             + "libapt-pkg6.0) printf '%s' " + shlex.quote("install ok installed\t" + library + "\n") + ";;\n"
                             + "*) exit 92;;\nesac\nexit " + str(query_exit) + "\n")
            if database_statuses is not None:
                database = root / "database"
                database.mkdir()
                database.joinpath("status").write_text("\n".join(
                    "Package: " + package + "\nStatus: " + state + "\nArchitecture: amd64\nVersion: 2.6.2\n"
                    + "Maintainer: Inert Source Fixture <test@example.invalid>\nDescription: inert read-only fixture\n"
                    for package, state in zip(("apt", "libapt-pkg6.0"), database_statuses)))
                status_file = database / "status"
                data = status_file.read_text()
                if database_mutation == "duplicate-package":
                    data += "\n" + data.split("\n\n", 1)[0] + "\n"
                elif database_mutation == "duplicate-status":
                    data = data.replace("Status: install ok installed\n", "Status: install ok installed\nStatus: install ok installed\n", 1)
                elif database_mutation == "malformed-status":
                    data = data.replace("Status: install ok installed\n", "Status: install unknown installed\n", 1)
                status_file.write_text(data)
                query.write_text("#!/bin/sh\n/usr/bin/env > " + shlex.quote(str(environment)) + "\n"
                                 + "printf 'query\\n' >> " + shlex.quote(str(calls)) + "\n"
                                 + "exec /usr/bin/dpkg-query --admindir=" + shlex.quote(str(database)) + ' "$@"\n')
            dpkg = root / "dpkg"
            comparison = "exit " + str(compare_exit) if compare_exit is not None else 'exec /usr/bin/dpkg "$@"'
            dpkg.write_text("#!/bin/sh\ncase \"$1\" in\n--print-architecture) printf '%s\\n' "
                            + shlex.quote(architecture) + ";;\n--compare-versions) " + comparison + ";;\n*) exit 93;;\nesac\n")
            apt_get = root / "apt-get"
            apt_get.write_text("#!/bin/sh\n[ \"$#\" -eq 1 ] && [ \"$1\" = --version ] || exit 94\nprintf '%s\\n' "
                               + shlex.quote(client if client is not None else "apt " + apt + " (amd64)") + "\n")
            for file in (query, dpkg, apt_get):
                file.chmod(0o700)
            source = (RUNTIME / "require-fixed-apt.sh").read_text()
            for original, replacement in (("/etc/os-release", os_release), ("/usr/bin/dpkg-query", query),
                                          ("/usr/bin/dpkg", dpkg), ("/usr/bin/apt-get", apt_get)):
                source = source.replace(original, shlex.quote(str(replacement)))
            script = root / "guard.sh"
            script.write_text(source)
            result = subprocess.run(["/usr/bin/env", "-i", "PATH=/usr/bin:/bin", "LC_ALL=C", "/bin/sh", str(script), *args],
                                    env={"GITHUB_TOKEN": "public-inert-sentinel", "AWS_ACCESS_KEY_ID": "public-inert-sentinel",
                                         "SSH_AUTH_SOCK": "/inert", "HTTP_PROXY": "http://inert", "APT_CONFIG": "/inert"},
                                    text=True, capture_output=True, timeout=10, check=False)
            child_env = environment.read_text() if environment.exists() else ""
            return result, child_env, calls.read_text() if calls.exists() else ""

    def test_exact_fixed_and_newer_matching_pairs_pass(self):
        for version in ("2.6.2", "2.6.2+deb12u1", "2.6.3"):
            with self.subTest(version=version):
                result, _, _ = self.run_guard(apt=version, library=version)
                self.assertEqual(0, result.returncode, result.stderr)
                self.assertIn("apt=" + version + " libapt-pkg6.0=" + version, result.stdout)

    def test_affected_and_prerelease_pairs_refuse(self):
        for version in ("2.6.1", "2.6.2~rc1"):
            with self.subTest(version=version):
                self.assertNotEqual(0, self.run_guard(apt=version, library=version)[0].returncode)

    def test_missing_malformed_duplicate_and_mixed_packages_refuse(self):
        cases = [{"raw_record": ""}, {"status": "unpacked"}, {"library": "2.6.1"},
                 {"raw_record": "install ok installed\t2.6.2\ninstall ok installed\t2.6.2\n"},
                 {"raw_record": "install ok installed\t2.6.2\textra\n"}, {"apt": "x2.6.2"}, {"apt": "2.6.2;id"},
                 {"apt": "2" * 101}, {"query_exit": 1}, {"compare_exit": 2}]
        for case in cases:
            with self.subTest(case=case):
                self.assertNotEqual(0, self.run_guard(**case)[0].returncode)

    def test_real_fixture_status_parser_preserves_normal_installed_positive(self):
        result, _, _ = self.run_guard(database_statuses=("install ok installed", "install ok installed"))
        self.assertEqual(0, result.returncode, result.stderr)

    def test_real_fixture_status_parser_refuses_error_and_noninstall_states(self):
        for state in ("install reinstreq installed", "hold ok installed", "deinstall ok installed", "purge ok installed"):
            for package in (0, 1):
                with self.subTest(state=state, package=package):
                    states = ["install ok installed", "install ok installed"]
                    states[package] = state
                    result, _, _ = self.run_guard(database_statuses=states)
                    self.assertNotEqual(0, result.returncode, "Partial/error package state cannot authorize APT")

    def test_real_fixture_status_parser_refuses_duplicate_and_malformed_records(self):
        for mutation in ("duplicate-package", "duplicate-status", "malformed-status"):
            with self.subTest(mutation=mutation):
                result, _, _ = self.run_guard(database_statuses=("install ok installed", "install ok installed"), database_mutation=mutation)
                self.assertNotEqual(0, result.returncode)

    def test_unknown_or_ambiguous_distro_and_foreign_architecture_refuse_before_query(self):
        for release in ('ID=ubuntu\nVERSION_ID="24.04"\n', 'ID=debian\nVERSION_ID="13"\n',
                        'ID=debian\nID=ubuntu\nVERSION_ID="12"\n', ""):
            with self.subTest(release=release):
                result, _, calls = self.run_guard(release=release)
                self.assertNotEqual(0, result.returncode)
                self.assertEqual("", calls)
        result, _, calls = self.run_guard(architecture="arm64")
        self.assertNotEqual(0, result.returncode)
        self.assertEqual("", calls)

    def test_client_package_mismatch_refuses(self):
        for client in ("apt 2.6.1 (amd64)", "apt 2.6.2 (arm64)", ""):
            with self.subTest(client=client):
                self.assertNotEqual(0, self.run_guard(client=client)[0].returncode)

    def test_no_version_or_release_overrides_or_ambient_credentials(self):
        result, _, calls = self.run_guard(args=("--version", "9"))
        self.assertNotEqual(0, result.returncode)
        self.assertEqual("", calls)
        result, environment, _ = self.run_guard()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertLessEqual({line.split("=", 1)[0] for line in environment.splitlines()}, {"PATH", "LC_ALL", "PWD"})

    def test_source_parent_proof_and_first_run_order(self):
        dockerfile = (RUNTIME / "Dockerfile").read_text()
        proof = json.loads((RUNTIME / "apt-parent.json").read_text())
        require_source_boundary(dockerfile, proof)
        for changed in (dockerfile.replace(proof["child"], "sha256:" + "0" * 64),
                        dockerfile.replace("RUN " + PREFIX + " \\\n    && ", "RUN "), "RUN apt-get update\n" + dockerfile):
            with self.assertRaises(ValueError):
                require_source_boundary(changed, proof)
        for field, value in (("release", "13"), ("architecture", "arm64"), ("packages", {"apt": "2.6.1"})):
            with self.assertRaises(ValueError):
                require_source_boundary(dockerfile, {**proof, field: value})
        source = (RUNTIME / "require-fixed-apt.sh").read_text()
        self.assertNotIn("eval", source)
        self.assertNotIn("apt-get update", source)
        self.assertNotIn("apt-get install", source)
        self.assertIn("/usr/bin/dpkg-query", source)
        self.assertIn(r"\${Status}", source)
        self.assertNotIn("db:Status-Status", source)
        self.assertIn("/usr/bin/dpkg --compare-versions", source)


if __name__ == "__main__":
    unittest.main()
