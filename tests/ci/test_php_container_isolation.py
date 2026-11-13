import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(os.environ.get("SDK_PHP_CI_TEST_IMAGE"), "requires the built PHP CI image")
class PhpContainerIsolationTests(unittest.TestCase):
    def test_unit_controls_are_read_only(self):
        self.check_boundary(False)

    def test_carbon_manifest_is_the_only_extra_writable_file(self):
        self.check_boundary(True)

    def check_boundary(self, carbon):
        # Use disposable controls so the predecessor can demonstrate its escape
        # without changing the checkout or executing a substituted host wrapper.
        with tempfile.TemporaryDirectory(prefix="sdk-php-isolation-") as directory:
            fixture = Path(directory)
            source = fixture / "source"
            scripts = source / "scripts/ci"
            scripts.mkdir(parents=True)
            launcher = scripts / "run-php-container.sh"
            shutil.copy2(ROOT / "scripts/ci/run-php-container.sh", launcher)
            setup = scripts / "setup-php-container.sh"
            shutil.copy2(ROOT / "scripts/ci/setup-php-container.sh", setup)
            ini = source / "docker/php-ci/ci-defaults.ini"
            ini.parent.mkdir(parents=True)
            shutil.copy2(ROOT / "docker/php-ci/ci-defaults.ini", ini)
            state = fixture / "state"
            (state / "bin").mkdir(parents=True)
            (state / "data/tmp").mkdir(parents=True)
            for name in ("vendor", "build", ".phpunit.cache"):
                (source / name).mkdir()
            if carbon:
                (state / "writable-manifest").write_text("carbon2\n")
            (source / "composer.lock").write_text("{}\n")
            (source / "composer.json").write_text('{"name":"sdk-php/isolation-control"}\n')
            subprocess.run(["git", "init", "--quiet", str(source)], check=True, timeout=10)
            for name in ("php", "composer"):
                (state / "bin" / name).symlink_to(launcher)
            expected = hashlib.sha256(launcher.read_bytes()).hexdigest()
            environment = {name: os.environ[name] for name in ("PATH", "DOCKER_CONFIG") if name in os.environ}
            environment.update({"SDK_PHP_CI_ROOT": str(source), "SDK_PHP_CI_STATE": str(state),
                                "SDK_PHP_CI_IMAGE": os.environ["SDK_PHP_CI_TEST_IMAGE"]})
            targets = [launcher, setup, state / "bin/php", state / "bin/composer",
                       source / "json.py", state / "bin/new-wrapper", state / "writable-manifest"]
            if not carbon:
                targets.append(source / "composer.json")
            program = r'''
                $targets = json_decode($argv[1], true, 512, JSON_THROW_ON_ERROR);
                $data = $argv[2]; $results = [];
                foreach ($targets as $target) {
                    $replacement = $data.'/replacement';
                    file_put_contents($replacement, 'synthetic replacement');
                    $results[] = [@file_put_contents($target, 'synthetic overwrite') !== false,
                                  @rename($replacement, $target)];
                    @unlink($replacement);
                }
                $outputs = [$data.'/canary', $argv[3].'/vendor/canary', $argv[3].'/composer.lock',
                            $argv[3].'/build/canary', $argv[3].'/.phpunit.cache/canary'];
                if ($argv[4] === 'carbon2') { $outputs[] = $argv[3].'/composer.json'; }
                foreach ($outputs as $output) {
                    if (file_put_contents($output, '{}') === false) { throw new RuntimeException('Writable output refused'); }
                }
                echo json_encode($results, JSON_THROW_ON_ERROR);
            '''
            result = subprocess.run([str(state / "bin/php"), "-r", program,
                                     json.dumps([str(path) for path in targets]), str(state / "data/tmp"), str(source), "carbon2" if carbon else "unit"],
                                    cwd=source, env=environment, capture_output=True, text=True, timeout=60)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout), [[False, False]] * len(targets),
                             "Container code replaced a host launcher or control path")
            self.assertEqual(hashlib.sha256(launcher.read_bytes()).hexdigest(), expected)
            for name in ("php", "composer"):
                self.assertTrue((state / "bin" / name).is_symlink())
                self.assertEqual((state / "bin" / name).resolve(), launcher)
            # Only invoke the next host command after verifying controls survived.
            followup = subprocess.run([str(state / "bin/php"), "-r", 'echo "original-container-php";'],
                                      cwd=source, env=environment, capture_output=True, text=True, timeout=30)
            self.assertEqual(followup.returncode, 0, followup.stderr)
            self.assertEqual(followup.stdout, "original-container-php")


if __name__ == "__main__":
    unittest.main()
