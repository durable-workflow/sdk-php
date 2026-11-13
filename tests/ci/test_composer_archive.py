#!/usr/bin/env python3
"""Exercise Composer's actual archive filters for both Git checkout shapes."""

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import zipfile


def run(*command, cwd=None):
    return subprocess.check_output(command, cwd=cwd, text=True, stderr=subprocess.STDOUT)


def check(source):
    required = run('git', 'ls-files', 'src', 'examples', 'resources', 'config',
                   'docs/quickstart-contract.json', 'docs/laravel-adoption-contract.json', cwd=source).splitlines()
    assert required
    with tempfile.TemporaryDirectory(prefix='sdk-archive-private-marker-') as temporary:
        state = Path(temporary)
        ordinary = state / 'ordinary'
        ordinary.mkdir()
        # Materialize tracked bytes without exporting Git's filters first: the
        # Composer invocation must prove its own CI/tooling exclusion policy.
        for name in run('git', 'ls-files', cwd=source).splitlines():
            original = source / name
            target = ordinary / name
            target.parent.mkdir(parents=True, exist_ok=True)
            if original.is_symlink():
                target.symlink_to(os.readlink(original))
            else:
                shutil.copy2(original, target)
        run('git', 'init', '-q', cwd=ordinary)
        run('git', 'add', '-f', '.', cwd=ordinary)
        run('git', '-c', 'core.hooksPath=/dev/null', '-c', 'commit.gpgSign=false',
            '-c', 'user.name=Archive fixture', '-c', 'user.email=archive@example.invalid',
            'commit', '-qm', 'Archive fixture', cwd=ordinary)
        linked = state / 'linked'
        run('git', 'worktree', 'add', '--detach', str(linked), 'HEAD', cwd=ordinary)
        assert (ordinary / '.git').is_dir() and (linked / '.git').is_file()
        for checkout in (ordinary, linked):
            output = state / (checkout.name + '-output')
            output.mkdir()
            run('composer', '--no-plugins', '--no-scripts', '--no-interaction',
                'archive', '--format=zip', '--dir=' + str(output), '--file=sdk', cwd=checkout)
            with zipfile.ZipFile(output / 'sdk.zip') as archive:
                names = {name.removeprefix('./') for name in archive.namelist()}
                forbidden = ('.git/', '.github/', 'docker/', 'scripts/ci/', 'tests/')
                assert not any(name in ('.git', '.dockerignore') or name.startswith(forbidden)
                               for name in names), 'Archive includes repository control or CI tooling'
                assert set(required) <= names, 'Archive omits required package inputs'
                for name in required:
                    assert archive.read(name) == (source / name).read_bytes(), name
                for name in names:
                    if not name.endswith('/'):
                        content = archive.read(name)
                        assert str(state).encode() not in content, 'Archive leaks its private worktree path'
                        assert str(source).encode() not in content, 'Archive leaks the source checkout path'
            print(checkout.name + ': real Composer ZIP excludes Git/CI/private paths and retains '
                  + str(len(required)) + ' exact package inputs')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=Path(__file__).resolve().parents[2])
    check(parser.parse_args().source.resolve(strict=True))
