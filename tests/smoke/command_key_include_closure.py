"""Capture and inventory the complete PHP include tree without loading PHP."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import stat


MAX_FILES = 10000
MAX_BYTES = 128 * 1024 * 1024


def inventory(root, owner, *, sealed):
    if root.resolve() != root:
        raise ValueError("The include root must be canonical")
    entries = {}
    total = 0
    paths = [root]
    for directory, folders, files in os.walk(root, followlinks=False):
        paths.extend(Path(directory) / name for name in [*folders, *files])
        if len(paths) > MAX_FILES:
            raise ValueError("The include tree exceeds its member bound")
    for path in sorted(paths):
        if any(part in {".git", ".hg", ".svn"} for part in path.relative_to(root).parts):
            raise ValueError("Repository metadata is outside the installed include closure")
        metadata = path.lstat()
        mode = stat.S_IMODE(metadata.st_mode)
        if metadata.st_uid != owner or mode & 0o022 or (sealed and mode & 0o222):
            raise ValueError("The include tree ownership or write permissions differ")
        if path.is_symlink() or path.resolve() != path:
            raise ValueError("The include tree contains an alias")
        name = path.relative_to(root).as_posix()
        if stat.S_ISDIR(metadata.st_mode):
            entries[name] = {"kind": "directory", "mode": mode}
        elif stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1:
            total += metadata.st_size
            if total > MAX_BYTES:
                raise ValueError("The include tree exceeds its byte bound")
            raw = path.read_bytes()
            if len(raw) != metadata.st_size:
                raise ValueError("The include file changed while capturing it")
            entries[name] = {"kind": "file", "mode": mode, "bytes": len(raw),
                             "sha256": hashlib.sha256(raw).hexdigest()}
        else:
            raise ValueError("The include tree needs single-link regular leaves")
        if len(entries) > MAX_FILES:
            raise ValueError("The include tree exceeds its member bound")
    digest = hashlib.sha256(json.dumps(entries, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {"entries": entries, "sha256": digest, "bytes": total}


def copy_tree(source, destination, expected):
    destination.mkdir(mode=0o700)
    for name, member in expected["entries"].items():
        if name == ".":
            continue
        original, target = source / name, destination / name
        if member["kind"] == "directory":
            target.mkdir(mode=0o700)
            continue
        descriptor = os.open(original, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                raise ValueError("The include source changed its leaf type")
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                raw = stream.read(MAX_BYTES + 1)
            if len(raw) != member["bytes"] or hashlib.sha256(raw).hexdigest() != member["sha256"]:
                raise ValueError("The include source changed before copying")
        finally:
            os.close(descriptor)
        output = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(output, "wb") as stream:
            stream.write(raw)
        target.chmod(0o555 if member["mode"] & 0o111 else 0o444)
    for path in sorted(destination.rglob("*"), reverse=True):
        if path.is_dir():
            path.chmod(0o555)
    destination.chmod(0o555)


def stage(consumer, fixture, destination):
    if set(path.name for path in consumer.iterdir()) != {"composer.json", "composer.lock", "vendor"}:
        raise ValueError("Unexpected consumer root members")
    if set(path.name for path in fixture.iterdir()) != {"command-key-client.php", "command-key-supply.json"}:
        raise ValueError("Unexpected captured fixture members")
    owner = os.getuid()
    before = inventory(consumer, owner, sealed=False)
    fixture_before = inventory(fixture, owner, sealed=False)
    for required in ("vendor/autoload.php", "vendor/composer/installed.json"):
        if before["entries"].get(required, {}).get("kind") != "file":
            raise ValueError("The complete installed autoloader metadata is required")
    metadata = destination.lstat()
    if (destination.resolve() != destination or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != owner or stat.S_IMODE(metadata.st_mode) != 0o700
            or list(destination.iterdir())):
        raise ValueError("The include destination must be a fresh private directory")
    copy_tree(consumer, destination / "consumer", before)
    copy_tree(fixture, destination / "fixture", fixture_before)
    if inventory(consumer, owner, sealed=False) != before or inventory(fixture, owner, sealed=False) != fixture_before:
        raise ValueError("An include source changed during staging")
    destination.chmod(0o555)
    return inventory(destination, owner, sealed=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--consumer", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    options = parser.parse_args()
    result = stage(options.consumer, options.fixture, options.destination)
    print(json.dumps({"inspection_only": True, "include_sha256": result["sha256"],
                      "members": len(result["entries"]), "bytes": result["bytes"]}))


if __name__ == "__main__":
    main()
