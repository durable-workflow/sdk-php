"""Verify the selected registry SDK bytes before PHP package loading."""

import hashlib
import json
from pathlib import Path, PurePosixPath
import stat
import sys
import zipfile


MAX_ARCHIVE_BYTES = 40 * 1024 * 1024
MAX_PACKAGE_BYTES = 100 * 1024 * 1024
MAX_FILES = 5000
PACKAGE = "durable-workflow/sdk"


def regular(path):
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
        raise ValueError("SDK evidence must be single-link regular files")
    return metadata


def package_metadata(consumer, supply):
    for path in (consumer / "composer.json", consumer / "composer.lock", consumer / "vendor/composer/installed.json"):
        regular(path)
        if path.resolve() != path:
            raise ValueError("Composer metadata must not be an alias")
    manifest = json.loads((consumer / "composer.json").read_text())
    if (manifest.get("repositories") or manifest.get("require-dev")
            or manifest.get("require") != {PACKAGE: supply["sdk_version"]}):
        raise ValueError("The consumer must select only the exact registry SDK")
    lock = json.loads((consumer / "composer.lock").read_text())
    installed = json.loads((consumer / "vendor/composer/installed.json").read_text())
    installed = installed["packages"] if isinstance(installed, dict) else installed
    selected = []
    for packages in (lock["packages"], installed):
        matches = [entry for entry in packages if entry["name"] == PACKAGE]
        if len(matches) != 1:
            raise ValueError("The selected SDK package must occur exactly once")
        entry = matches[0]
        if (entry["version"].removeprefix("v") != supply["sdk_version"]
                or entry.get("source", {}).get("url") != supply["sdk_source_url"]
                or entry.get("source", {}).get("reference") != supply["sdk_reference"]
                or entry.get("dist", {}).get("url") != supply["sdk_dist_url"]
                or entry.get("dist", {}).get("reference") != supply["sdk_reference"]):
            raise ValueError("The SDK lock/source/dist identities differ")
        selected.append(entry)
    if selected[1].get("installation-source") != "dist":
        raise ValueError("The SDK must be a registry dist installation")

    package = consumer / "vendor/durable-workflow/sdk"
    if package.resolve() != package or not package.is_dir():
        raise ValueError("The installed SDK root must not be an alias")
    if (consumer / "vendor/composer" / selected[1]["install-path"]).resolve() != package:
        raise ValueError("Composer's installed SDK path differs")
    return package


def archive_member(entry):
    path = PurePosixPath(entry.filename)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise ValueError("The package member is outside its root")
    if entry.is_dir():
        return None
    name = PurePosixPath(*path.parts[1:]).as_posix()
    mode = entry.external_attr >> 16
    # GitHub uses DOS entries without Unix modes for ordinary files, and Unix
    # entries for executables. Composer extracts the former as nonexecutables.
    if entry.create_system == 0 and mode == 0:
        mode = stat.S_IFREG | 0o644
    if not name or name == "." or stat.S_IFMT(mode) not in (0, stat.S_IFREG):
        raise ValueError("The package inventory contains an unsupported member")
    return name, mode & 0o777


def archive_files(archive, supply):
    size = regular(archive).st_size
    if not 0 < size <= MAX_ARCHIVE_BYTES:
        raise ValueError("The registry archive exceeds its byte bound")
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()

    expected = {}
    total = 0
    with zipfile.ZipFile(archive) as source:
        if len(source.infolist()) > MAX_FILES:
            raise ValueError("The package inventory exceeds its bound")
        prefixes = {PurePosixPath(entry.filename).parts[0] for entry in source.infolist()}
        if len(prefixes) != 1:
            raise ValueError("The registry dist must have one package root")
        for entry in source.infolist():
            member = archive_member(entry)
            if member is None:
                continue
            name, mode = member
            if name in expected:
                raise ValueError("The package inventory contains a duplicate member")
            total += entry.file_size
            if total > MAX_PACKAGE_BYTES:
                raise ValueError("The package expands beyond its byte bound")
            expected[name] = (source.read(entry), mode)
    if member_digest(expected) != supply["sdk_archive_members_sha256"]:
        raise ValueError("The registry archive members differ from the approved SDK supply")
    return expected, digest, total


def member_digest(files):
    members = {name: {"mode": mode, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
               for name, (raw, mode) in files.items()}
    return hashlib.sha256(json.dumps(members, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def installed_files(package, expected):
    actual = set()
    for path in package.rglob("*"):
        if path.is_symlink():
            raise ValueError("The installed SDK contains an alias")
        if path.is_dir():
            continue
        name = path.relative_to(package).as_posix()
        if name not in expected:
            raise ValueError("The installed SDK contains an unapproved file")
        metadata = regular(path)
        data, mode = expected[name]
        # Private umasks may remove archive permissions, but never add them.
        if metadata.st_mode & 0o777 & ~mode or metadata.st_mode & 0o022:
            raise ValueError("The installed SDK file mode exceeds its archive mode")
        if metadata.st_size != len(data) or path.read_bytes() != data:
            raise ValueError("The installed SDK bytes differ from the registry archive")
        actual.add(name)
    if actual != set(expected) or not any(name.startswith("src/") for name in actual):
        raise ValueError("The installed SDK inventory is incomplete")
    return len(actual)


def verify(consumer, archive, supply):
    consumer = Path(consumer)
    if consumer.resolve() != consumer or not consumer.is_dir():
        raise ValueError("The consumer must be a canonical directory")
    package = package_metadata(consumer, supply)
    expected, digest, total = archive_files(Path(archive), supply)
    count = installed_files(package, expected)
    return {"sdk_version": supply["sdk_version"], "sdk_reference": supply["sdk_reference"],
            "archive_sha256": digest, "archive_members_sha256": member_digest(expected), "installed_files": count,
            "installed_bytes": total, "source_fallback": False, "inspection_only": True}


if __name__ == "__main__":
    supply = json.loads(Path(__file__).with_name("command-key-supply.json").read_text())
    print(json.dumps(verify(Path(sys.argv[1]), Path(sys.argv[2]), supply)))
