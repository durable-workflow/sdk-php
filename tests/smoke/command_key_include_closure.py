"""Capture and inventory the complete PHP include tree without loading PHP."""

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat


MAX_FILES = 10000
MAX_BYTES = 128 * 1024 * 1024
SDK_PREFIX = "consumer/vendor/durable-workflow/sdk/"


def members_digest(members):
    return hashlib.sha256(json.dumps(members, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def approved_directories(prefix, approved):
    directories = {prefix.rstrip("/")}
    for name, expected in approved.items():
        if expected["mode"] not in (0o644, 0o755):
            raise ValueError("The approved member mode is unsupported")
        path = PurePosixPath(name)
        if path.is_absolute() or ".." in path.parts or path.as_posix() != name:
            raise ValueError("The approved member path is not canonical")
        directories.update(prefix + parent.as_posix() for parent in path.parents if parent.as_posix() != ".")
    return directories


def bind_members(snapshot, prefix, approved, *, sealed):
    files = {name.removeprefix(prefix): entry for name, entry in snapshot["entries"].items()
             if name.startswith(prefix) and entry["kind"] == "file"}
    if not approved or set(files) != set(approved):
        raise ValueError("The staged member set differs from its approved authority")
    expected_directories = approved_directories(prefix, approved)
    for name, expected in approved.items():
        actual = files[name]
        mode_matches = (actual["mode"] == (expected["mode"] & ~0o222) if sealed
                        else not (actual["mode"] & ~expected["mode"]))
        if not mode_matches or actual["bytes"] != expected["bytes"] or actual["sha256"] != expected["sha256"]:
            raise ValueError("The staged member differs from its approved authority")
    actual_directories = {name for name, entry in snapshot["entries"].items()
                          if (name == prefix.rstrip("/") or name.startswith(prefix)) and entry["kind"] == "directory"}
    if actual_directories != expected_directories:
        raise ValueError("The staged directory set differs from its approved authority")
    return {prefix + name: {"kind": "file", **member} for name, member in approved.items()}


def verify_sealed_sdk(snapshot, approved_digest):
    approved = {}
    for name, entry in snapshot["entries"].items():
        if name.startswith(SDK_PREFIX) and entry["kind"] == "file":
            if entry["mode"] not in (0o444, 0o555):
                raise ValueError("The sealed SDK member mode differs")
            approved[name.removeprefix(SDK_PREFIX)] = {
                "mode": 0o755 if entry["mode"] == 0o555 else 0o644,
                "bytes": entry["bytes"], "sha256": entry["sha256"]}
    if members_digest(approved) != approved_digest:
        raise ValueError("The sealed SDK differs from the approved archive member authority")
    bind_members(snapshot, SDK_PREFIX, approved, sealed=True)


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
            if (not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1
                    or metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) & ~member["mode"]):
                raise ValueError("The include source type, owner or mode changed")
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


def stage(consumer, fixture, destination, approved_sdk, approved_digest, approved_fixture):
    if members_digest(approved_sdk) != approved_digest:
        raise ValueError("The staging SDK authority differs from the approved archive")
    if set(path.name for path in consumer.iterdir()) != {"composer.json", "composer.lock", "vendor"}:
        raise ValueError("Unexpected consumer root members")
    if set(path.name for path in fixture.iterdir()) != {"command-key-client.php", "command-key-supply.json"}:
        raise ValueError("Unexpected captured fixture members")
    owner = os.getuid()
    before = inventory(consumer, owner, sealed=False)
    fixture_before = inventory(fixture, owner, sealed=False)
    # Expected SDK and fixture leaf bytes come from approved archive/Git data,
    # never from a fresh read of the mutable install tree.
    consumer_copy = {"entries": dict(before["entries"])}
    consumer_copy["entries"].update(bind_members(before, "vendor/durable-workflow/sdk/", approved_sdk, sealed=False))
    fixture_snapshot = {"entries": {"fixture" if name == "." else "fixture/" + name: entry
                                    for name, entry in fixture_before["entries"].items()}}
    fixture_copy = {"entries": dict(fixture_before["entries"])}
    fixture_copy["entries"].update({name.removeprefix("fixture/"): entry for name, entry in
                                  bind_members(fixture_snapshot, "fixture/", approved_fixture, sealed=False).items()})
    for required in ("vendor/autoload.php", "vendor/composer/installed.json"):
        if before["entries"].get(required, {}).get("kind") != "file":
            raise ValueError("The complete installed autoloader metadata is required")
    metadata = destination.lstat()
    if (destination.resolve() != destination or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != owner or stat.S_IMODE(metadata.st_mode) != 0o700
            or list(destination.iterdir())):
        raise ValueError("The include destination must be a fresh private directory")
    copy_tree(consumer, destination / "consumer", consumer_copy)
    copy_tree(fixture, destination / "fixture", fixture_copy)
    if inventory(consumer, owner, sealed=False) != before or inventory(fixture, owner, sealed=False) != fixture_before:
        raise ValueError("An include source changed during staging")
    destination.chmod(0o555)
    result = inventory(destination, owner, sealed=True)
    verify_sealed_sdk(result, approved_digest)
    bind_members(result, "fixture/", approved_fixture, sealed=True)
    return result
