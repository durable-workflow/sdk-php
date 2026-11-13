"""Two finite command-key fixture roles with captured-ID lifecycle control."""

import json
import os
from pathlib import Path
import re
import stat
import subprocess
from types import SimpleNamespace


OWNER = "durable-workflow.php-command-keys"
ROLE = "durable-workflow.fixture-role"
NONCE = "durable-workflow.fixture-nonce"
ROLES = {"server", "consumer"}


class FixtureTimeout(RuntimeError):
    def __init__(self, output):
        super().__init__("The bounded fixture command timed out")
        self.output = output.decode() if isinstance(output, bytes) else (output or "")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def private_directory(path):
    metadata = path.lstat()
    require(path.resolve() == path and stat.S_ISDIR(metadata.st_mode)
            and metadata.st_uid == os.getuid() and stat.S_IMODE(metadata.st_mode) == 0o700,
            "Fixture control directories must be canonical, private and owned")
    return metadata


def exact_missing(process, identity):
    return (process.returncode == 1 and process.stdout == "[]\n"
            and process.stderr in {"Error: No such object: " + identity + "\n",
                                   "error: no such object: " + identity + "\n"})


class Containers:
    def __init__(self, configuration, state, owner, nonce, record_created=None):
        require(isinstance(owner, str) and re.fullmatch(r"[0-9]+-[0-9]+", owner), "Invalid fixture run owner")
        require(isinstance(nonce, str) and re.fullmatch(r"[0-9a-f]{32}", nonce), "Invalid fixture nonce")
        private_directory(configuration)
        initial = private_directory(state)
        self.configuration, self.state, self.owner, self.nonce = configuration, state, owner, nonce
        self.state_descriptor = os.open(state, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        metadata = os.fstat(self.state_descriptor)
        current = state.lstat()
        if ((metadata.st_dev, metadata.st_ino) != (current.st_dev, current.st_ino)
                or (metadata.st_dev, metadata.st_ino) != (initial.st_dev, initial.st_ino)
                or metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o700):
            os.close(self.state_descriptor)
            raise RuntimeError("The fixture state directory changed before it was held")
        self.records = {}
        self.identities = {}
        self.record_created = record_created

    def close(self):
        for descriptor in self.records.values():
            os.close(descriptor)
        self.records.clear()
        if self.state_descriptor is not None:
            os.close(self.state_descriptor)
            self.state_descriptor = None

    def environment(self):
        private_directory(self.configuration)
        require(not list(self.configuration.iterdir()), "Docker configuration must remain empty")
        return {"PATH": "/usr/bin:/bin", "DOCKER_CONFIG": str(self.configuration),
                "DOCKER_HOST": "unix:///var/run/docker.sock"}

    def command(self, arguments, *, timeout=30, input=None):
        try:
            return subprocess.run(["/usr/bin/docker", *arguments], env=self.environment(), input=input,
                                  capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired as error:
            raise FixtureTimeout(error.stdout) from None

    def inspect(self, identity, *, allow_missing=False):
        require(isinstance(identity, str) and re.fullmatch(r"[0-9a-f]{64}", identity),
                "A full captured container ID is required")
        result = self.command(["inspect", identity], timeout=15)
        if allow_missing and exact_missing(result, identity):
            return None
        require(result.returncode == 0, "The captured container cannot be inspected")
        rows = json.loads(result.stdout)
        require(isinstance(rows, list) and len(rows) == 1 and rows[0]["Id"] == identity,
                "The captured container identity differs")
        return rows[0]

    def owned(self, identity, role, *, allow_missing=False):
        require(role in ROLES, "Unexpected fixture role")
        record = self.inspect(identity, allow_missing=allow_missing)
        if record is None:
            return None
        labels = record["Config"].get("Labels") or {}
        require((labels.get(OWNER), labels.get(ROLE), labels.get(NONCE)) == (self.owner, role, self.nonce),
                "Container ownership changed; preserve the replacement")
        return record

    def pull(self, image, configuration):
        require(re.fullmatch(r"(?:php|durableworkflow/server)@sha256:[0-9a-f]{64}", image) is not None,
                "An exact public fixture image is required")
        require(self.command(["pull", "--platform", "linux/amd64", image], timeout=180).returncode == 0,
                "The selected public fixture image could not be pulled")
        result = self.command(["image", "inspect", image], timeout=15)
        require(result.returncode == 0, "The selected fixture image cannot be inspected")
        rows = json.loads(result.stdout)
        require(isinstance(rows, list) and len(rows) == 1, "The selected image inspection is ambiguous")
        selected = rows[0]
        descriptor = selected.get("Descriptor") or {}
        index = image.split("@", 1)[1]
        descriptor_matches = (descriptor.get("digest") == index and descriptor.get("mediaType") in {
            "application/vnd.oci.image.index.v1+json", "application/vnd.docker.distribution.manifest.list.v2+json"})
        index_identity = selected["Id"] == index and descriptor_matches
        require((selected["Id"] == configuration or index_identity)
                and (not descriptor or descriptor_matches)
                and selected["Os"] == "linux" and selected["Architecture"] == "amd64",
                "The selected public image configuration differs")
        # The created container must independently bind its selected platform
        # manifest before start when the engine uses the index as its image ID.
        return selected

    def create(self, role, arguments):
        require(role in ROLES and role not in self.records, "A fixture role may be created only once")
        descriptor = os.open(role + ".json", os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600, dir_fd=self.state_descriptor)
        self.records[role] = descriptor
        flags = ["create", "--name", "sdk-php-command-keys-" + self.owner + "-" + self.nonce[:10] + "-" + role,
                 "--label", OWNER + "=" + self.owner, "--label", ROLE + "=" + role,
                 "--label", NONCE + "=" + self.nonce, "--platform", "linux/amd64", "--init", "--read-only",
                 "--cpus", "1", "--memory", "1g", "--pids-limit", "256",
                 "--cap-drop", "ALL", "--security-opt", "no-new-privileges"]
        try:
            result = self.command([*flags, *arguments], timeout=30)
        except FixtureTimeout as error:
            result = SimpleNamespace(returncode=124, stdout=error.output)
        require(re.fullmatch(r"[0-9a-f]{64}\n", result.stdout) is not None,
                "Container creation did not return one full ID")
        identity = result.stdout.rstrip("\n")
        self.identities[role] = identity
        # Retain the create stdout independently before any later preparation
        # operation can fail. No package code runs in this preparation phase.
        if self.record_created is not None:
            self.record_created(role, identity)
        record = {"id": identity, "owner": self.owner, "role": role, "nonce": self.nonce}
        raw = (json.dumps(record, sort_keys=True) + "\n").encode()
        offset = 0
        while offset < len(raw):
            written = os.write(descriptor, raw[offset:])
            require(written > 0, "The held fixture record could not be written")
            offset += written
        os.fsync(descriptor)
        # Keep this descriptor held for the complete preparation phase.
        require(result.returncode == 0, "Container creation failed after returning its identity")
        self.owned(identity, role)
        return identity

    def start(self, identity, role):
        self.owned(identity, role)
        require(self.command(["start", identity]).returncode == 0, "The captured fixture could not start")

    def remove(self, identity, role):
        if self.owned(identity, role, allow_missing=True) is None:
            return
        require(self.command(["rm", "--force", "--volumes", identity]).returncode == 0,
                "The captured fixture could not be removed")
        require(self.owned(identity, role, allow_missing=True) is None,
                "Exact fixture absence was not established")

    def remove_all(self, pairs):
        failed = False
        for role, identity in pairs:
            try:
                self.remove(identity, role)
            except Exception:
                failed = True
        require(not failed, "One or more captured fixtures could not be removed")
