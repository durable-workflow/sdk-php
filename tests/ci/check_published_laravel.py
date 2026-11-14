"""Bind Laravel bridge observations to the requested registry distributions."""

import json
from pathlib import Path
import re
import sys


FRAMEWORKS = {
    "12.72.0": "b089d3020e508f49b3625726ab4a2c897f1b78d5",
    "13.41.0": "95934d2868c90c1aa8e711791ea33d1e990e1719",
}


def package(rows, name):
    matches = [row for row in rows if row.get("name") == name]
    if len(matches) != 1:
        raise ValueError(f"{name} must occur exactly once")
    return matches[0]


def check(root, sdk_version, sdk_reference, laravel_version):
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", sdk_version):
        raise ValueError("An exact stable SDK version is required")
    if not re.fullmatch(r"[0-9a-f]{40}", sdk_reference):
        raise ValueError("An exact SDK source reference is required")
    framework_reference = FRAMEWORKS[laravel_version]
    manifest = json.loads((root / "composer.json").read_text())
    if "repositories" in manifest:
        raise ValueError("Registry qualification cannot override repositories")
    lock = json.loads((root / "composer.lock").read_text())
    installed = json.loads((root / "vendor/composer/installed.json").read_text())
    for name, version, reference in [
        ("durable-workflow/sdk", sdk_version, sdk_reference),
        ("laravel/framework", laravel_version, framework_reference),
    ]:
        if manifest["require"].get(name) != version:
            raise ValueError(f"{name} must have an exact root requirement")
        for rows in [lock["packages"], installed["packages"]]:
            row = package(rows, name)
            if row["version"].removeprefix("v") != version:
                raise ValueError(f"{name} version mismatch")
            if row["source"]["reference"] != reference:
                raise ValueError(f"{name} source mismatch")
            if row["dist"]["reference"] != reference or row["dist"]["type"] != "zip":
                raise ValueError(f"{name} distribution mismatch")
        if package(installed["packages"], name).get("installation-source") != "dist":
            raise ValueError(f"{name} was not installed from its distribution")
    sdk = json.loads((root / "vendor/durable-workflow/sdk/composer.json").read_text())
    if sdk["extra"]["durable-workflow"]["product-train"] != sdk_version:
        raise ValueError("Installed SDK product train mismatch")
    return {"sdk_version": sdk_version, "sdk_reference": sdk_reference,
            "laravel_version": laravel_version, "laravel_reference": framework_reference,
            "source_fallback": False, "repository_override": False}


if __name__ == "__main__":
    print(json.dumps(check(Path(sys.argv[1]), *sys.argv[2:5])))
