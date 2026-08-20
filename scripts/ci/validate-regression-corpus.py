#!/usr/bin/env python3
"""Validate immutable replay and payload-codec regression evidence."""

from __future__ import annotations

import argparse
import base64
import binascii
import fnmatch
import hashlib
import json
import math
import re
import shutil
import subprocess
import sys
import tempfile
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

POLICY_SCHEMA = "durable-workflow.regression-corpus-policy/v1"
CODEC_SCHEMA = "durable-workflow.codec-regression/v1"
REPLAY_SCHEMA = "durable-workflow.replay-regression/v1"
GOLDEN_HISTORY_SCHEMA = "durable-workflow.golden-history.v1"
PHP_PAYLOAD_PROJECTION_SCHEMA = "durable-workflow.php-replay-payload-projection/v1"
PHP_CODEC_VALUE_PROJECTION_SCHEMA = "durable-workflow.php-codec-value-projection/v1"
SUPPORTED_FORMATS = {
    "avro-value-golden-v1",
    "codec-regression-v1",
    "golden-history-v1",
    "replay-regression-v1",
}
SUPPORTED_CATEGORIES = {"codec", "replay"}
SUPPORTED_BINDINGS = {"php", "python", "rust"}
PHP_FIXTURE_FORMATS = {
    "codec": {"avro-value-golden-v1", "codec-regression-v1"},
    "replay": {"replay-regression-v1"},
}
CODEC_DEPENDENCY_DEFINITIONS = {"composer.json", "composer.lock"}
CODEC_DEPENDENCY_SOURCES = {
    Path("apache/avro/lang/php/lib"),
    Path("psr/container/src"),
    Path("psr/log/src"),
}
REQUIRED_CODEC_DEPENDENCY_SOURCE = Path("apache/avro/lang/php/lib")
CODEC_CONSUMER_CONTRACT_PATHS = {
    "scripts/ci/run-codec-regression-fixture.php",
    "tests/Support/CodecRegressionFixture.php",
}
CODEC_TASK_TRANSPORT_WITNESS_RUNNER = "scripts/ci/run-replay-regression-fixture.php"
CODEC_TASK_TRANSPORT_WITNESS_FIXTURE = (
    "tests/fixtures/replay-regressions/fiber-straight-line-transition.json"
)
CODEC_OUTCOME_CODES = {
    "pass": 0,
    "assertion-failure": 1,
    "operational-error": 2,
}
PORTABLE_PHP_FIXTURE_GLOB = re.compile(
    r"^(?:[A-Za-z0-9._-]+/)*(?:[A-Za-z0-9._-]+|\*)\.json$"
)
ZERO_COMMIT = re.compile(r"^0+$")
LEGACY_MALFORMED_WIRE_REPAIRS = {
    "%%%": "JSUl",
}
PHP_NAMED_FUNCTION = re.compile(
    r"(?m)^[ \t]*(?:(?:abstract|final|private|protected|public|readonly|static)\s+)*"
    r"function\s+([A-Za-z_][A-Za-z0-9_]*)\s*\("
)
PHP_REPLAY_NEUTRAL_OBSERVABILITY_STATEMENT = re.compile(
    r"[ \t]*\$this->handlerFailure\([^();=]*\);[ \t]*"
)
PHP_NUMERIC_STRING = re.compile(
    r"[+-]?(?:(?:[0-9]+(?:\.[0-9]*)?)|(?:\.[0-9]+))(?:[eE][+-]?[0-9]+)?"
)
PHP_INTEGER_STRING = re.compile(r"[+-]?[0-9]+")
PHP_INT_MIN = -(2**63)
PHP_INT_MAX = 2**63 - 1
PHP_INT_MODULUS = 2**64
SUBPROCESS_DIAGNOSTIC_LIMIT = 4096


class CorpusError(RuntimeError):
    """The regression-corpus contract is not satisfied."""


class PhpReplayPayloadProjector:
    """Obtain replay payload identity from the official PHP consumer."""

    def __init__(
        self,
        *,
        executable: str,
        runner: Path,
        vendor_root: Path,
        source_root: Path,
    ) -> None:
        self.executable = executable
        self.runner = runner
        self.vendor_root = vendor_root
        self.source_root = source_root
        self.cache: dict[str, Any] = {}

    def project(self, operation: str, value: Any) -> Any:
        try:
            request = json.dumps(
                {"operation": operation, "value": value},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError) as error:
            raise CorpusError(
                "replay payload cannot be passed to the official PHP consumer"
            ) from error
        if request in self.cache:
            return self.cache[request]
        if not self.runner.is_file():
            raise CorpusError(
                f"official PHP replay payload consumer is unavailable: {self.runner}"
            )
        if not self.vendor_root.is_dir() or not self.source_root.is_dir():
            raise CorpusError(
                "official PHP replay payload consumer dependencies are unavailable"
            )

        try:
            result = subprocess.run(
                [
                    self.executable,
                    str(self.runner),
                    "--vendor-root",
                    str(self.vendor_root),
                    "--source-root",
                    str(self.source_root),
                ],
                input=request,
                check=False,
                capture_output=True,
                text=True,
            )
        except OSError as error:
            raise CorpusError(
                "official PHP replay payload consumer is unavailable"
            ) from error
        if result.returncode != 0:
            raise CorpusError(
                "official PHP replay payload consumer rejected a supported payload: "
                f"{_process_detail(result)}"
            )
        try:
            report = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise CorpusError(
                "official PHP replay payload consumer returned an invalid projection"
            ) from error
        if (
            not isinstance(report, Mapping)
            or report.get("schema") != PHP_PAYLOAD_PROJECTION_SCHEMA
        ):
            raise CorpusError(
                "official PHP replay payload consumer returned an invalid projection"
            )
        projections = report.get("projections")
        if (
            not isinstance(projections, Sequence)
            or isinstance(projections, str | bytes)
            or len(projections) != 2
        ):
            raise CorpusError(
                "official PHP replay payload consumer returned an invalid projection"
            )
        if projections[0] != projections[1]:
            raise CorpusError(
                "official PHP replay payload consumer returned inconsistent projections"
            )
        self.cache[request] = projections[0]
        return projections[0]


class PhpCodecValueProjector:
    """Obtain encode-reject identity from the official PHP fixture consumer."""

    def __init__(
        self,
        *,
        executable: str,
        runner: Path,
        vendor_root: Path,
        consumer_root: Path,
        source_root: Path,
    ) -> None:
        self.executable = executable
        self.runner = runner
        self.vendor_root = vendor_root
        self.consumer_root = consumer_root
        self.source_root = source_root
        self.cache: dict[str, Any] = {}

    def project(self, value: Any) -> Any:
        try:
            request = json.dumps(
                {"value": value},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError) as error:
            raise CorpusError(
                "codec value cannot be passed to the official PHP consumer"
            ) from error
        if request in self.cache:
            return self.cache[request]
        if not self.runner.is_file():
            raise CorpusError(
                f"official PHP codec value consumer is unavailable: {self.runner}"
            )
        if (
            not self.vendor_root.is_dir()
            or not self.consumer_root.is_dir()
            or not self.source_root.is_dir()
        ):
            raise CorpusError(
                "official PHP codec value consumer dependencies are unavailable"
            )

        try:
            result = subprocess.run(
                [
                    self.executable,
                    str(self.runner),
                    "--vendor-root",
                    str(self.vendor_root),
                    "--consumer-root",
                    str(self.consumer_root),
                    "--source-root",
                    str(self.source_root),
                ],
                input=request,
                check=False,
                capture_output=True,
                text=True,
            )
        except OSError as error:
            raise CorpusError(
                "official PHP codec value consumer is unavailable"
            ) from error
        if result.returncode != 0:
            raise CorpusError(
                "official PHP codec value consumer could not project tagged value: "
                f"{_process_detail(result)}"
            )
        try:
            report = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise CorpusError(
                "official PHP codec value consumer returned an invalid projection"
            ) from error
        if (
            not isinstance(report, Mapping)
            or report.get("schema") != PHP_CODEC_VALUE_PROJECTION_SCHEMA
        ):
            raise CorpusError(
                "official PHP codec value consumer returned an invalid projection"
            )
        projections = report.get("projections")
        if (
            not isinstance(projections, Sequence)
            or isinstance(projections, str | bytes)
            or len(projections) != 2
        ):
            raise CorpusError(
                "official PHP codec value consumer returned an invalid projection"
            )
        if projections[0] != projections[1]:
            raise CorpusError(
                "official PHP codec value consumer returned inconsistent projections"
            )
        self.cache[request] = projections[0]
        return projections[0]


@dataclass(frozen=True)
class Evidence:
    category: str
    identity: str
    path: str
    protocol_version: str
    semantic_digest: str
    duplicate_digests: tuple[str, ...]
    supersedes: tuple[str, ...] = ()


def _canonical_digest(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _object(value: Any, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CorpusError(f"{context} must be an object")
    return value


def _list(value: Any, context: str, *, nonempty: bool = False) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        raise CorpusError(f"{context} must be an array")
    if nonempty and not value:
        raise CorpusError(f"{context} must not be empty")
    return value


def _string(value: Any, context: str) -> str:
    if not isinstance(value, str) or not value:
        raise CorpusError(f"{context} must be a non-empty string")
    return value


def _nullable_string(value: Any, context: str) -> str | None:
    if value is None:
        return None
    return _string(value, context)


def _unique_strings(value: Any, context: str, *, allowed: set[str] | None = None) -> tuple[str, ...]:
    values = tuple(_string(item, f"{context}[]") for item in _list(value, context, nonempty=True))
    if len(values) != len(set(values)):
        raise CorpusError(f"{context} contains duplicates")
    if allowed is not None and not set(values) <= allowed:
        raise CorpusError(f"{context} contains unsupported values: {sorted(set(values) - allowed)}")
    return values


def _json(content: bytes, path: str) -> Mapping[str, Any]:
    try:
        value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CorpusError(f"{path} is not valid UTF-8 JSON: {error}") from error
    return _object(value, path)


def _canonical_base64(value: str, context: str) -> str:
    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as error:
        raise CorpusError(f"{context} is not canonical base64") from error
    canonical = base64.b64encode(decoded).decode("ascii")
    if value != canonical:
        raise CorpusError(f"{context} is not canonical base64")
    return canonical


def _canonical_wire_replacement(value: str) -> str | None:
    """Return the only permitted canonical replacement for a legacy wire."""

    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        return LEGACY_MALFORMED_WIRE_REPAIRS.get(value)

    canonical = base64.b64encode(decoded).decode("ascii")
    return canonical if canonical != value else None


def _avro_golden_migration(base_content: bytes, current_content: bytes) -> bool:
    """Allow one-way repairs of legacy malformed-frame wire metadata."""

    try:
        base_document = json.loads(base_content)
        current_document = json.loads(current_content)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    if not isinstance(base_document, dict) or not isinstance(current_document, dict):
        return False
    base_frames = base_document.get("malformed_frames")
    current_frames = current_document.get("malformed_frames")
    if not isinstance(base_frames, list) or not isinstance(current_frames, list):
        return False
    if len(base_frames) != len(current_frames):
        return False

    migrated = False
    for index, (base_frame, current_frame) in enumerate(
        zip(base_frames, current_frames, strict=True)
    ):
        if not isinstance(base_frame, dict) or not isinstance(current_frame, dict):
            return False
        base_wire = base_frame.get("wire_base64")
        current_wire = current_frame.get("wire_base64")
        if base_wire != current_wire:
            if not isinstance(base_wire, str) or not isinstance(current_wire, str):
                return False
            if current_wire != _canonical_wire_replacement(base_wire):
                return False
            try:
                _canonical_base64(
                    current_wire,
                    f"current.malformed_frames[{index}].wire_base64",
                )
            except CorpusError:
                return False
            base_frame["wire_base64"] = current_wire
            migrated = True

        base_name = base_frame.get("name")
        current_name = current_frame.get("name")
        if base_name != current_name:
            if (
                base_name != "invalid_base64"
                or current_name != "decoded_non_magic_bytes"
                or current_wire != "JSUl"
                or base_frame.get("error") != "invalid_payload_framing"
                or current_frame.get("error") != "invalid_payload_framing"
            ):
                return False
            base_frame["name"] = current_name
            migrated = True

    return migrated and base_document == current_document


def _canonical_command_type(value: str) -> str:
    """Normalize runtime command class names to their wire discriminator."""

    words = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", value)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", words).lower()


def _canonical_replay_command(value: Any) -> Any:
    """Normalize the command forms accepted by replay consumers."""

    if not isinstance(value, Mapping):
        return value

    command = dict(value)
    command_type = command.get("command_type")
    if not isinstance(command_type, str) or not command_type:
        return command

    wire_type = _canonical_command_type(command_type)
    declared_type = command.get("type")
    if declared_type is None or declared_type == wire_type:
        command.pop("command_type")
        command["type"] = wire_type
    return command


def _canonical_replay_commands(value: Any) -> Any:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        return value
    return [_canonical_replay_command(command) for command in value]


def _php_int_value(value: Any) -> int | None:
    """Match is_numeric() followed by PHP's integer cast."""

    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        if PHP_INT_MIN <= value <= PHP_INT_MAX:
            return value
        # json_decode() materializes an out-of-range JSON integer as a double.
        try:
            value = float(value)
        except OverflowError:
            return 0
    if isinstance(value, float):
        if not math.isfinite(value):
            return 0
        # PHP wraps finite double casts to the platform integer width.
        wrapped = int(value) % PHP_INT_MODULUS
        return wrapped if wrapped <= PHP_INT_MAX else wrapped - PHP_INT_MODULUS
    if not isinstance(value, str) or not PHP_NUMERIC_STRING.fullmatch(value.strip()):
        return None
    numeric = value.strip()
    if PHP_INTEGER_STRING.fullmatch(numeric):
        try:
            exact = int(numeric)
        except ValueError:
            exact = None
        if exact is not None and PHP_INT_MIN <= exact <= PHP_INT_MAX:
            return exact
    # Numeric strings cap finite overflow, while a non-finite double casts to zero.
    parsed = float(numeric)
    if not math.isfinite(parsed):
        return 0
    return min(max(int(parsed), PHP_INT_MIN), PHP_INT_MAX)


def _php_string_value(value: Any) -> str:
    """Return the scalar spelling produced by the replay consumer's string cast."""

    if isinstance(value, str):
        return value
    if value is True:
        return "1"
    if value is False or value is None:
        return ""
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _php_bool_value(value: Any) -> bool:
    if value is None or value is False or value == 0 or value == "":
        return False
    if value == "0":
        return False
    if isinstance(value, Mapping | Sequence) and not isinstance(value, str | bytes):
        return bool(value)
    return True


def _payload_value(payload: Mapping[str, Any], *fields: str) -> Any:
    for field in fields:
        value = payload.get(field)
        if value is not None:
            return value
    return None


def _canonical_payload_envelope(
    value: Any,
    payload_projector: PhpReplayPayloadProjector | None,
    *,
    operation: str = "replayer-result",
) -> Any:
    """Resolve replay payloads through the official binding when available."""

    if payload_projector is not None:
        return payload_projector.project(operation, value)

    if isinstance(value, str):
        return {"codec": "avro", "blob": value}
    if isinstance(value, Mapping):
        codec = value.get("codec")
        if codec is None:
            codec = "avro"
        return {"codec": codec, "blob": value.get("blob")}
    return value


def _payload_detail(payload: Mapping[str, Any], shape: str) -> str | None:
    value = {
        "activity": _payload_value(payload, "activity_type", "activity_name"),
        "timer": payload.get("delay_seconds"),
        "child_workflow": _payload_value(
            payload,
            "child_workflow_type",
            "workflow_type",
        ),
    }.get(shape)
    return None if value is None else _php_string_value(value)


def _resolved_sequence(payload: Mapping[str, Any]) -> int | None:
    value = _payload_value(payload, "sequence", "workflow_sequence")
    return _php_int_value(value)


def _canonical_failure_payload(
    event_type: str,
    payload: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Project fields exposed through the ActivityFailed exception."""

    if event_type in {"ActivityFailed", "ActivityTimedOut"}:
        message = _payload_value(payload, "message", "closed_reason")
        return {
            "message": _php_string_value(
                "Activity failed." if message is None else message
            ),
            "activity_type": _payload_detail(payload, "activity"),
            "exception_type": (
                _php_string_value(payload["exception_type"])
                if payload.get("exception_type") is not None
                else None
            ),
            "non_retryable": _php_bool_value(payload.get("non_retryable", False)),
            "failure": dict(payload),
        }
    message = payload.get("message")
    return {
        "message": _php_string_value(
            "Child workflow failed." if message is None else message
        ),
        "failure": dict(payload),
    }


def _canonical_recorded_steps(
    value: Any,
    payload_projector: PhpReplayPayloadProjector | None,
) -> Sequence[Mapping[str, Any]]:
    """Resolve history exactly as Replayer::recordedSteps does."""

    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        return []

    steps: dict[int, dict[str, Any]] = {}
    fallback_sequence = 1_000_000
    for index, raw_event in enumerate(value):
        event = _object(raw_event, f"replay history[{index}]")
        event_type = _php_string_value(_payload_value(event, "event_type", "type"))
        raw_payload = event.get("payload")
        payload = dict(raw_payload) if isinstance(raw_payload, Mapping) else {}
        sequence = _resolved_sequence(payload)
        if sequence is None:
            sequence = fallback_sequence
            fallback_sequence += 1

        if event_type in {"ActivityScheduled", "ActivityStarted"}:
            steps.setdefault(
                sequence,
                {
                    "sequence": sequence,
                    "shape": "activity",
                    "detail": _payload_detail(payload, "activity"),
                    "resolved": False,
                },
            )
        elif event_type == "ActivityCompleted":
            detail = _payload_detail(payload, "activity")
            if detail is None and sequence in steps:
                detail = steps[sequence]["detail"]
            steps[sequence] = {
                "sequence": sequence,
                "shape": "activity",
                "detail": detail,
                "resolved": True,
                "value": _canonical_payload_envelope(
                    _payload_value(payload, "result", "output"),
                    payload_projector,
                ),
            }
        elif event_type in {"ActivityFailed", "ActivityTimedOut"}:
            detail = _payload_detail(payload, "activity")
            if detail is None and sequence in steps:
                detail = steps[sequence]["detail"]
            steps[sequence] = {
                "sequence": sequence,
                "shape": "activity",
                "detail": detail,
                "resolved": True,
                "failure": _canonical_failure_payload(event_type, payload),
            }
        elif event_type == "TimerScheduled":
            if payload.get("timer_kind") not in {
                "condition_timeout",
                "signal_timeout",
            }:
                steps.setdefault(
                    sequence,
                    {
                        "sequence": sequence,
                        "shape": "timer",
                        "detail": _payload_detail(payload, "timer"),
                        "resolved": False,
                    },
                )
        elif event_type == "TimerFired":
            if payload.get("timer_kind") not in {
                "condition_timeout",
                "signal_timeout",
            }:
                detail = _payload_detail(payload, "timer")
                if detail is None and sequence in steps:
                    detail = steps[sequence]["detail"]
                steps[sequence] = {
                    "sequence": sequence,
                    "shape": "timer",
                    "detail": detail,
                    "resolved": True,
                    "value": None,
                }
        elif event_type in {"ChildWorkflowScheduled", "ChildRunStarted"}:
            steps.setdefault(
                sequence,
                {
                    "sequence": sequence,
                    "shape": "child_workflow",
                    "detail": _payload_detail(payload, "child_workflow"),
                    "resolved": False,
                },
            )
        elif event_type == "ChildRunCompleted":
            detail = _payload_detail(payload, "child_workflow")
            if detail is None and sequence in steps:
                detail = steps[sequence]["detail"]
            steps[sequence] = {
                "sequence": sequence,
                "shape": "child_workflow",
                "detail": detail,
                "resolved": True,
                "value": _canonical_payload_envelope(
                    _payload_value(payload, "result", "output"),
                    payload_projector,
                ),
            }
        elif event_type in {
            "ChildRunFailed",
            "ChildRunCancelled",
            "ChildRunTerminated",
        }:
            detail = _payload_detail(payload, "child_workflow")
            if detail is None and sequence in steps:
                detail = steps[sequence]["detail"]
            steps[sequence] = {
                "sequence": sequence,
                "shape": "child_workflow",
                "detail": detail,
                "resolved": True,
                "failure": _canonical_failure_payload(event_type, payload),
            }
        elif event_type == "SideEffectRecorded":
            steps[sequence] = {
                "sequence": sequence,
                "shape": "side_effect",
                "detail": None,
                "resolved": True,
                "value": _canonical_payload_envelope(
                    _payload_value(payload, "result", "output"),
                    payload_projector,
                ),
            }
        elif event_type == "SearchAttributesUpserted":
            steps[sequence] = {
                "sequence": sequence,
                "shape": "search_attributes",
                "detail": None,
                "resolved": True,
                "value": None,
            }

    return [steps[sequence] for sequence in sorted(steps)]


def _canonical_context_history(
    value: Any,
    workflow_type: str,
    workflow_input: Any,
    payload_projector: PhpReplayPayloadProjector | None,
) -> Any:
    """Project history read directly by the supported WorkflowContext paths."""

    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        return None
    input_values = (
        workflow_input
        if isinstance(workflow_input, Sequence)
        and not isinstance(workflow_input, str | bytes)
        else []
    )
    target = _php_string_value(input_values[0]) if input_values else ""

    if workflow_type == "golden.signal":
        signals = []
        for index, raw_event in enumerate(value):
            event = _object(raw_event, f"replay history[{index}]")
            if _payload_value(event, "event_type", "type") != "SignalReceived":
                continue
            raw_payload = event.get("payload")
            payload = raw_payload if isinstance(raw_payload, Mapping) else {}
            if payload.get("signal_name") != target:
                continue
            signals.append(
                _canonical_payload_envelope(
                    _payload_value(payload, "value", "input", "arguments"),
                    payload_projector,
                    operation="workflow-signal-arguments",
                )
            )
        return {"signals": signals}

    if workflow_type == "golden.update":
        updates = []
        seen = set()
        for index, raw_event in enumerate(value):
            event = _object(raw_event, f"replay history[{index}]")
            if _payload_value(event, "event_type", "type") not in {
                "UpdateAccepted",
                "UpdateApplied",
            }:
                continue
            raw_payload = event.get("payload")
            payload = raw_payload if isinstance(raw_payload, Mapping) else {}
            if payload.get("update_name") != target or payload.get("arguments") is None:
                continue
            update_id = _php_string_value(payload.get("update_id"))
            if update_id and update_id in seen:
                continue
            if update_id:
                seen.add(update_id)
            updates.append(
                _canonical_payload_envelope(
                    payload["arguments"],
                    payload_projector,
                    operation="workflow-update-arguments",
                )
            )
        return {"updates": updates}

    if workflow_type == "golden.worker-update":
        accepted: Mapping[str, Any] = {}
        for index, raw_event in reversed(list(enumerate(value))):
            event = _object(raw_event, f"replay history[{index}]")
            if _payload_value(event, "event_type", "type") != "UpdateAccepted":
                continue
            raw_payload = event.get("payload")
            payload = raw_payload if isinstance(raw_payload, Mapping) else {}
            if payload.get("update_id") == "update-1":
                accepted = payload
                break
        effective_update_name = (
            accepted["update_name"]
            if accepted.get("update_name") is not None
            else "golden.update"
        )
        return {
            "worker_update": {
                "update_name": (
                    payload_projector.project(
                        "worker-update-name",
                        effective_update_name,
                    )
                    if payload_projector is not None
                    else _php_string_value(effective_update_name)
                ),
                "arguments": _canonical_payload_envelope(
                    (
                        accepted.get("arguments")
                        if accepted.get("arguments") is not None
                        else workflow_input
                    ),
                    payload_projector,
                    operation="worker-update-arguments",
                ),
            }
        }

    return None


def _canonical_replay_history(
    value: Any,
    workflow_type: str,
    workflow_input: Any,
    payload_projector: PhpReplayPayloadProjector | None,
) -> Mapping[str, Any]:
    history: dict[str, Any] = {
        "recorded_steps": _canonical_recorded_steps(value, payload_projector),
    }
    context = _canonical_context_history(
        value,
        workflow_type,
        workflow_input,
        payload_projector,
    )
    if context is not None:
        history["context"] = context
    return history


def _merge_replay_assertions(left: Any, right: Any, context: str) -> Any:
    """Merge two compatible partial assertions over the same replay output."""

    if isinstance(left, Mapping) and isinstance(right, Mapping):
        merged = dict(left)
        for key, value in right.items():
            if key in merged:
                merged[key] = _merge_replay_assertions(
                    merged[key],
                    value,
                    f"{context}.{key}",
                )
            else:
                merged[key] = value
        return merged

    if (
        isinstance(left, Sequence)
        and not isinstance(left, str | bytes)
        and isinstance(right, Sequence)
        and not isinstance(right, str | bytes)
    ):
        if len(left) != len(right):
            raise CorpusError(f"replay command assertions conflict at {context}")
        return [
            _merge_replay_assertions(left_item, right_item, f"{context}[{index}]")
            for index, (left_item, right_item) in enumerate(
                zip(left, right, strict=True)
            )
        ]

    if left != right:
        raise CorpusError(f"replay command assertions conflict at {context}")
    return left


def _canonical_executed_commands(
    command_sequence: Any,
    expected: Mapping[str, Any],
) -> Any:
    """Collapse every consumer-supported command assertion onto one output."""

    executed_commands = (
        _canonical_replay_commands(command_sequence)
        if command_sequence is not None
        else None
    )
    expected_sequence = expected.get("command_sequence")
    if expected_sequence is not None:
        canonical_expected = _canonical_replay_commands(expected_sequence)
        executed_commands = (
            canonical_expected
            if executed_commands is None
            else _merge_replay_assertions(
                executed_commands,
                canonical_expected,
                "command_sequence",
            )
        )

    first_command = {
        key: value
        for key, value in expected.items()
        if key != "command_sequence"
    }
    if first_command:
        canonical_first = _canonical_replay_command(first_command)
        if executed_commands is None:
            executed_commands = [canonical_first]
        elif (
            not isinstance(executed_commands, Sequence)
            or isinstance(executed_commands, str | bytes)
            or len(executed_commands) != 1
        ):
            raise CorpusError(
                "flattened expected command requires exactly one executed command"
            )
        else:
            executed_commands = [
                _merge_replay_assertions(
                    executed_commands[0],
                    canonical_first,
                    "command_sequence[0]",
                )
            ]

    return executed_commands


def _replay_semantic(
    *,
    workflow_type: str,
    workflow_input: Any,
    history: Any,
    command_sequence: Any,
    expected: Mapping[str, Any],
    payload_projector: PhpReplayPayloadProjector | None,
) -> Mapping[str, Any]:
    """Project every replay representation onto consumer-executed values."""

    return {
        "workflow": {"type": workflow_type, "input": workflow_input},
        "history": _canonical_replay_history(
            history,
            workflow_type,
            workflow_input,
            payload_projector,
        ),
        "executed_commands": _canonical_executed_commands(
            command_sequence,
            expected,
        ),
    }


def _fixture_evidence(
    *,
    category: str,
    identity: str,
    path: str,
    protocol_version: str,
    semantic_value: Any,
    duplicate_values: Sequence[Any] | None = None,
    supersedes: tuple[str, ...] = (),
) -> Evidence:
    duplicate_values = [semantic_value] if duplicate_values is None else duplicate_values
    return Evidence(
        category=category,
        identity=identity,
        path=path,
        protocol_version=protocol_version,
        semantic_digest=_canonical_digest(semantic_value),
        duplicate_digests=tuple(_canonical_digest(value) for value in duplicate_values),
        supersedes=supersedes,
    )


def _normalized_codec_version(codec: str, version: str) -> str:
    if codec == "avro":
        match = re.fullmatch(r"(?:avro-value-v)?([1-9][0-9]*)", version)
        if match is not None:
            return match.group(1)
    return version


def _codec_semantic(
    *,
    wire: str | None,
    operation: str,
    error: str | None,
    rejected_value: Any = None,
) -> dict[str, Any]:
    semantic = {"failure_policy": {"operation": operation, "error": error}}
    if operation == "encode_reject":
        semantic["value"] = rejected_value
    else:
        semantic["wire_base64"] = wire
    return semantic


def _task_transport_semantic(value: Any, path: str) -> Mapping[str, Any]:
    transport = _object(value, f"{path}.task_transport")
    worker_paths = _unique_strings(
        transport.get("worker_paths"),
        f"{path}.task_transport.worker_paths",
        allowed={"workflow", "update", "activity", "query"},
    )
    cases = _list(
        transport.get("payload_codec_cases"),
        f"{path}.task_transport.payload_codec_cases",
        nonempty=True,
    )
    canonical_cases = []
    case_names = set()
    for index, raw_case in enumerate(cases):
        case_path = f"{path}.task_transport.payload_codec_cases[{index}]"
        case = _object(raw_case, case_path)
        name = _string(case.get("name"), f"{case_path}.name")
        if name in case_names:
            raise CorpusError(f"{path}.task_transport.payload_codec_cases contains duplicate names")
        case_names.add(name)
        present = case.get("present")
        if not isinstance(present, bool):
            raise CorpusError(f"{case_path}.present must be a boolean")
        if present != ("value" in case):
            raise CorpusError(
                f"{case_path}.value must be present exactly when the root codec is present"
            )
        if set(case) - {"name", "present", "value"}:
            raise CorpusError(f"{case_path} contains unsupported fields")
        canonical_case = {"name": name, "present": present}
        if present:
            canonical_case["value"] = case["value"]
        canonical_cases.append(canonical_case)
    expected_error = _string(
        transport.get("expected_error"),
        f"{path}.task_transport.expected_error",
    )
    forbidden_error = _string(
        transport.get("forbidden_error"),
        f"{path}.task_transport.forbidden_error",
    )
    if expected_error == forbidden_error:
        raise CorpusError(f"{path}.task_transport error policies must be distinct")
    if set(transport) != {
        "worker_paths",
        "payload_codec_cases",
        "expected_error",
        "forbidden_error",
    }:
        raise CorpusError(f"{path}.task_transport contains unsupported fields")
    return {
        "worker_paths": worker_paths,
        "payload_codec_cases": canonical_cases,
        "expected_error": expected_error,
        "forbidden_error": forbidden_error,
    }


def _codec_fixture(
    document: Mapping[str, Any],
    path: str,
    binding: str | None,
    value_projector: PhpCodecValueProjector | None,
) -> list[Evidence]:
    _string(document.get("$schema"), f"{path}.$schema")
    if document.get("fixture_schema") != CODEC_SCHEMA:
        raise CorpusError(f"{path} must declare fixture_schema={CODEC_SCHEMA}")
    identity = _string(document.get("id"), f"{path}.id")
    protocol = _object(document.get("protocol"), f"{path}.protocol")
    codec = _string(protocol.get("codec"), f"{path}.protocol.codec")
    _string(protocol.get("schema"), f"{path}.protocol.schema")
    version = _string(protocol.get("version"), f"{path}.protocol.version")
    _nullable_string(protocol.get("fingerprint"), f"{path}.protocol.fingerprint")
    bindings = _unique_strings(
        document.get("bindings"),
        f"{path}.bindings",
        allowed=SUPPORTED_BINDINGS,
    )
    if binding is not None and binding not in bindings:
        raise CorpusError(f"{path} does not name this repository's {binding} binding")

    value = _object(document.get("value"), f"{path}.value")
    _string(value.get("type"), f"{path}.value.type")
    framing = _object(document.get("framing"), f"{path}.framing")
    _string(framing.get("encoding"), f"{path}.framing.encoding")
    wire = framing.get("wire_base64")
    if wire is not None and not isinstance(wire, str):
        raise CorpusError(f"{path}.framing.wire_base64 must be a string or null")
    policy = _object(document.get("failure_policy"), f"{path}.failure_policy")
    operation = _string(policy.get("operation"), f"{path}.failure_policy.operation")
    if operation not in {"round_trip", "decode_reject", "encode_reject"}:
        raise CorpusError(f"{path}.failure_policy.operation is unsupported")
    error = _nullable_string(policy.get("error"), f"{path}.failure_policy.error")
    if operation in {"round_trip", "decode_reject"} and wire is None:
        raise CorpusError(f"{path} must include wire_base64 for {operation}")
    if operation == "round_trip" and error is not None:
        raise CorpusError(f"{path} round-trip evidence cannot declare an error")
    if operation != "round_trip" and error is None:
        raise CorpusError(f"{path} rejection evidence must declare its stable error policy")
    canonical_wire = (
        _canonical_base64(wire, f"{path}.framing.wire_base64")
        if wire is not None
        else None
    )
    rejected_value: Any = None
    if operation == "encode_reject":
        if binding == "php":
            if value_projector is None:
                raise CorpusError(
                    f"{path} cannot project encode_reject through the official PHP consumer"
                )
            rejected_value = value_projector.project(value)
        else:
            rejected_value = value

    supersedes = tuple(
        _string(item, f"{path}.supersedes[]")
        for item in _list(document.get("supersedes", []), f"{path}.supersedes")
    )
    if len(supersedes) != len(set(supersedes)) or identity in supersedes:
        raise CorpusError(f"{path}.supersedes is invalid")
    # Declarations that the PHP corpus consumer only validates structurally do
    # not distinguish another execution of the same binding behavior.
    semantic = _codec_semantic(
        wire=canonical_wire,
        operation=operation,
        error=error,
        rejected_value=rejected_value,
    )
    if "task_transport" in document:
        task_transport = _task_transport_semantic(document["task_transport"], path)
        semantic = {
            "payload": semantic,
            "task_transport": task_transport,
        }
    return [
        _fixture_evidence(
            category="codec",
            identity=identity,
            path=path,
            protocol_version=_normalized_codec_version(codec, version),
            semantic_value=semantic,
            supersedes=supersedes,
        )
    ]


def _replay_fixture(
    document: Mapping[str, Any],
    path: str,
    binding: str | None,
    payload_projector: PhpReplayPayloadProjector | None,
) -> list[Evidence]:
    _string(document.get("$schema"), f"{path}.$schema")
    if document.get("fixture_schema") != REPLAY_SCHEMA:
        raise CorpusError(f"{path} must declare fixture_schema={REPLAY_SCHEMA}")
    identity = _string(document.get("id"), f"{path}.id")
    protocol_version = _string(document.get("protocol_version"), f"{path}.protocol_version")
    bindings = _unique_strings(
        document.get("bindings"),
        f"{path}.bindings",
        allowed=SUPPORTED_BINDINGS,
    )
    if binding is not None and binding not in bindings:
        raise CorpusError(f"{path} does not name this repository's {binding} binding")
    workflow = _object(document.get("workflow"), f"{path}.workflow")
    _string(workflow.get("type"), f"{path}.workflow.type")
    history = document.get("history")
    commands = document.get("command_sequence")
    workflow_tasks = document.get("workflow_tasks")
    if history is None and commands is None and workflow_tasks is None:
        raise CorpusError(
            f"{path} must include history, command_sequence, or workflow_tasks"
        )
    if history is not None:
        _list(history, f"{path}.history", nonempty=True)
    if commands is not None:
        _list(commands, f"{path}.command_sequence", nonempty=True)
    if workflow_tasks is not None and commands is not None:
        raise CorpusError(
            f"{path} cannot combine workflow_tasks with command_sequence"
        )
    expected = _object(document.get("expected"), f"{path}.expected")
    if not expected:
        raise CorpusError(f"{path}.expected must not be empty")
    supersedes = tuple(
        _string(item, f"{path}.supersedes[]")
        for item in _list(document.get("supersedes", []), f"{path}.supersedes")
    )
    if len(supersedes) != len(set(supersedes)) or identity in supersedes:
        raise CorpusError(f"{path}.supersedes is invalid")
    workflow_input = workflow.get("input", workflow.get("arguments", []))
    if workflow_tasks is None:
        # Keep the digest aligned with values passed to or asserted after Replayer.
        semantic = _replay_semantic(
            workflow_type=workflow["type"],
            workflow_input=workflow_input,
            history=history if history is not None else [],
            command_sequence=commands,
            expected=expected,
            payload_projector=payload_projector,
        )
    else:
        task_definitions = _list(
            workflow_tasks,
            f"{path}.workflow_tasks",
            nonempty=True,
        )
        task_expectations = _list(
            expected.get("workflow_tasks"),
            f"{path}.expected.workflow_tasks",
            nonempty=True,
        )
        if len(task_definitions) != len(task_expectations):
            raise CorpusError(
                f"{path}.expected.workflow_tasks must match workflow_tasks length"
            )
        executed_tasks = []
        for index, (raw_task, raw_expected) in enumerate(
            zip(task_definitions, task_expectations, strict=True)
        ):
            task = _object(raw_task, f"{path}.workflow_tasks[{index}]")
            if set(task) != {"workflow_id", "run_id"}:
                raise CorpusError(
                    f"{path}.workflow_tasks[{index}] must contain only workflow_id and run_id"
                )
            expected_task = _object(
                raw_expected,
                f"{path}.expected.workflow_tasks[{index}]",
            )
            if not expected_task:
                raise CorpusError(
                    f"{path}.expected.workflow_tasks[{index}] must not be empty"
                )
            executed_tasks.append(
                {
                    "workflow_id": _string(
                        task.get("workflow_id"),
                        f"{path}.workflow_tasks[{index}].workflow_id",
                    ),
                    "run_id": _string(
                        task.get("run_id"),
                        f"{path}.workflow_tasks[{index}].run_id",
                    ),
                    "executed_commands": _canonical_executed_commands(
                        None,
                        expected_task,
                    ),
                }
            )
        semantic = {
            "workflow": {"type": workflow["type"], "input": workflow_input},
            "history": _canonical_replay_history(
                history if history is not None else [],
                workflow["type"],
                workflow_input,
                payload_projector,
            ),
            "workflow_tasks": executed_tasks,
        }
    return [
        _fixture_evidence(
            category="replay",
            identity=identity,
            path=path,
            protocol_version=protocol_version,
            semantic_value=semantic,
            supersedes=supersedes,
        )
    ]


def _avro_golden_fixture(document: Mapping[str, Any], path: str) -> list[Evidence]:
    _string(document.get("schema"), f"{path}.schema")
    _string(document.get("fingerprint"), f"{path}.fingerprint")
    fixture_version = "avro-value-v1"
    protocol_version = "1"
    evidence: list[Evidence] = []
    sections = {
        "case": _list(document.get("cases"), f"{path}.cases", nonempty=True),
        "malformed": _list(document.get("malformed_frames"), f"{path}.malformed_frames", nonempty=True),
        "alternate": _list(document.get("alternate_map_orders"), f"{path}.alternate_map_orders", nonempty=True),
    }
    for section, entries in sections.items():
        for index, raw_entry in enumerate(entries):
            entry = _object(raw_entry, f"{path}.{section}[{index}]")
            name = _string(entry.get("name"), f"{path}.{section}[{index}].name")
            wire = entry.get("wire_base64")
            if section == "alternate":
                semantic_wire = [
                    _canonical_base64(
                        wire_value,
                        f"{path}.{section}[{index}].wire_base64[]",
                    )
                    for wire_value in _unique_strings(
                        wire,
                        f"{path}.{section}[{index}].wire_base64",
                    )
                ]
            elif section == "case":
                wire_value = _string(wire, f"{path}.{section}[{index}].wire_base64")
                semantic_wire = _canonical_base64(
                    wire_value,
                    f"{path}.{section}[{index}].wire_base64",
                )
            elif not isinstance(wire, str):
                raise CorpusError(f"{path}.{section}[{index}].wire_base64 must be a string")
            else:
                semantic_wire = _canonical_base64(
                    wire,
                    f"{path}.{section}[{index}].wire_base64",
                )
            operation = "decode_reject" if section == "malformed" else "round_trip"
            error = entry.get("error") if section == "malformed" else None
            wires = semantic_wire if isinstance(semantic_wire, list) else [semantic_wire]
            duplicate_values = [
                _codec_semantic(
                    wire=wire_value,
                    operation=operation,
                    error=error,
                )
                for wire_value in wires
            ]
            semantic = (
                duplicate_values[0]
                if len(duplicate_values) == 1
                else {"equivalent_wire_encodings": duplicate_values}
            )
            evidence.append(
                _fixture_evidence(
                    category="codec",
                    identity=f"{fixture_version}:{section}:{name}",
                    path=path,
                    protocol_version=protocol_version,
                    semantic_value=semantic,
                    duplicate_values=duplicate_values,
                )
            )
    return evidence


def _golden_history_fixture(
    document: Mapping[str, Any],
    path: str,
    *,
    require_single_case: bool,
    payload_projector: PhpReplayPayloadProjector | None,
) -> list[Evidence]:
    if document.get("fixture_schema") != GOLDEN_HISTORY_SCHEMA:
        raise CorpusError(f"{path} must declare fixture_schema={GOLDEN_HISTORY_SCHEMA}")
    source = _object(document.get("source"), f"{path}.source")
    runtime = _string(source.get("runtime"), f"{path}.source.runtime")
    version = _string(source.get("version"), f"{path}.source.version")
    protocol_version = _string(
        source.get("worker_protocol_version"),
        f"{path}.source.worker_protocol_version",
    )
    cases = _list(document.get("cases"), f"{path}.cases", nonempty=True)
    if require_single_case and len(cases) != 1:
        raise CorpusError(f"new golden-history fixture {path} must contain exactly one minimal case")
    evidence: list[Evidence] = []
    for index, raw_case in enumerate(cases):
        case = _object(raw_case, f"{path}.cases[{index}]")
        name = _string(case.get("name"), f"{path}.cases[{index}].name")
        history = _list(case.get("history"), f"{path}.cases[{index}].history", nonempty=True)
        expected = case.get("expected", case.get("expected_state"))
        _object(expected, f"{path}.cases[{index}].expected")
        workflow_type = case.get("workflow_type", case.get("scenario"))
        _string(workflow_type, f"{path}.cases[{index}].workflow identity")
        semantic = _replay_semantic(
            workflow_type=workflow_type,
            workflow_input=case.get("start_input", []),
            history=history,
            command_sequence=case.get("command_sequence"),
            expected=expected,
            payload_projector=payload_projector,
        )
        evidence.append(
            _fixture_evidence(
                category="replay",
                identity=f"{runtime}@{version}:{name}",
                path=path,
                protocol_version=protocol_version,
                semantic_value=semantic,
            )
        )
    return evidence


def _run(command: Sequence[str], root: Path, *, check: bool = True) -> str:
    result = subprocess.run(
        command,
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    if check and result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise CorpusError(f"{' '.join(command)} failed: {detail}")
    return result.stdout


def _run_bytes(command: Sequence[str], root: Path, *, check: bool = True) -> bytes:
    result = subprocess.run(
        command,
        cwd=root,
        check=False,
        capture_output=True,
        text=False,
    )
    if check and result.returncode != 0:
        raw_detail = result.stderr.strip() or result.stdout.strip()
        detail = raw_detail[:SUBPROCESS_DIAGNOSTIC_LIMIT].decode(
            "utf-8",
            errors="replace",
        )
        if len(raw_detail) > SUBPROCESS_DIAGNOSTIC_LIMIT:
            detail += "…"
        if not detail:
            detail = f"exit status {result.returncode}"
        raise CorpusError(f"{' '.join(command)} failed: {detail}")
    return result.stdout


def _run_replay_fixture(
    *,
    root: Path,
    php_executable: str,
    replay_runner: Path,
    vendor_root: Path,
    source_root: Path,
    fixture: Path,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            php_executable,
            str(replay_runner),
            "--vendor-root",
            str(vendor_root),
            "--source-root",
            str(source_root),
            "--fixture",
            str(fixture),
        ],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )


def _run_codec_fixture(
    *,
    root: Path,
    php_executable: str,
    codec_runner: Path,
    vendor_root: Path,
    consumer_root: Path,
    source_root: Path,
    fixture: Path,
    fixture_format: str,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            php_executable,
            str(codec_runner),
            "--vendor-root",
            str(vendor_root),
            "--consumer-root",
            str(consumer_root),
            "--source-root",
            str(source_root),
            "--fixture",
            str(fixture),
            "--format",
            fixture_format,
        ],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )


def _process_detail(result: subprocess.CompletedProcess[str]) -> str:
    return result.stderr.strip() or result.stdout.strip() or f"exit status {result.returncode}"


def _codec_outcome(
    result: subprocess.CompletedProcess[str],
    *,
    path: str,
    revision: str,
) -> str:
    try:
        report = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise CorpusError(
            f"new codec fixture {path} produced an invalid {revision} consumer verdict: "
            f"{_process_detail(result)}"
        ) from error
    if not isinstance(report, Mapping) or not isinstance(report.get("outcome"), str):
        raise CorpusError(
            f"new codec fixture {path} produced an invalid {revision} consumer verdict"
        )
    outcome = report["outcome"]
    expected_code = CODEC_OUTCOME_CODES.get(outcome)
    if expected_code is None or result.returncode != expected_code:
        raise CorpusError(
            f"new codec fixture {path} produced an inconsistent {revision} consumer verdict: "
            f"outcome={outcome}, exit={result.returncode}"
        )
    return outcome


def _materialize_files(root: Path, files: Mapping[str, bytes]) -> None:
    for path, content in files.items():
        destination = root / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)


def _materialize_codec_dependencies(vendor_root: Path, destination: Path) -> None:
    for relative_source in CODEC_DEPENDENCY_SOURCES:
        source = vendor_root / relative_source
        if not source.is_dir():
            if relative_source != REQUIRED_CODEC_DEPENDENCY_SOURCE:
                continue
            raise CorpusError(
                f"PHP codec dependency source is missing: {source}; "
                "install dependencies before validation"
            )
        shutil.copytree(source, destination / relative_source)


def _require_codec_dependencies_unchanged(
    *,
    base_files: Mapping[str, bytes],
    current_files: Mapping[str, bytes],
    changed: set[str],
) -> None:
    changed_definitions = sorted(
        path
        for path in CODEC_DEPENDENCY_DEFINITIONS
        if _codec_dependency_projection(path, base_files.get(path))
        != _codec_dependency_projection(path, current_files.get(path))
    )
    changed_vendor_sources = sorted(
        path for path in changed if Path(path).parts[:1] == ("vendor",)
    )
    if changed_definitions or changed_vendor_sources:
        paths = changed_definitions + changed_vendor_sources
        raise CorpusError(
            "codec implementation and counterfactual dependency definitions "
            f"must change independently: {', '.join(paths)}"
        )


def _codec_dependency_projection(path: str, content: bytes | None) -> Any:
    """Project Composer metadata onto inputs that can change the Avro consumer."""

    if content is None:
        return None
    document = _json(content, path)
    if path == "composer.json":
        require = document.get("require")
        requirements = require if isinstance(require, Mapping) else {}
        return {
            "apache/avro": requirements.get("apache/avro"),
            "autoload": document.get("autoload"),
        }
    if path == "composer.lock":
        packages: list[Mapping[str, Any]] = []
        for section in ("packages", "packages-dev"):
            entries = document.get(section, [])
            if not isinstance(entries, Sequence) or isinstance(entries, str | bytes):
                raise CorpusError(f"{path}.{section} must be an array")
            packages.extend(
                package
                for package in entries
                if isinstance(package, Mapping) and package.get("name") == "apache/avro"
            )
        return {"apache/avro": packages}
    raise CorpusError(f"unsupported codec dependency definition {path}")


def _verify_new_codec_evidence(
    *,
    root: Path,
    base_files: Mapping[str, bytes],
    current_evidence: Sequence[Evidence],
    current_formats: Mapping[str, str],
    added_paths: set[str],
    php_executable: str,
    codec_runner_relative: str,
    vendor_root: Path,
) -> tuple[int, dict[str, str]]:
    new_evidence = [
        item
        for item in current_evidence
        if item.category == "codec" and item.path in added_paths
    ]
    if not new_evidence:
        raise CorpusError(
            "codec implementation changed but no newly added codec fixture "
            "can prove the defective revision"
        )
    evidence_per_path = Counter(item.path for item in new_evidence)
    compound_paths = sorted(
        path
        for path, evidence_count in evidence_per_path.items()
        if evidence_count != 1
    )
    if compound_paths:
        raise CorpusError(
            "each new codec evidence file must contain exactly one independently "
            f"verified fixture: {', '.join(compound_paths)}"
        )
    paths = sorted(evidence_per_path)
    if codec_runner_relative not in base_files:
        raise CorpusError(
            f"official PHP codec runner is missing from the base revision: "
            f"{codec_runner_relative}"
        )
    if not vendor_root.is_dir():
        raise CorpusError(
            f"PHP dependency directory is missing: {vendor_root}; "
            "install dependencies before validation"
        )

    with tempfile.TemporaryDirectory(prefix="sdk-php-codec-base-") as temporary:
        base_root = Path(temporary)
        _materialize_files(base_root, base_files)
        if not (base_root / "src").is_dir():
            raise CorpusError("the base revision has no SDK source tree for codec validation")
        trusted_codec_runner = base_root / codec_runner_relative
        consumer_contract_paths = CODEC_CONSUMER_CONTRACT_PATHS | {
            codec_runner_relative,
        }
        consumer_contract_changed = any(
            (
                (root / contract_path).read_bytes()
                if (root / contract_path).is_file()
                else None
            )
            != base_files.get(contract_path)
            for contract_path in consumer_contract_paths
        )
        used_candidate_consumer = False

        for index, path in enumerate(paths):
            fixture_format = current_formats.get(path)
            if fixture_format is None:
                raise CorpusError(f"new codec fixture {path} has no selected format")
            execution_root = base_root / ".codec-counterfactual" / str(index)
            target_vendor = execution_root / "target-vendor"
            candidate_vendor = execution_root / "candidate-vendor"
            _materialize_codec_dependencies(vendor_root, target_vendor)
            _materialize_codec_dependencies(vendor_root, candidate_vendor)
            fixture_contents = (root / path).read_bytes()
            fixture_document = _json(fixture_contents, path)
            use_candidate_consumer = (
                consumer_contract_changed and "task_transport" in fixture_document
            )
            used_candidate_consumer = used_candidate_consumer or use_candidate_consumer
            codec_runner = (
                root / codec_runner_relative
                if use_candidate_consumer
                else trusted_codec_runner
            )
            consumer_root = root if use_candidate_consumer else base_root
            target_fixture = execution_root / "target-fixture.json"
            candidate_fixture = execution_root / "candidate-fixture.json"
            target_fixture.parent.mkdir(parents=True, exist_ok=True)
            target_fixture.write_bytes(fixture_contents)
            candidate_fixture.write_bytes(fixture_contents)

            defective = _run_codec_fixture(
                root=root,
                php_executable=php_executable,
                codec_runner=codec_runner,
                vendor_root=target_vendor,
                consumer_root=consumer_root,
                source_root=base_root,
                fixture=target_fixture,
                fixture_format=fixture_format,
            )
            defective_outcome = _codec_outcome(
                defective,
                path=path,
                revision="target",
            )
            if defective_outcome == "pass":
                raise CorpusError(
                    f"new codec fixture {path} also passes on the defective base; "
                    "it does not reproduce the guarded codec change"
                )
            if defective_outcome != "assertion-failure":
                raise CorpusError(
                    f"new codec fixture {path} did not establish a deterministic "
                    f"target-revision assertion failure through the official PHP binding: "
                    f"{_process_detail(defective)}"
                )

            candidate = _run_codec_fixture(
                root=root,
                php_executable=php_executable,
                codec_runner=codec_runner,
                vendor_root=candidate_vendor,
                consumer_root=consumer_root,
                source_root=root,
                fixture=candidate_fixture,
                fixture_format=fixture_format,
            )
            candidate_outcome = _codec_outcome(
                candidate,
                path=path,
                revision="candidate",
            )
            if candidate_outcome != "pass":
                raise CorpusError(
                    f"new codec fixture {path} does not pass on the candidate "
                    f"through the official PHP binding: {_process_detail(candidate)}"
                )

            if use_candidate_consumer:
                legacy_target = _run_codec_fixture(
                    root=root,
                    php_executable=php_executable,
                    codec_runner=trusted_codec_runner,
                    vendor_root=target_vendor,
                    consumer_root=base_root,
                    source_root=base_root,
                    fixture=target_fixture,
                    fixture_format=fixture_format,
                )
                legacy_candidate = _run_codec_fixture(
                    root=root,
                    php_executable=php_executable,
                    codec_runner=trusted_codec_runner,
                    vendor_root=candidate_vendor,
                    consumer_root=base_root,
                    source_root=root,
                    fixture=candidate_fixture,
                    fixture_format=fixture_format,
                )
                legacy_target_outcome = _codec_outcome(
                    legacy_target,
                    path=path,
                    revision="legacy target",
                )
                legacy_candidate_outcome = _codec_outcome(
                    legacy_candidate,
                    path=path,
                    revision="legacy candidate",
                )
                if legacy_target_outcome != "pass" or legacy_candidate_outcome != "pass":
                    raise CorpusError(
                        "a task-transport codec consumer extension must preserve the "
                        f"legacy payload fixture verdict at both source revisions: {path}"
                    )
                witness_runner = base_root / CODEC_TASK_TRANSPORT_WITNESS_RUNNER
                witness_fixture = base_root / CODEC_TASK_TRANSPORT_WITNESS_FIXTURE
                if not witness_runner.is_file() or not witness_fixture.is_file():
                    raise CorpusError(
                        "the base revision has no trusted worker transport witness"
                    )
                witness_target = _run_replay_fixture(
                    root=root,
                    php_executable=php_executable,
                    replay_runner=witness_runner,
                    vendor_root=vendor_root,
                    source_root=base_root,
                    fixture=witness_fixture,
                )
                witness_candidate = _run_replay_fixture(
                    root=root,
                    php_executable=php_executable,
                    replay_runner=witness_runner,
                    vendor_root=vendor_root,
                    source_root=root,
                    fixture=witness_fixture,
                )
                if witness_target.returncode != 0 or witness_candidate.returncode == 0:
                    raise CorpusError(
                        "task-transport codec evidence must make the trusted base worker "
                        f"consumer cross the rejection boundary: {path}"
                    )

    return len(new_evidence), {
        "target": "assertion-failure",
        "candidate": "pass",
        "consumer": "codec-worker" if used_candidate_consumer else "codec",
    }


def _verify_new_replay_evidence(
    *,
    root: Path,
    base_files: Mapping[str, bytes],
    current_evidence: Sequence[Evidence],
    added_paths: set[str],
    php_executable: str,
    replay_runner: Path,
    vendor_root: Path,
) -> tuple[int, dict[str, str]]:
    paths = sorted(
        {
            item.path
            for item in current_evidence
            if item.category == "replay" and item.path in added_paths
        }
    )
    if not paths:
        raise CorpusError(
            "replay implementation changed but no newly added replay fixture "
            "can prove the defective revision"
        )
    try:
        runner_path = replay_runner.relative_to(root).as_posix()
    except ValueError as error:
        raise CorpusError("official PHP replay runner must be inside the repository") from error
    base_runner = base_files.get(runner_path)
    if base_runner is None:
        raise CorpusError("the base revision has no official PHP replay runner")
    if not replay_runner.is_file():
        raise CorpusError("the candidate revision has no official PHP replay runner")
    runner_changed = replay_runner.read_bytes() != base_runner
    if not vendor_root.is_dir():
        raise CorpusError(
            f"PHP dependency directory is missing: {vendor_root}; install dependencies before validation"
        )

    with tempfile.TemporaryDirectory(prefix="sdk-php-replay-base-") as temporary:
        base_root = Path(temporary)
        source_files = {
            path: content
            for path, content in base_files.items()
            if Path(path).parts and Path(path).parts[0] == "src"
        }
        if not source_files:
            raise CorpusError("the base revision has no SDK source tree to replay")
        for path, content in source_files.items():
            destination = base_root / path
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(content)
        trusted_runner = base_root / runner_path
        trusted_runner.parent.mkdir(parents=True, exist_ok=True)
        trusted_runner.write_bytes(base_runner)

        for path in paths:
            fixture = root / path
            candidate = _run_replay_fixture(
                root=root,
                php_executable=php_executable,
                replay_runner=replay_runner if runner_changed else trusted_runner,
                vendor_root=vendor_root,
                source_root=root,
                fixture=fixture,
            )
            if candidate.returncode != 0:
                raise CorpusError(
                    f"new replay fixture {path} does not pass on the candidate "
                    f"through the official PHP binding: {_process_detail(candidate)}"
                )

            defective = _run_replay_fixture(
                root=root,
                php_executable=php_executable,
                replay_runner=replay_runner if runner_changed else trusted_runner,
                vendor_root=vendor_root,
                source_root=base_root,
                fixture=fixture,
            )
            if defective.returncode == 0:
                raise CorpusError(
                    f"new replay fixture {path} also passes on the defective base; "
                    "it does not reproduce the guarded replay change"
                )

            if runner_changed:
                legacy = _run_replay_fixture(
                    root=root,
                    php_executable=php_executable,
                    replay_runner=trusted_runner,
                    vendor_root=vendor_root,
                    source_root=base_root,
                    fixture=fixture,
                )
                cross_version = _run_replay_fixture(
                    root=root,
                    php_executable=php_executable,
                    replay_runner=trusted_runner,
                    vendor_root=vendor_root,
                    source_root=root,
                    fixture=fixture,
                )
                if legacy.returncode != 0 or cross_version.returncode == 0:
                    raise CorpusError(
                        "the official PHP replay runner must remain unchanged unless a fixture "
                        "passes with both matching runner/source revisions and fails across the "
                        f"runner/source contract boundary: {path}"
                    )

    return len(paths), {
        "base": "fail",
        "candidate": "pass",
        "consumer": "worker",
    }


def _policy(document: Mapping[str, Any], path: str) -> Mapping[str, Any]:
    _string(document.get("$schema"), f"{path}.$schema")
    if document.get("schema") != POLICY_SCHEMA:
        raise CorpusError(f"{path} must declare schema={POLICY_SCHEMA}")
    _string(document.get("repository"), f"{path}.repository")
    binding = document.get("binding")
    if binding is not None and binding not in SUPPORTED_BINDINGS:
        raise CorpusError(f"{path}.binding is unsupported")
    categories = _object(document.get("categories"), f"{path}.categories")
    if not categories or not set(categories) <= SUPPORTED_CATEGORIES:
        raise CorpusError(f"{path}.categories must contain only replay and/or codec")
    for name, raw_category in categories.items():
        category = _object(raw_category, f"{path}.categories.{name}")
        fixtures = _list(category.get("fixtures"), f"{path}.categories.{name}.fixtures", nonempty=True)
        for index, raw_fixture in enumerate(fixtures):
            fixture = _object(raw_fixture, f"{path}.categories.{name}.fixtures[{index}]")
            fixture_glob = _string(
                fixture.get("glob"),
                f"{path}.categories.{name}.fixtures[{index}].glob",
            )
            fixture_format = _string(
                fixture.get("format"),
                f"{path}.categories.{name}.fixtures[{index}].format",
            )
            if fixture_format not in SUPPORTED_FORMATS:
                raise CorpusError(f"{path}.categories.{name}.fixtures[{index}].format is unsupported")
            if not fixture_format.startswith(name) and not (
                name == "codec" and fixture_format == "avro-value-golden-v1"
            ) and not (name == "replay" and fixture_format == "golden-history-v1"):
                raise CorpusError(f"{path}.categories.{name} contains a fixture for another category")
            if binding == "php":
                if fixture_format not in PHP_FIXTURE_FORMATS[name]:
                    raise CorpusError(
                        f"{path}.categories.{name}.fixtures[{index}].format "
                        "has no official PHP consumer"
                    )
                if PORTABLE_PHP_FIXTURE_GLOB.fullmatch(fixture_glob) is None:
                    raise CorpusError(
                        f"{path}.categories.{name}.fixtures[{index}].glob "
                        "is not portable to the official PHP consumer"
                    )
        guards = _list(category.get("guards"), f"{path}.categories.{name}.guards", nonempty=True)
        for index, raw_guard in enumerate(guards):
            guard = _object(raw_guard, f"{path}.categories.{name}.guards[{index}]")
            _string(guard.get("glob"), f"{path}.categories.{name}.guards[{index}].glob")
            patterns = guard.get("content_patterns")
            if patterns is not None:
                for pattern in _unique_strings(
                    patterns,
                    f"{path}.categories.{name}.guards[{index}].content_patterns",
                ):
                    try:
                        re.compile(pattern)
                    except re.error as error:
                        raise CorpusError(f"invalid guard regex {pattern!r}: {error}") from error
    return document


def _require_policy_extension(
    base_policy: Mapping[str, Any],
    current_policy: Mapping[str, Any],
    path: str,
) -> None:
    for field in ("repository", "binding"):
        if current_policy.get(field) != base_policy.get(field):
            raise CorpusError(f"{path}.{field} cannot change from the base policy")

    base_categories = _object(base_policy["categories"], "base categories")
    current_categories = _object(current_policy["categories"], "current categories")
    for category_name, raw_base_category in base_categories.items():
        if category_name not in current_categories:
            raise CorpusError(f"{path}.categories.{category_name} cannot be removed from the base policy")
        base_category = _object(raw_base_category, f"base categories.{category_name}")
        current_category = _object(
            current_categories[category_name],
            f"current categories.{category_name}",
        )
        for selector_type in ("fixtures", "guards"):
            base_selectors = _list(
                base_category[selector_type],
                f"base categories.{category_name}.{selector_type}",
            )
            current_selectors = _list(
                current_category[selector_type],
                f"current categories.{category_name}.{selector_type}",
            )
            for base_selector in base_selectors:
                if base_selector not in current_selectors:
                    raise CorpusError(
                        f"{path}.categories.{category_name}.{selector_type} cannot remove "
                        "or change a base selector"
                    )


def _tracked_worktree_files(root: Path) -> dict[str, bytes]:
    paths = _run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        root,
    ).split("\0")
    return {
        path: (root / path).read_bytes()
        for path in paths
        if path and (root / path).is_file()
    }


def _ref_files(root: Path, ref: str) -> dict[str, bytes]:
    paths = _run(["git", "ls-tree", "-r", "--name-only", "-z", ref], root).split("\0")
    return {
        path: _run_bytes(["git", "show", f"{ref}:{path}"], root)
        for path in paths
        if path
    }


def _matches(path: str, pattern: str) -> bool:
    path_segments = path.split("/")
    pattern_segments = pattern.split("/")
    return len(path_segments) == len(pattern_segments) and all(
        (not path_segment.startswith(".") or pattern_segment.startswith("."))
        and fnmatch.fnmatchcase(path_segment, pattern_segment)
        for path_segment, pattern_segment in zip(path_segments, pattern_segments)
    )


def _inventory(
    policy: Mapping[str, Any],
    files: Mapping[str, bytes],
    *,
    new_paths: set[str] | None = None,
    payload_projector: PhpReplayPayloadProjector | None = None,
    codec_value_projector: PhpCodecValueProjector | None = None,
) -> list[Evidence]:
    binding = policy.get("binding")
    evidence: list[Evidence] = []
    selected_paths: set[str] = set()
    for category_name, raw_category in _object(policy["categories"], "categories").items():
        category = _object(raw_category, f"categories.{category_name}")
        for raw_fixture in _list(category["fixtures"], f"categories.{category_name}.fixtures"):
            fixture = _object(raw_fixture, f"categories.{category_name}.fixtures[]")
            pattern = _string(fixture["glob"], "fixture.glob")
            fixture_format = _string(fixture["format"], "fixture.format")
            for path in sorted(candidate for candidate in files if _matches(candidate, pattern)):
                if path in selected_paths:
                    raise CorpusError(f"fixture path {path} is selected more than once")
                selected_paths.add(path)
                document = _json(files[path], path)
                if fixture_format == "codec-regression-v1":
                    parsed = _codec_fixture(
                        document,
                        path,
                        binding if isinstance(binding, str) else None,
                        codec_value_projector,
                    )
                elif fixture_format == "replay-regression-v1":
                    parsed = _replay_fixture(
                        document,
                        path,
                        binding if isinstance(binding, str) else None,
                        payload_projector,
                    )
                elif fixture_format == "avro-value-golden-v1":
                    parsed = _avro_golden_fixture(document, path)
                else:
                    parsed = _golden_history_fixture(
                        document,
                        path,
                        require_single_case=new_paths is not None and path in new_paths,
                        payload_projector=payload_projector,
                    )
                if any(item.category != category_name for item in parsed):
                    raise CorpusError(f"{path} produced evidence for the wrong category")
                evidence.extend(parsed)

    identities = Counter(item.identity for item in evidence)
    repeated_identities = sorted(identity for identity, count in identities.items() if count > 1)
    if repeated_identities:
        raise CorpusError(f"duplicate fixture identities: {repeated_identities}")
    semantics = Counter(
        (item.category, digest)
        for item in evidence
        for digest in item.duplicate_digests
    )
    duplicate_semantics = sorted(key for key, count in semantics.items() if count > 1)
    if duplicate_semantics:
        paths = {
            key: sorted(
                item.path
                for item in evidence
                if item.category == key[0] and key[1] in item.duplicate_digests
            )
            for key in duplicate_semantics
        }
        raise CorpusError(f"duplicate semantic fixtures: {paths}")
    return evidence


def _fixture_paths(policy: Mapping[str, Any], files: Mapping[str, bytes]) -> set[str]:
    return {
        path
        for raw_category in _object(policy["categories"], "categories").values()
        for raw_fixture in _list(
            _object(raw_category, "category")["fixtures"],
            "category.fixtures",
        )
        for path in files
        if _matches(path, _string(_object(raw_fixture, "fixture")["glob"], "fixture.glob"))
    }


def _fixture_formats(
    policy: Mapping[str, Any],
    files: Mapping[str, bytes],
) -> dict[str, str]:
    formats: dict[str, str] = {}
    for raw_category in _object(policy["categories"], "categories").values():
        for raw_fixture in _list(
            _object(raw_category, "category")["fixtures"],
            "category.fixtures",
        ):
            fixture = _object(raw_fixture, "fixture")
            pattern = _string(fixture["glob"], "fixture.glob")
            fixture_format = _string(fixture["format"], "fixture.format")
            for path in files:
                if _matches(path, pattern):
                    if path in formats:
                        raise CorpusError(f"fixture path {path} is selected more than once")
                    formats[path] = fixture_format
    return formats


def _changed_paths(root: Path, base_ref: str) -> tuple[set[str], set[str]]:
    output = _run(["git", "diff", "--name-status", "--find-renames", base_ref, "--"], root)
    changed: set[str] = set()
    added: set[str] = set()
    for line in output.splitlines():
        parts = line.split("\t")
        status = parts[0]
        paths = parts[1:]
        if not paths:
            continue
        changed.update(paths)
        if status.startswith("A"):
            added.add(paths[-1])
    untracked = {
        path
        for path in _run(
            ["git", "ls-files", "--others", "--exclude-standard"],
            root,
        ).splitlines()
        if path
    }
    return changed | untracked, added | untracked


def _php_named_functions(content: str) -> dict[str, tuple[str, str]]:
    matches = list(PHP_NAMED_FUNCTION.finditer(content))
    functions: dict[str, tuple[str, str]] = {}
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(content)
        chunk = content[match.start():end]
        closing_brace = chunk.rfind("}")
        if closing_brace >= 0:
            chunk = chunk[: closing_brace + 1]
        opening_brace = chunk.find("{")
        header = chunk[:opening_brace] if opening_brace >= 0 else match.group(0)
        functions[match.group(1)] = (header, chunk)
    return functions


def _changed_php_function_context(
    root: Path,
    base_ref: str,
    matching: Sequence[str],
) -> str:
    context: list[str] = []
    for path in matching:
        if not path.endswith(".php"):
            continue
        base_content = _run(["git", "show", f"{base_ref}:{path}"], root, check=False)
        current_path = root / path
        current_content = (
            current_path.read_text(encoding="utf-8", errors="replace")
            if current_path.is_file()
            else ""
        )
        base_functions = _php_named_functions(base_content)
        current_functions = _php_named_functions(current_content)
        for name in sorted(base_functions.keys() | current_functions.keys()):
            base_header, base_chunk = base_functions.get(name, ("", ""))
            current_header, current_chunk = current_functions.get(name, ("", ""))
            base_semantics = "\n".join(
                line
                for line in base_chunk.splitlines()
                if not PHP_REPLAY_NEUTRAL_OBSERVABILITY_STATEMENT.fullmatch(line)
            )
            current_semantics = "\n".join(
                line
                for line in current_chunk.splitlines()
                if not PHP_REPLAY_NEUTRAL_OBSERVABILITY_STATEMENT.fullmatch(line)
            )
            if base_semantics != current_semantics:
                context.extend((base_header, current_header))
    return "\n".join(context)


def _guard_matches(
    root: Path,
    base_ref: str,
    changed: set[str],
    raw_guard: Any,
) -> bool:
    guard = _object(raw_guard, "guard")
    matching = sorted(path for path in changed if _matches(path, _string(guard["glob"], "guard.glob")))
    if not matching:
        return False
    patterns = guard.get("content_patterns")
    if patterns is None:
        return True
    diff = _run(["git", "diff", "--unified=0", base_ref, "--", *matching], root)
    untracked = set(
        _run(["git", "ls-files", "--others", "--exclude-standard"], root).splitlines()
    )
    for path in matching:
        if path in untracked and (root / path).is_file():
            diff += "\n" + (root / path).read_text(encoding="utf-8", errors="replace")
    changed_content = "\n".join(
        line[1:]
        for line in diff.splitlines()
        if line.startswith(("+", "-")) and not line.startswith(("+++", "---"))
    )
    changed_content += "\n" + _changed_php_function_context(root, base_ref, matching)
    return any(re.search(pattern, changed_content) for pattern in patterns)


def validate(
    root: Path,
    policy_path: Path,
    base_ref: str | None,
    *,
    php_executable: str,
    codec_runner_path: Path,
    replay_runner_path: Path,
    vendor_root_path: Path,
    codec_value_projector_executable: str,
    codec_value_projector_path: Path,
    payload_projector_executable: str,
    payload_projector_path: Path,
    payload_consumer_root_path: Path,
    payload_consumer_vendor_root_path: Path,
) -> dict[str, Any]:
    policy_file = (policy_path if policy_path.is_absolute() else root / policy_path).resolve()
    replay_runner = (
        replay_runner_path
        if replay_runner_path.is_absolute()
        else root / replay_runner_path
    ).resolve()
    codec_runner = (
        codec_runner_path
        if codec_runner_path.is_absolute()
        else root / codec_runner_path
    ).resolve()
    vendor_root = (
        vendor_root_path
        if vendor_root_path.is_absolute()
        else root / vendor_root_path
    ).resolve()
    payload_projector_runner = (
        payload_projector_path
        if payload_projector_path.is_absolute()
        else root / payload_projector_path
    ).resolve()
    payload_consumer_root = (
        payload_consumer_root_path
        if payload_consumer_root_path.is_absolute()
        else root / payload_consumer_root_path
    ).resolve()
    payload_consumer_vendor_root = (
        payload_consumer_vendor_root_path
        if payload_consumer_vendor_root_path.is_absolute()
        else root / payload_consumer_vendor_root_path
    ).resolve()
    codec_value_projector_runner = (
        codec_value_projector_path
        if codec_value_projector_path.is_absolute()
        else payload_consumer_root / codec_value_projector_path
    ).resolve()
    try:
        policy_relative_path = policy_file.relative_to(root).as_posix()
    except ValueError as error:
        raise CorpusError("policy must be inside the repository root") from error
    try:
        codec_runner_relative = codec_runner.relative_to(root).as_posix()
    except ValueError as error:
        raise CorpusError("codec runner must be inside the repository root") from error
    policy = _policy(_json(policy_file.read_bytes(), str(policy_path)), str(policy_path))
    payload_projector = (
        PhpReplayPayloadProjector(
            executable=payload_projector_executable,
            runner=payload_projector_runner,
            vendor_root=payload_consumer_vendor_root,
            source_root=payload_consumer_root,
        )
        if policy.get("binding") == "php"
        else None
    )
    codec_value_projector = (
        PhpCodecValueProjector(
            executable=codec_value_projector_executable,
            runner=codec_value_projector_runner,
            vendor_root=payload_consumer_vendor_root,
            consumer_root=payload_consumer_root,
            source_root=payload_consumer_root,
        )
        if policy.get("binding") == "php"
        else None
    )
    current_files = _tracked_worktree_files(root)
    current_formats = _fixture_formats(policy, current_files)
    changed: set[str] = set()
    added_paths: set[str] = set()
    base_files: dict[str, bytes] = {}
    base_evidence: list[Evidence] = []
    if base_ref and not ZERO_COMMIT.fullmatch(base_ref):
        _run(["git", "rev-parse", "--verify", f"{base_ref}^{{commit}}"], root)
        changed, added_paths = _changed_paths(root, base_ref)
        base_files = _ref_files(root, base_ref)
        raw_base_policy = base_files.get(policy_relative_path)
        base_policy = (
            _policy(_json(raw_base_policy, policy_relative_path), policy_relative_path)
            if raw_base_policy is not None
            else policy
        )
        if raw_base_policy is not None:
            _require_policy_extension(base_policy, policy, str(policy_path))
        # Apply current coverage to both revisions so selector expansion cannot
        # manufacture growth from pre-existing fixture evidence.
        for path in _fixture_paths(policy, base_files):
            current_content = current_files.get(path)
            if current_content != base_files[path] and current_content is not None:
                if _avro_golden_migration(base_files[path], current_content):
                    base_files[path] = current_content
                    continue
            if current_content != base_files[path]:
                raise CorpusError(f"immutable fixture file {path} was changed, moved, or removed")
        base_evidence = _inventory(
            policy,
            base_files,
            payload_projector=payload_projector,
            codec_value_projector=codec_value_projector,
        )
    current_evidence = _inventory(
        policy,
        current_files,
        new_paths=added_paths,
        payload_projector=payload_projector,
        codec_value_projector=codec_value_projector,
    )

    current_by_id = {item.identity: item for item in current_evidence}
    base_by_id = {item.identity: item for item in base_evidence}
    for identity, previous in base_by_id.items():
        current = current_by_id.get(identity)
        if current is None:
            raise CorpusError(f"immutable fixture {identity} was removed")
        if current.path != previous.path or current.semantic_digest != previous.semantic_digest:
            raise CorpusError(f"immutable fixture {identity} was changed; append a superseding fixture instead")
    for item in current_evidence:
        for superseded in item.supersedes:
            previous = current_by_id.get(superseded)
            if previous is None:
                raise CorpusError(f"{item.identity} supersedes unknown fixture {superseded}")
            if previous.category != item.category or previous.protocol_version == item.protocol_version:
                raise CorpusError(
                    f"{item.identity} must supersede evidence in the same category at an older protocol version"
                )

    counts: dict[str, dict[str, Any]] = {}
    for category_name, raw_category in _object(policy["categories"], "categories").items():
        current_count = sum(item.category == category_name for item in current_evidence)
        base_count = sum(item.category == category_name for item in base_evidence)
        related = False
        if base_ref and not ZERO_COMMIT.fullmatch(base_ref):
            category = _object(raw_category, f"categories.{category_name}")
            related = any(
                _guard_matches(root, base_ref, changed, guard)
                for guard in _list(category["guards"], f"categories.{category_name}.guards")
            )
            if related and current_count <= base_count:
                raise CorpusError(
                    f"{category_name} implementation changed but its corpus did not grow "
                    f"(base={base_count}, current={current_count})"
                )
        revision_verified = 0
        counterfactual: dict[str, str] | None = None
        if category_name == "codec" and related:
            _require_codec_dependencies_unchanged(
                base_files=base_files,
                current_files=current_files,
                changed=changed,
            )
            revision_verified, counterfactual = _verify_new_codec_evidence(
                root=root,
                base_files=base_files,
                current_evidence=current_evidence,
                current_formats=current_formats,
                added_paths=added_paths,
                php_executable=php_executable,
                codec_runner_relative=codec_runner_relative,
                vendor_root=vendor_root,
            )
        elif category_name == "replay" and related:
            revision_verified, counterfactual = _verify_new_replay_evidence(
                root=root,
                base_files=base_files,
                current_evidence=current_evidence,
                added_paths=added_paths,
                php_executable=php_executable,
                replay_runner=replay_runner,
                vendor_root=vendor_root,
            )
        count = {
            "base": base_count,
            "current": current_count,
            "related_change": related,
        }
        if category_name in {"codec", "replay"}:
            count["revision_verified"] = revision_verified
        if category_name == "codec":
            count["candidate_passed"] = revision_verified
            count["target_failed"] = revision_verified
            if counterfactual is not None:
                count["counterfactual"] = counterfactual
        elif category_name == "replay":
            if counterfactual is not None:
                count["counterfactual"] = counterfactual
        counts[category_name] = count
    return {
        "schema": POLICY_SCHEMA,
        "repository": policy["repository"],
        "base_ref": base_ref,
        "changed_paths": len(changed),
        "counts": counts,
        "status": "pass",
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--policy", type=Path, default=Path("regression-corpus-policy.json"))
    parser.add_argument("--base-ref")
    parser.add_argument("--php-executable", default="php")
    parser.add_argument(
        "--codec-runner",
        type=Path,
        default=Path("scripts/ci/run-codec-regression-fixture.php"),
    )
    parser.add_argument(
        "--replay-runner",
        type=Path,
        default=Path("scripts/ci/run-replay-regression-fixture.php"),
    )
    parser.add_argument("--vendor-root", type=Path, default=Path("vendor"))
    parser.add_argument("--codec-value-projector-executable")
    parser.add_argument(
        "--codec-value-projector",
        type=Path,
        default=Path("scripts/ci/project-codec-regression-value.php"),
    )
    parser.add_argument("--payload-projector-executable")
    parser.add_argument(
        "--payload-projector",
        type=Path,
        default=Path("scripts/ci/project-replay-payload.php"),
    )
    parser.add_argument("--payload-consumer-root", type=Path, default=Path("."))
    parser.add_argument(
        "--payload-consumer-vendor-root",
        type=Path,
        default=Path("vendor"),
    )
    args = parser.parse_args(argv)
    try:
        result = validate(
            args.root.resolve(),
            args.policy,
            args.base_ref,
            php_executable=args.php_executable,
            codec_runner_path=args.codec_runner,
            replay_runner_path=args.replay_runner,
            vendor_root_path=args.vendor_root,
            codec_value_projector_executable=(
                args.codec_value_projector_executable
                or args.payload_projector_executable
                or args.php_executable
            ),
            codec_value_projector_path=args.codec_value_projector,
            payload_projector_executable=(
                args.payload_projector_executable or args.php_executable
            ),
            payload_projector_path=args.payload_projector,
            payload_consumer_root_path=args.payload_consumer_root,
            payload_consumer_vendor_root_path=args.payload_consumer_vendor_root,
        )
    except (CorpusError, OSError) as error:
        print(f"regression corpus validation failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
