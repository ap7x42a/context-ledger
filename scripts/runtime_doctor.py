#!/usr/bin/env python3
"""Static/runtime diagnostics for context-ledger hook installation."""

from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from ledger_core import (  # noqa: E402
    ValidationError,
    hook_observations,
    init_ledger,
    validate_ledger,
)


def _load_contracts() -> Dict[str, Any]:
    path = SKILL_ROOT / "references" / "runtime-contracts.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValidationError(f"cannot read runtime contract manifest {path}: {exc}") from exc
    if not isinstance(value, dict) or not isinstance(value.get("runtimes"), dict):
        raise ValidationError("runtime contract manifest has an invalid shape")
    return value


def _check(name: str, status: str, detail: str, **extra: Any) -> Dict[str, Any]:
    result = {"name": name, "status": status, "detail": detail}
    result.update(extra)
    return result


def _load_document(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ValidationError(f"hook configuration is missing: {path}")
    except (OSError, json.JSONDecodeError) as exc:
        raise ValidationError(f"cannot parse hook configuration {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValidationError(f"hook configuration must be a JSON object: {path}")
    return value


def _marked_handlers(document: Mapping[str, Any], event_name: str, marker: str) -> List[Dict[str, Any]]:
    hooks = document.get("hooks")
    if not isinstance(hooks, Mapping):
        return []
    groups = hooks.get(event_name)
    if not isinstance(groups, list):
        return []
    result: List[Dict[str, Any]] = []
    for group in groups:
        if not isinstance(group, Mapping):
            continue
        handlers = group.get("hooks")
        if not isinstance(handlers, list):
            continue
        for handler in handlers:
            if not isinstance(handler, Mapping):
                continue
            command = str(handler.get("command") or "")
            command_windows = str(handler.get("commandWindows") or "")
            if marker in command or marker in command_windows:
                result.append({"group": dict(group), "handler": dict(handler)})
    return result


def _probe_binary(runtime: str) -> Dict[str, Any]:
    executable = "claude" if runtime == "claude-code" else "codex"
    resolved = shutil.which(executable)
    if not resolved:
        return _check(f"{runtime} binary", "warn", f"{executable} is not on PATH; static configuration was still checked")
    try:
        completed = subprocess.run(
            [resolved, "--version"],
            text=True,
            capture_output=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return _check(f"{runtime} binary", "warn", f"version probe failed: {exc}")
    output = (completed.stdout or completed.stderr).strip().splitlines()
    detail = output[0] if output else f"exit {completed.returncode} with no version text"
    return _check(
        f"{runtime} binary",
        "pass" if completed.returncode == 0 else "warn",
        f"{resolved}: {detail}",
    )


def run_doctor(root: Path | str, *, runtime: str = "both", probe_binary: bool = False) -> Dict[str, Any]:
    root_path = Path(root).expanduser().resolve()
    init_ledger(root_path)
    contracts = _load_contracts()
    marker = str(contracts["marker"])
    runtimes = ["claude-code", "codex"] if runtime == "both" else [runtime]
    checks: List[Dict[str, Any]] = []

    py_ok = sys.version_info >= (3, 10)
    checks.append(
        _check(
            "Python runtime",
            "pass" if py_ok else "fail",
            f"{sys.version.split()[0]} (requires 3.10+)",
        )
    )
    sqlite_ok = sqlite3.sqlite_version_info >= (3, 24, 0)
    checks.append(
        _check(
            "SQLite runtime",
            "pass" if sqlite_ok else "fail",
            f"{sqlite3.sqlite_version} (requires 3.24+)",
        )
    )
    quick = validate_ledger(root_path, full=False)
    checks.append(
        _check(
            "ledger database",
            "pass" if quick["ok"] else "fail",
            "SQLite quick-check and state shape passed" if quick["ok"] else "; ".join(quick["errors"]),
        )
    )

    observations = hook_observations(root_path)
    observed_by_runtime: Dict[str, List[Dict[str, Any]]] = {}
    for item in observations:
        observed_by_runtime.setdefault(str(item["runtime"]), []).append(item)

    for runtime_name in runtimes:
        contract = contracts["runtimes"][runtime_name]
        config_path = root_path / str(contract["config_path"])
        try:
            document = _load_document(config_path)
        except ValidationError as exc:
            checks.append(_check(f"{runtime_name} hook configuration", "fail", str(exc)))
            if probe_binary:
                checks.append(_probe_binary(runtime_name))
            continue
        stale_text = json.dumps(document, ensure_ascii=False)
        if "--context-ledger-hook-v1" in stale_text:
            checks.append(_check(f"{runtime_name} stale marker", "fail", "v1 hook marker remains installed"))
        else:
            checks.append(_check(f"{runtime_name} stale marker", "pass", "no v1 marker found"))

        for event_name, event_contract in contract["events"].items():
            handlers = _marked_handlers(document, event_name, marker)
            if len(handlers) != 1:
                checks.append(
                    _check(
                        f"{runtime_name} {event_name}",
                        "fail",
                        f"expected exactly one {marker} handler; found {len(handlers)}",
                    )
                )
                continue
            group = handlers[0]["group"]
            handler = handlers[0]["handler"]
            expected_matcher = event_contract.get("matcher")
            actual_matcher = group.get("matcher")
            matcher_ok = expected_matcher == actual_matcher or (expected_matcher is None and actual_matcher in {None, ""})
            command = str(handler.get("command") or "")
            path_ok = "runtime_hook.py" in command and f"--runtime {runtime_name}" in command and marker in command
            timeout_ok = isinstance(handler.get("timeout"), int) and int(handler["timeout"]) > 0
            status = "pass" if matcher_ok and path_ok and timeout_ok else "fail"
            detail_parts = []
            if not matcher_ok:
                detail_parts.append(f"matcher {actual_matcher!r}, expected {expected_matcher!r}")
            if not path_ok:
                detail_parts.append("command is missing the v2 adapter/runtime marker")
            if not timeout_ok:
                detail_parts.append("timeout is missing or invalid")
            checks.append(
                _check(
                    f"{runtime_name} {event_name}",
                    status,
                    "installed handler matches the checked contract" if status == "pass" else "; ".join(detail_parts),
                )
            )

        known_events = set(contract["events"])
        for observation in observed_by_runtime.get(runtime_name, []):
            event_name = str(observation["event_name"])
            fields = set(str(x) for x in observation["field_names"])
            if event_name not in known_events:
                checks.append(
                    _check(
                        f"{runtime_name} observed event {event_name}",
                        "warn",
                        "event is not in the package's checked contract manifest",
                    )
                )
                continue
            required = set(str(x) for x in contract["events"][event_name]["required_fields"])
            missing = sorted(required - fields)
            checks.append(
                _check(
                    f"{runtime_name} observed shape {event_name}",
                    "warn" if missing else "pass",
                    f"missing observed fields: {', '.join(missing)}" if missing else "observed fields include the checked required set",
                )
            )
        if probe_binary:
            checks.append(_probe_binary(runtime_name))

    ok = not any(check["status"] == "fail" for check in checks)
    return {
        "ok": ok,
        "checked_at": contracts["checked_at"],
        "marker": marker,
        "root": str(root_path),
        "checks": checks,
    }
