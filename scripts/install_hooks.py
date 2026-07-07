#!/usr/bin/env python3
"""Install or remove context-ledger lifecycle hooks without clobbering peers."""

from __future__ import annotations

import argparse
import copy
import difflib
import json
import os
import shlex
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_ROOT = SCRIPT_DIR.parent
HOOK_SCRIPT = SKILL_ROOT / "hooks" / "runtime_hook.py"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from ledger_core import LedgerError, ValidationError, atomic_write_text, init_ledger  # noqa: E402

MARKER = "--context-ledger-hook-v2"
LEGACY_MARKERS = {"--context-ledger-hook-v1", MARKER}

RUNTIME_EVENTS = {
    "claude-code": [
        ("SessionStart", "startup|resume|clear|compact", 10),
        ("UserPromptSubmit", None, 10),
        ("PostToolUse", None, 10),
        ("PostToolUseFailure", None, 10),
        ("Stop", None, 15),
        ("PreCompact", "manual|auto", 30),
        ("PostCompact", "manual|auto", 15),
    ],
    "codex": [
        ("SessionStart", "startup|resume|clear|compact", 10),
        ("UserPromptSubmit", None, 10),
        ("PostToolUse", None, 10),
        ("Stop", None, 15),
        ("PreCompact", "manual|auto", 30),
        ("PostCompact", "manual|auto", 15),
    ],
}


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _load_json_object(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValidationError(f"cannot parse existing hook config {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValidationError(f"existing hook config must be a JSON object: {path}")
    return value


def _command(python: Path, runtime: str, project: Path, *, windows: bool = False) -> str:
    args = [
        str(python),
        str(HOOK_SCRIPT),
        "--runtime",
        runtime,
        "--project-root",
        str(project),
        MARKER,
    ]
    return subprocess.list2cmdline(args) if windows else shlex.join(args)


def _handler(python: Path, runtime: str, project: Path, timeout: int) -> Dict[str, Any]:
    if runtime == "codex":
        return {
            "type": "command",
            "command": _command(python, runtime, project),
            "commandWindows": _command(python, runtime, project, windows=True),
            "timeout": timeout,
        }
    handler: Dict[str, Any] = {
        "type": "command",
        "command": _command(python, runtime, project, windows=os.name == "nt"),
        "timeout": timeout,
    }
    if os.name == "nt":
        handler["shell"] = "powershell"
    return handler


def _remove_marker_hooks(document: MutableMapping[str, Any]) -> int:
    hooks = document.get("hooks")
    if not isinstance(hooks, MutableMapping):
        return 0
    removed = 0
    for event_name in list(hooks.keys()):
        groups = hooks.get(event_name)
        if not isinstance(groups, list):
            continue
        next_groups: List[Any] = []
        for group in groups:
            if not isinstance(group, MutableMapping):
                next_groups.append(group)
                continue
            handlers = group.get("hooks")
            if not isinstance(handlers, list):
                next_groups.append(group)
                continue
            next_handlers = []
            for handler in handlers:
                command = handler.get("command") if isinstance(handler, Mapping) else None
                command_windows = handler.get("commandWindows") if isinstance(handler, Mapping) else None
                if (isinstance(command, str) and any(marker in command for marker in LEGACY_MARKERS)) or (
                    isinstance(command_windows, str) and any(marker in command_windows for marker in LEGACY_MARKERS)
                ):
                    removed += 1
                else:
                    next_handlers.append(handler)
            if next_handlers:
                updated = copy.deepcopy(dict(group))
                updated["hooks"] = next_handlers
                next_groups.append(updated)
        if next_groups:
            hooks[event_name] = next_groups
        else:
            hooks.pop(event_name, None)
    if not hooks:
        document.pop("hooks", None)
    return removed


def _install_runtime(
    document: MutableMapping[str, Any], runtime: str, project: Path, python: Path
) -> int:
    _remove_marker_hooks(document)
    hooks = document.setdefault("hooks", {})
    if not isinstance(hooks, MutableMapping):
        raise ValidationError("top-level hooks field exists but is not an object")
    count = 0
    for event_name, matcher, timeout in RUNTIME_EVENTS[runtime]:
        groups = hooks.setdefault(event_name, [])
        if not isinstance(groups, list):
            raise ValidationError(f"hooks.{event_name} exists but is not an array")
        group: Dict[str, Any] = {"hooks": [_handler(python, runtime, project, timeout)]}
        if matcher is not None:
            group["matcher"] = matcher
        groups.append(group)
        count += 1
    return count


def _target_path(project: Path, runtime: str) -> Path:
    if runtime == "claude-code":
        return project / ".claude" / "settings.json"
    return project / ".codex" / "hooks.json"


def _render(document: Mapping[str, Any]) -> str:
    return json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _write_with_backup(path: Path, old_text: str, new_text: str) -> Optional[Path]:
    if old_text == new_text:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    backup: Optional[Path] = None
    if path.exists():
        backup = path.with_name(path.name + f".context-ledger.{_timestamp()}.bak")
        shutil.copy2(path, backup)
        try:
            backup.chmod(0o600)
        except OSError:
            pass
    atomic_write_text(path, new_text)
    return backup


def _process(
    project: Path,
    runtime: str,
    python: Path,
    *,
    uninstall: bool,
    dry_run: bool,
) -> Dict[str, Any]:
    path = _target_path(project, runtime)
    old_text = path.read_text(encoding="utf-8") if path.exists() else "{}\n"
    document = _load_json_object(path)
    removed = _remove_marker_hooks(document)
    installed = 0
    if not uninstall:
        installed = _install_runtime(document, runtime, project, python)
    new_text = _render(document)
    diff = "".join(
        difflib.unified_diff(
            old_text.splitlines(keepends=True),
            new_text.splitlines(keepends=True),
            fromfile=str(path) + ":before",
            tofile=str(path) + ":after",
        )
    )
    backup = None if dry_run else _write_with_backup(path, old_text, new_text)
    return {
        "runtime": runtime,
        "path": str(path),
        "changed": old_text != new_text,
        "installed_handlers": installed,
        "removed_handlers": removed,
        "backup": str(backup) if backup else None,
        "diff": diff,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Install context-ledger hooks for Claude Code and/or Codex")
    parser.add_argument("--project", default=".", help="project root (default: current directory)")
    parser.add_argument("--runtime", choices=["claude-code", "codex", "both"], default="both")
    parser.add_argument("--python", default=sys.executable, help="Python interpreter used by hooks")
    parser.add_argument("--dry-run", action="store_true", help="show the merged configuration without writing")
    parser.add_argument("--uninstall", action="store_true", help="remove only context-ledger hook handlers")
    parser.add_argument("--no-ledger-init", action="store_true", help="do not initialize .context-ledger")
    parser.add_argument("--show-diff", action="store_true", help="print unified diffs to stderr")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        project = Path(args.project).expanduser().resolve()
        project.mkdir(parents=True, exist_ok=True)
        python = Path(args.python).expanduser().resolve()
        if not python.exists():
            raise ValidationError(f"Python interpreter does not exist: {python}")
        if not HOOK_SCRIPT.is_file():
            raise ValidationError(f"hook adapter is missing: {HOOK_SCRIPT}")
        if not args.no_ledger_init and not args.uninstall:
            init_ledger(project)
        runtimes = ["claude-code", "codex"] if args.runtime == "both" else [args.runtime]
        reports = [
            _process(
                project,
                runtime,
                python,
                uninstall=args.uninstall,
                dry_run=args.dry_run,
            )
            for runtime in runtimes
        ]
        if args.show_diff:
            for report in reports:
                if report["diff"]:
                    print(report["diff"], file=sys.stderr, end="")
        print(
            json.dumps(
                {
                    "ok": True,
                    "dry_run": args.dry_run,
                    "uninstall": args.uninstall,
                    "project": str(project),
                    "reports": [{k: v for k, v in report.items() if k != "diff"} for report in reports],
                    "next_steps": (
                        [
                            "Run context_ledger.py doctor --runtime both after installation.",
                            "Review the installed hooks with the host hook browser before relying on them."
                        ]
                        if not args.uninstall
                        else []
                    ),
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except LedgerError as exc:
        print(f"install-hooks: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
