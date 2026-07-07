#!/usr/bin/env python3
"""Command-line interface for context-ledger v2."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from ledger_core import (  # noqa: E402
    LedgerError,
    ValidationError,
    append_event,
    apply_delta,
    archive_events,
    checkpoint,
    export_audit,
    get_event,
    init_ledger,
    inspect_ledger,
    load_config,
    pending_dispositions,
    reconcile,
    recover_transcript,
    safe_actor_id,
    set_disposition,
    validate_ledger,
    write_restore_capsule,
)


def _json_dump(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _read_text_argument(text: Optional[str], text_file: Optional[str]) -> Optional[str]:
    if text is not None and text_file is not None:
        raise ValidationError("use either --text or --text-file, not both")
    if text_file == "-":
        return sys.stdin.read()
    if text_file:
        try:
            return Path(text_file).read_text(encoding="utf-8")
        except OSError as exc:
            raise ValidationError(f"cannot read text file {text_file}: {exc}") from exc
    return text


def _read_json_file(path: str) -> Any:
    try:
        if path == "-":
            return json.load(sys.stdin)
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValidationError(f"cannot read JSON from {path}: {exc}") from exc


def _actor(root: Path, supplied: Optional[str]) -> str:
    if supplied:
        return safe_actor_id(supplied)
    env = os.environ.get("CONTEXT_LEDGER_ACTOR") or os.environ.get("CONTEXT_LEDGER_CHANNEL")
    if env:
        return safe_actor_id(env)
    return safe_actor_id(str(load_config(root)["default_actor"]))


def _add_actor_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--actor", "--channel", dest="actor", help="actor/session state id")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Preserve visible operational continuity in a transactional, provenance-backed context ledger."
    )
    parser.add_argument("--root", default=".", help="project root containing .context-ledger")
    sub = parser.add_subparsers(dest="command", required=True)

    init_p = sub.add_parser("init", help="initialize or migrate a private project ledger")
    init_p.add_argument("--no-gitignore", action="store_true")

    append_p = sub.add_parser("append", help="append a raw event without semantic promotion")
    _add_actor_argument(append_p)
    append_p.add_argument("--role", required=True, choices=["user", "assistant", "tool", "system"])
    append_p.add_argument("--event-type", required=True)
    append_p.add_argument("--text")
    append_p.add_argument("--text-file")
    append_p.add_argument("--payload-file")
    append_p.add_argument("--session-id")
    append_p.add_argument("--turn-id")
    append_p.add_argument("--runtime", default="manual")
    append_p.add_argument("--dedupe-key")

    apply_p = sub.add_parser("apply", help="apply a provenance-backed semantic delta")
    _add_actor_argument(apply_p)
    apply_p.add_argument("--delta-file", required=True)
    apply_p.add_argument("--runtime", default="agent")
    apply_p.add_argument("--session-id")
    apply_p.add_argument("--turn-id")
    apply_p.add_argument("--strict-revision", action="store_true")

    dispose_p = sub.add_parser("dispose", help="mark a captured user turn no-state-change or deferred")
    _add_actor_argument(dispose_p)
    dispose_p.add_argument("--event-id", required=True)
    dispose_p.add_argument("--status", required=True, choices=["no_state_change", "deferred"])
    dispose_p.add_argument("--reason", required=True)
    dispose_p.add_argument("--runtime", default="agent")
    dispose_p.add_argument("--session-id")

    checkpoint_p = sub.add_parser("checkpoint", help="checkpoint the actor execution cursor")
    _add_actor_argument(checkpoint_p)
    checkpoint_p.add_argument("--goal")
    checkpoint_p.add_argument("--acceptance", action="append", default=[])
    checkpoint_p.add_argument("--phase")
    checkpoint_p.add_argument("--last-completed")
    checkpoint_p.add_argument("--in-progress")
    checkpoint_p.add_argument("--next-action")
    checkpoint_p.add_argument("--blocker", action="append")
    checkpoint_p.add_argument("--status", choices=["active", "paused", "completed", "cancelled"])
    checkpoint_p.add_argument("--source-event", action="append", default=[])
    checkpoint_p.add_argument("--session-id")
    checkpoint_p.add_argument("--turn-id")
    checkpoint_p.add_argument("--delta-id")

    capsule_p = sub.add_parser("capsule", help="regenerate and print a bounded restore capsule")
    _add_actor_argument(capsule_p)
    capsule_p.add_argument("--path-only", action="store_true")

    validate_p = sub.add_parser("validate", help="verify SQLite, chain, dispositions, and replayed state")
    validate_p.add_argument("--quick", action="store_true", help="skip full chain/replay verification")
    validate_p.add_argument("--json", action="store_true")

    sub.add_parser("reconcile", help="rebuild state and dispositions from the complete audit history")

    inspect_p = sub.add_parser("inspect", help="show current project and actor state")
    _add_actor_argument(inspect_p)
    inspect_p.add_argument("--recent-events", type=int, default=20)

    pending_p = sub.add_parser("pending", help="show unresolved user-turn dispositions")
    _add_actor_argument(pending_p)
    pending_p.add_argument("--turn-id")
    pending_p.add_argument("--include-deferred", action="store_true")

    event_p = sub.add_parser("event", help="show one exact source event")
    event_p.add_argument("event_id")

    export_p = sub.add_parser("export-audit", help="write a complete JSONL audit export")
    export_p.add_argument("--path")

    archive_p = sub.add_parser("archive", help="archive a contiguous live event prefix")
    archive_p.add_argument("--keep-live", type=int)
    archive_p.add_argument("--through-sequence", type=int)

    recover_p = sub.add_parser("recover-transcript", help="best-effort bounded transcript import")
    recover_p.add_argument("transcript")
    _add_actor_argument(recover_p)
    recover_p.add_argument("--runtime", required=True)
    recover_p.add_argument("--session-id")
    recover_p.add_argument("--max-bytes", type=int, default=50_000_000)

    doctor_p = sub.add_parser("doctor", help="inspect ledger and installed Claude Code/Codex hook contracts")
    doctor_p.add_argument("--runtime", choices=["claude-code", "codex", "both"], default="both")
    doctor_p.add_argument("--probe-binary", action="store_true")
    doctor_p.add_argument("--json", action="store_true")

    sub.add_parser("config", help="print effective configuration")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    root = Path(args.root).expanduser().resolve()
    try:
        if args.command == "init":
            directory = init_ledger(root, write_gitignore=not args.no_gitignore)
            _json_dump({"ok": True, "ledger_dir": str(directory)})
            return 0

        init_ledger(root)

        if args.command == "append":
            payload: Optional[Dict[str, Any]] = None
            if args.payload_file:
                loaded = _read_json_file(args.payload_file)
                if not isinstance(loaded, dict):
                    raise ValidationError("--payload-file must contain a JSON object")
                payload = loaded
            event, created, state = append_event(
                root,
                actor_id=_actor(root, args.actor),
                role=args.role,
                event_type=args.event_type,
                text=_read_text_argument(args.text, args.text_file),
                payload=payload,
                session_id=args.session_id,
                turn_id=args.turn_id,
                runtime=args.runtime,
                dedupe_key=args.dedupe_key,
            )
            _json_dump(
                {
                    "ok": True,
                    "created": created,
                    "event": event,
                    "project_revision": state["project_revision"],
                    "actor_revision": state["actor_revision"],
                }
            )
            return 0

        if args.command == "apply":
            delta = _read_json_file(args.delta_file)
            if not isinstance(delta, dict):
                raise ValidationError("delta file must contain a JSON object")
            event, state = apply_delta(
                root,
                actor_id=_actor(root, args.actor),
                delta=delta,
                runtime=args.runtime,
                session_id=args.session_id,
                turn_id=args.turn_id,
                strict_revision=True if args.strict_revision else None,
            )
            _json_dump(
                {
                    "ok": True,
                    "event_id": event["event_id"],
                    "rebased": bool(event.get("payload", {}).get("rebased")),
                    "project_revision": state["project_revision"],
                    "actor_revision": state["actor_revision"],
                }
            )
            return 0

        if args.command == "dispose":
            event, state = set_disposition(
                root,
                event_id=args.event_id,
                actor_id=_actor(root, args.actor),
                status=args.status,
                reason=args.reason,
                runtime=args.runtime,
                session_id=args.session_id,
            )
            _json_dump({"ok": True, "event_id": event["event_id"], "state": state})
            return 0

        if args.command == "checkpoint":
            event, state = checkpoint(
                root,
                actor_id=_actor(root, args.actor),
                goal=args.goal,
                acceptance_criteria=args.acceptance,
                phase=args.phase,
                last_completed=args.last_completed,
                in_progress=args.in_progress,
                next_action=args.next_action,
                blockers=args.blocker,
                status=args.status,
                source_event_ids=args.source_event,
                session_id=args.session_id,
                turn_id=args.turn_id,
                delta_id=args.delta_id,
            )
            _json_dump(
                {
                    "ok": True,
                    "event_id": event["event_id"],
                    "project_revision": state["project_revision"],
                    "actor_revision": state["actor_revision"],
                }
            )
            return 0

        if args.command == "capsule":
            path, text = write_restore_capsule(root, actor_id=_actor(root, args.actor))
            print(path if args.path_only else text, end="\n" if args.path_only else "")
            return 0

        if args.command == "validate":
            report = validate_ledger(root, full=not args.quick)
            if args.json:
                _json_dump(report)
            else:
                print("RESULT:", "PASS" if report["ok"] else "FAIL")
                for error in report["errors"]:
                    print(f"ERROR: {error}")
                for warning in report["warnings"]:
                    print(f"WARNING: {warning}")
                for key, value in sorted(report["checks"].items()):
                    print(f"{key}: {value}")
            return 0 if report["ok"] else 1

        if args.command == "reconcile":
            result = reconcile(root)
            _json_dump(
                {
                    "ok": True,
                    "event_count": result["event_count"],
                    "project_revision": result["project"]["revision"],
                    "actors": sorted(result["actors"]),
                }
            )
            return 0

        if args.command == "inspect":
            _json_dump(inspect_ledger(root, actor_id=_actor(root, args.actor), recent_events=args.recent_events))
            return 0

        if args.command == "pending":
            _json_dump(
                pending_dispositions(
                    root,
                    actor_id=_actor(root, args.actor),
                    turn_id=args.turn_id,
                    include_deferred=args.include_deferred,
                )
            )
            return 0

        if args.command == "event":
            _json_dump(get_event(root, args.event_id))
            return 0

        if args.command == "export-audit":
            _json_dump(export_audit(root, args.path))
            return 0

        if args.command == "archive":
            _json_dump(
                archive_events(
                    root,
                    keep_live_events=args.keep_live,
                    through_sequence=args.through_sequence,
                )
            )
            return 0

        if args.command == "recover-transcript":
            _json_dump(
                recover_transcript(
                    root,
                    transcript_path=args.transcript,
                    actor_id=_actor(root, args.actor),
                    runtime=args.runtime,
                    session_id=args.session_id,
                    max_bytes=args.max_bytes,
                )
            )
            return 0

        if args.command == "doctor":
            from runtime_doctor import run_doctor  # imported lazily to keep core CLI small

            report = run_doctor(root, runtime=args.runtime, probe_binary=args.probe_binary)
            if args.json:
                _json_dump(report)
            else:
                print("RESULT:", "PASS" if report["ok"] else "FAIL")
                for check in report["checks"]:
                    print(f"{check['status'].upper()}: {check['name']} — {check['detail']}")
            return 0 if report["ok"] else 1

        if args.command == "config":
            _json_dump(load_config(root))
            return 0

        raise ValidationError(f"unsupported command: {args.command}")
    except LedgerError as exc:
        print(f"context-ledger: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
