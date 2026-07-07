#!/usr/bin/env python3
"""Small process worker used by the context-ledger contention harness."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import ledger_core as lc  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="context-ledger contention worker")
    parser.add_argument("--root", required=True)
    parser.add_argument("--actor", required=True)
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--semantic", action="store_true")
    args = parser.parse_args()
    if args.count < 0:
        parser.error("--count must be non-negative")
    root = Path(args.root).expanduser().resolve()
    for index in range(args.count):
        event, created, _ = lc.append_event(
            root,
            actor_id=args.actor,
            role="user" if args.semantic else "tool",
            event_type="user_message" if args.semantic else "tool_result",
            text=f"{args.actor}-{index}" if args.semantic else None,
            payload={} if args.semantic else {"activity_summary": f"tool-{args.actor}-{index}"},
            turn_id=f"{args.actor}-{index}",
            runtime="contention-test",
            dedupe_key=f"{args.actor}-{index}",
        )
        if not created:
            raise RuntimeError("unexpected duplicate in contention worker")
        if args.semantic:
            lc.apply_delta(
                root,
                actor_id=args.actor,
                delta={
                    "delta_id": f"delta-{args.actor}-{index}",
                    "base_project_revision": 0,
                    "source_event_ids": [event["event_id"]],
                    "operations": [
                        {"op": "add_directive", "data": {"text": f"directive-{args.actor}-{index}"}}
                    ],
                },
                runtime="contention-test",
            )
    print(json.dumps({"actor": args.actor, "count": args.count, "semantic": args.semantic}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
