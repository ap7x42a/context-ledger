# Context Ledger

Context Ledger is an agent skill and lifecycle adapter for preserving visible
operational continuity outside a model context window. It records exact visible
events, user directives, decisions, active task state, evidence references, and
handoff cursors in a project-local SQLite ledger.

It is not hidden memory. It does not preserve private chain-of-thought or infer
what a user "really meant." Captured content is data; semantic state changes are
explicit operations with provenance.

## Use It When

- Long-running agent work must survive compaction, resume, or model handoff.
- A project needs exact user directives and decisions to remain visible.
- Several agent runtimes share one repository and need non-overwriting state.
- Handoffs keep losing the current cursor, caveats, or evidence.
- You need an audit trail that separates raw capture from semantic
  classification.

## What The Package Includes

- `SKILL.md` - the operating contract for continuity work.
- `scripts/context_ledger.py` - operator CLI for inspect, apply, dispose,
  checkpoint, validate, archive, reconcile, export, and recovery operations.
- `scripts/ledger_core.py` - SQLite event/state/archive implementation.
- `hooks/runtime_hook.py` - lifecycle hook adapter for supported runtimes.
- `scripts/install_hooks.py` - non-clobbering hook installer and uninstaller.
- `scripts/runtime_doctor.py` - installation and hook-contract diagnostics.
- `hooks/` - runtime hook fragments.
- `schemas/` - JSON schemas for config, deltas, events, and state.
- `references/` - lifecycle, precedence, privacy, runtime adapter, and delta
  protocol documentation.
- `scripts/self_test.py` - regression suite for event capture, revisions,
  archives, recovery, hook behavior, and contention cases.

## Storage Model

The private project ledger lives under:

```text
<repo>/.context-ledger/
├── ledger.sqlite3
├── archives/
├── restore.md
├── restores/
├── config.json
└── exports/
```

The installer adds `.context-ledger/` to `.gitignore` by default. Review capture
and retention settings before using it in projects with sensitive data.

## Quick Start

Run the package tests:

```bash
python3 scripts/self_test.py
```

Install hooks from a stable package path:

```bash
python3 scripts/install_hooks.py --project /absolute/path/to/repo --runtime both
python3 scripts/context_ledger.py --root /absolute/path/to/repo doctor --runtime both
```

Use `--runtime claude-code` or `--runtime codex` to install one runtime only.
The installer backs up changed config, removes prior marked handlers, preserves
peer hooks, supports `--dry-run`, and uninstalls only its own marked handlers.

## Operator Workflow

Inspect recent events and revisions:

```bash
python3 scripts/context_ledger.py --root . inspect \
  --actor codex:SESSION --recent-events 12
```

Apply a provenance-backed semantic delta:

```bash
python3 scripts/context_ledger.py --root . apply \
  --actor codex:SESSION --delta-file delta.json
```

Mark a captured user turn as handled without durable state change:

```bash
python3 scripts/context_ledger.py --root . dispose \
  --actor codex:SESSION \
  --event-id evt-000000000042 \
  --status no_state_change \
  --reason "Answered a one-off question"
```

Checkpoint before stopping:

```bash
python3 scripts/context_ledger.py --root . checkpoint \
  --actor codex:SESSION \
  --goal "Refactor parser" \
  --last-completed "Added failing fixture" \
  --in-progress "Implementing parser change" \
  --next-action "Run parser regression suite" \
  --source-event evt-000000000042
```

## Install As An Agent Skill

```bash
git clone https://github.com/ap7x42a/context-ledger.git
cp -a context-ledger ~/.codex/skills/context-ledger
```

For project-local skill surfaces, copy the directory into the location your
runtime uses, such as `.agents/skills/context-ledger`.

## Verify The Package

```bash
python3 scripts/self_test.py
python3 scripts/write_manifest.py --check
sha256sum -c SHA256SUMS.txt
```

## Limits

The runtime adapter is designed to fail open at the host boundary so a ledger
fault does not wedge the client. Core ledger commands fail closed on invalid
data. Context Ledger preserves visible operational state; it does not override
current instructions, prove authenticity, or make compact summaries authoritative.
