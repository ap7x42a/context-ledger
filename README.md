# context-ledger v2

A Python Agent Skill and lifecycle adapter that preserves visible operational continuity outside the model context window.

## What changed in v2

- SQLite is the authoritative transactional store; append no longer replays the full history.
- Every captured user turn must reach `classified`, `no_state_change`, or `deferred` before Stop proceeds.
- Project authority and actor execution state use independent revisions.
- Hash-chain verification moved off the write hot path.
- Verified prefix archives bound live database growth while preserving exact lookup, dedupe, and full audit export.
- `runtime_doctor.py` checks installed hook handlers against a dated contract manifest and reports observed shape drift.
- Valid v1 JSONL ledgers are chain-verified before migration; invalid ones are rejected.

## Quick start

```bash
python3 scripts/self_test.py
python3 scripts/install_hooks.py --project /absolute/path/to/repo --runtime both
python3 scripts/context_ledger.py --root /absolute/path/to/repo doctor --runtime both
```

The project ledger lives under `<repo>/.context-ledger/` and is added to `.gitignore` by default. Review capture and retention settings before use with sensitive data.

Raw visible capture is automatic. Semantic classification remains agent-assisted; the package enforces a terminal disposition and provenance, not perfect interpretation.

## Package map

- `scripts/context_ledger.py`: operator CLI.
- `scripts/ledger_core.py`: authoritative SQLite, event, state, archive, and restore library.
- `hooks/runtime_hook.py`: Claude Code and Codex lifecycle adapter.
- `scripts/install_hooks.py`: non-clobbering hook installer and uninstaller.
- `scripts/runtime_doctor.py`: dated hook-contract and installation diagnostics.
- `scripts/contention_worker.py`: isolated multiprocess regression helper used by `scripts/self_test.py`.
- `scripts/write_manifest.py`: package drift-manifest writer and verifier.
