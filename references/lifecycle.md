# Lifecycle

## Capture and disposition

1. `UserPromptSubmit` transactionally appends the visible user message and creates a `pending` disposition.
2. The hook returns the event ID and the required disposition choices.
3. A provenance-backed semantic delta atomically classifies the source event, or `dispose` records `no_state_change` / `deferred` with a reason.
4. `PostToolUse` records configured tool metadata. Claude Code also exposes `PostToolUseFailure` in the checked contract.
5. `Stop` captures the bounded assistant message, blocks pending user dispositions, then blocks an uncheckpointed execution cursor when material work occurred.

## Compaction and resume

1. `PreCompact` appends a boundary event, writes the actor capsule, performs full audit verification, and runs configured prefix archival.
2. `PostCompact` stores a bounded provider summary as `diagnostic-only`; it cannot update user authority.
3. `SessionStart` injects the actor capsule for `startup`, `resume`, and `compact`. `clear` is excluded from restore injection by default.

## Failure split

Core library and CLI operations fail closed on malformed state, provenance violations, invalid revisions, corrupt archives, or failed replay.

The lifecycle adapter catches errors at the host boundary and emits a neutral response so a ledger failure does not wedge the coding client. Compaction can be configured to block on checkpoint failure only after deployment-specific testing.

## Recovery

SQLite transactions and WAL protect the hot store. `reconcile` verifies the complete archive + live chain, replays semantic events, and replaces materialized project, actor, and disposition rows. A v1 JSONL ledger is imported only after its original chain validates.
