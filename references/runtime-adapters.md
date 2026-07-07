# Runtime adapters

The package records its checked assumptions in `runtime-contracts.json`. Run the doctor after installation and after host upgrades:

```bash
python3 scripts/context_ledger.py --root . doctor --runtime both --probe-binary
```

The doctor checks Python/SQLite versions, ledger shape, installed marker count, event matchers, command paths, timeouts, stale v1 handlers, and observed hook fields. It reports unknown observed events or missing expected fields. It detects drift; it cannot prove every future host behavior.

## Claude Code

Managed events:

- `SessionStart`
- `UserPromptSubmit`
- `PostToolUse`
- `PostToolUseFailure`
- `Stop`
- `PreCompact`
- `PostCompact`

Configuration: `.claude/settings.json`.

## Codex

Managed events:

- `SessionStart`
- `UserPromptSubmit`
- `PostToolUse`
- `Stop`
- `PreCompact`
- `PostCompact`

Configuration: `.codex/hooks.json`. Project-local hooks must be reviewed and trusted by the operator.

## Shared versus actor state

The project database is shared. Approved directives, preferences, decisions, artifacts, and evidence use a project revision. Each runtime/session receives a stable actor ID and its own cursor, brainstorming state, commitments, open loops, and deliberation pointer.

Set `actor_mode` to `session`, `runtime`, or `shared` according to the deployment. Sharing a database does not imply that two actor cursors overwrite each other.
