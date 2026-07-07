# State delta protocol

A delta is an idempotent JSON object:

```json
{
  "delta_id": "stable-idempotency-key",
  "base_project_revision": 3,
  "base_actor_revision": 8,
  "source_event_ids": ["evt-000000000009"],
  "operations": [
    {"op": "update_cursor", "data": {"next_action": "Run tests"}}
  ]
}
```

`delta_id` is unique and idempotent. Base revisions are advisory under normal rebasing and mandatory under `--strict-revision`. Each operation can override `source_event_ids`; otherwise it inherits the top-level list.

A successful delta and disposition updates occur in one SQLite transaction. Any cited pending user event becomes `classified`; a checkpoint deliberately does not classify its source events.

## Project operations

| Operation | Required data | Purpose |
|---|---|---|
| `add_directive` | `text` | Add user instruction; user provenance required |
| `set_directive_status` | `id`, `status` | Supersede, expire, cancel, or tombstone directive |
| `add_preference` | `value` | Add explicit/inferred preference; user provenance required |
| `set_preference_status` | `id`, `status` | Transition preference |
| `add_decision` | `decision` | Add conclusion and rationale |
| `set_decision_status` | `id`, `status` | Supersede, reverse, or defer decision |
| `upsert_artifact` | `path` | Add/update artifact reference |
| `set_artifact_status` | `id`, `status` | Transition artifact |
| `add_evidence` | `summary` | Add observation/reference |
| `set_evidence_status` | `id`, `status` | Verify, dispute, refute, or supersede evidence |

## Actor operations

| Operation | Required data | Purpose |
|---|---|---|
| `set_active_task` | `goal` | Create/replace task and cursor |
| `update_cursor` | existing active task | Update phase, completion, work, next action, blockers, status |
| `add_idea` | `text` | Add brainstorm branch |
| `set_idea_status` | `id`, `status` | Transition branch |
| `add_commitment` | `text` | Record promised work |
| `set_commitment_status` | `id`, `status` | Complete/cancel/defer commitment |
| `add_open_loop` | `question` | Record unresolved issue |
| `set_open_loop_status` | `id`, `status` | Complete/cancel/defer loop |
| `set_active_deliberation` | `run_id`, `path` | Point continuity state at a deliberation run |
| `clear_active_deliberation` | none | Remove active run pointer |

## Disposition operations

`classified` can only be produced by a semantic delta citing the user event. `dispose` accepts only `no_state_change` or `deferred` and requires a reason. A pending event cannot be archived.

## Concurrency

Project and actor mutations increment independent revisions. Normal mode replays each operation against current state inside an immediate transaction, preventing lost-update overwrites for compatible operations. Strict mode rejects stale expected revisions. Semantic conflicts remain domain-level decisions and are not automatically resolved by the database.
