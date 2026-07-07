# Build receipt — context-ledger v2

Validated on YYYY-MM-DD.

## Contract

Preserve the v1 authority boundary while replacing history-sized appends, enforcing a terminal semantic disposition for every captured user turn, isolating shared project authority from per-actor execution state, bounding live history, and diagnosing host-hook drift.

## Verified behavior

| Claim | Instrument | Observed result | Status | Caveat |
|---|---|---|---|---|
| Captured data is never executed or promoted into authority without provenance | injection fixture plus user-role provenance tests | shell-like prompt remained inert; tool-sourced directive/preference operations were rejected | verified | tests cannot prove every future extension preserves the boundary |
| Normal append avoids full-history replay | source guard plus 1,000-event latency benchmark | no `load_events`/`validate_ledger` call in append; early median 0.005737 s, late median 0.006215 s, ratio 1.083 | verified | SQLite/index cost can still change with filesystem, journal mode, and very large databases |
| Every captured user turn has an enforced disposition | disposition lifecycle and Stop-hook tests | pending turn blocked Stop; semantic delta produced `classified`; explicit `no_state_change` and `deferred` remained auditable | verified | enforcement does not prove that an agent's classification is semantically correct |
| Shared authority and execution cursors do not overwrite each other | independent revision tests plus multiprocess contention | four writers preserved 80/80 raw events; four semantic writers preserved 40/40 directives; actor cursors retained independent revisions | verified | network filesystems and unusual SQLite builds were not tested |
| Prefix archival bounds live rows without losing audit continuity | archive, exact lookup, dedupe, export, and tamper fixtures | archived prefixes remained chain-verifiable and addressable; dedupe survived archival; modified archive was rejected | verified | archives are local consistency records, not authenticated releases |
| v1 migration does not silently trust malformed history | valid and tampered JSONL fixtures | valid v1 chain migrated and was retained as a validated backup; tampered chain was rejected before import | verified | only the documented v1 event shape is covered |
| Restore capsules are bounded and preserve operational categories | capsule, backlog, deliberation-pointer, and clipping tests | directives, cursor, branches, deferred work, and active deliberation pointer rendered within configured limit | verified | older omitted details require targeted ledger lookup |
| Hook installation is non-clobbering and contract drift is visible | installer idempotency/uninstall fixtures plus runtime doctor positive/negative cases | peer handlers survived; v1/v2 managed handlers deduplicated; missing event and unknown observed shape were reported | verified | doctor checks the dated manifest and observed shapes; it cannot guarantee future undocumented host behavior |
| Materialized state is recoverable | state-row tamper plus reconcile test | full verification detected mismatch; reconcile rebuilt state from verified audit events | verified | wholesale malicious rewrite of database and manifests is outside the threat model |

## Executed package checks

- Source `scripts/self_test.py`: **31/31** behavioral and adversarial cases passed.
- Extracted archive `scripts/self_test.py`: **31/31** cases passed.
- Multiprocess contention: raw sequence uniqueness and semantic no-lost-update invariants passed.
- Python compilation: all scripts and hooks passed.
- JSON parsing: schemas, hook fragments, and runtime contract manifest passed.
- Drift manifest: **24/24** package entries verified.
- Static skill validator: **PASS**, with no errors or warnings.
- Archive safety: one `context-ledger/` root; no traversal paths, symlinks, caches, bytecode, or nested archives.
- Cross-skill integration: a generated `set_active_deliberation` delta applied to context-ledger and restored the exact run ID, phase, three active branches, source event, and next action.

## Boundaries

- No claim is made that this skill runs untrusted code safely.
- The package does not preserve or request hidden chain-of-thought.
- Semantic extraction remains agent-assisted; the package enforces disposition and provenance, not perfect interpretation.
- Hash chaining is consistency detection, not authenticity or tamper prevention.
- The hook adapter fails open at the host boundary; core state operations fail closed.
- Full user prompts are retained by default unless capture/redaction policy is changed.
