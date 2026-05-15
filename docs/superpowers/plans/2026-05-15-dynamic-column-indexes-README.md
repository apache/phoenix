# Feature C — Dynamic-Column Secondary Indexes

**Spec:** `docs/superpowers/specs/2026-05-15-dynamic-column-indexes-design.md`
**Branch:** `feature/json-indexes`
**Mode:** Fully autonomous via `superpowers:subagent-driven-development`. Each task tracked with TaskCreate/TaskUpdate; each phase ends with a `git commit` (unsigned).

## Phase order — **strictly sequential**

| Phase | Plan file | What changes for users |
|---|---|---|
| 0 | `2026-05-15-phase-0-isvirtual-plumbing.md` | Nothing visible; `isVirtual` flag plumbed end-to-end. |
| 1 | `2026-05-15-phase-1-ddl-grammar-promotion.md` | `CREATE INDEX … DYNAMIC` parses and promotes column; queries resolve. |
| 2 | `2026-05-15-phase-2-write-path.md` | UPSERTs maintain index; type-conflict guard; sparse-skip. |
| 3 | `2026-05-15-phase-3-drop-observability.md` | DROP INDEX un-promotes; counters; user guide. |

A subagent dispatched on Phase N **must not start** until Phase N-1's exit criteria are green.

## Autonomous execution recipe

1. **Subagent dispatch per phase**, fresh context, single plan file.
2. **Two-stage review** between phases:
   - Stage 1: subagent reports tests green and commits visible.
   - Stage 2: parent runs `./run-it-tests-local.sh <PhaseN_ITs>` to verify independently before allowing Phase N+1 to start.
3. **Fix-forward only.** Do not skip failing tests; do not amend prior phase commits to mask Phase N regressions — fix in a new commit on the current phase.
4. **No commit signing.** All commits use `git -c commit.gpgsign=false commit --no-gpg-sign`.
5. **Terminal task tracking.** Each phase opens its own root task in TaskCreate, with one child task per Task block in the plan; child tasks update to `completed` as they're done so the user sees the live phase state.

## Rollback

| Phase | Rollback |
|---|---|
| 0 | Strictly additive; revert phase commits if needed. |
| 1 | `phoenix.index.dynamic.enabled=false` rejects all `DYNAMIC` DDL at parse time. |
| 2 | Same flag covers the type-conflict guard. Disabling preserves availability at the cost of correctness — emergency only. |
| 3 | Cosmetic. |

## Definition of Done

- All 4 phases' exit criteria met.
- `git log --oneline` shows ~20–25 small Feature-C commits.
- `PROGRESS.md` Feature-C section all four boxes ticked.
- `BsonFlatIndexIT`, `JsonFlatIndexIT`, `DynamicColumnIT`, all five new dyncol ITs green.
