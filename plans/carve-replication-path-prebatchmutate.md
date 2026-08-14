# Carve a dedicated replication path in preBatchMutateWithExceptions

## Motivation

`IndexRegionObserver.preBatchMutateWithExceptions` interleaves the active-cluster
write path and the standby replication-replay path in one ~170-line method,
distinguished by five scattered `context.isReplication` guards. This is fragile:
the standby path is implicit (defined by what the active-only blocks skip, not by
what the standby does), so a new active-side step silently leaks onto the standby
unless every future editor re-derives the `isReplication` question.

Evidence: `getCurrentRowStates` ran on the standby (gated only by index flags, not
`isReplication`), performing a data-table region scan under lock -- the exact
out-of-order-unsafe read the PRE_IMAGE mechanism exists to eliminate -- and
discarding the result. Benign only because the output was unused.

## Design

Fork the *preparation*, keep the *commit* shared. The two-phase index commit
(prepare -> unlock -> doPre -> lock -> [wait] -> post) is a genuine shared protocol
and must not be duplicated (the two copies would have to stay in lockstep).

Seam: after the shared prologue sets `currentPhase = PRE`, dispatch.

```
context.currentPhase = BatchMutatePhase.PRE;
if (context.isReplication) {
  preBatchMutateReplication(c, miniBatchOp, context, indexMetaData);
  return;
}
// active-only body follows, all isReplication guards removed
```

### New method: preBatchMutateReplication
Standby-only. Self-contained list of what the standby does:
- global/uncovered/transform branch -> shared commit helper (batchTimestamp = 0;
  prepareReplicatedIndexMutations ignores it, using group.ts)
- local branch -> getReplicatedRowGroups + buildReplayLocalIndexInputs +
  handleLocalIndexUpdates(PreImageLocalTable)
- failDataTableUpdatesForTesting throw

### New helper: prepareAndCommitGlobalIndexUpdates (shared)
Extracted from the global branch's commit mechanics:
preparePreIndexMutations (+ index-prepare metric) -> unlockRows -> doPre ->
lockRows -> if lastConcurrentBatchContext != null waitForPreviousConcurrentBatch
-> preparePostIndexMutations.

### Active body changes (all now unreachable-on-standby guards removed)
- getCurrentRowStates block: drop the `!context.isReplication &&` disjunct.
- timestamp + captureReplicationCells block: unwrap `if (!context.isReplication)`.
- global branch: unwrap the inner `if (!context.isReplication)` around
  prepareDataRowStates + capturePreImageCells; call shared commit helper.
- local branch: drop the `if (context.isReplication)` half, keep the active `else`.

## Verification
- `mvn -o compile -pl phoenix-core-server` clean.
- `IndexRegionObserverReplayTest` (16 tests) still green.
- `ReplicationLogGroupIT` incl. `testConcurrentUpserts` still green (exercises the
  standby global + local replay paths end-to-end with cross-cluster equality).
- spotless:apply before commit.

## Out of scope
No behavior change intended -- pure structural carve on top of the
getCurrentRowStates skip fix. If any assertion changes, stop and reassess.
