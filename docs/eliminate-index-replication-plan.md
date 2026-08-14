# Plan: Eliminate Index Mutation Replication

## Context

Today the primary cluster replicates **both** data table mutations and index table mutations to the standby cluster. Since the standby has the same index metadata (via SYSTEM.CATALOG replication), it can regenerate index mutations locally from data mutations using IndexRegionObserver (IRO). Eliminating index mutation replication reduces replication log size/bandwidth and simplifies the write path.

### Current flow (primary → standby)

1. **`preBatchMutate`** generates index mutations and writes them to the WAL as `IndexedKeyValue` entries (for replication crash recovery path).
2. **`postBatchMutateIndispensably` → `replicateMutations()`** appends data mutations, pre-index mutations, and post-index mutations to the `ReplicationLogGroup`.
3. **`preWALRestore`** (crash recovery) extracts both data and `IndexedKeyValue` entries from the WAL and appends both to the `ReplicationLogGroup`.
4. On the standby, **`ReplicationLogProcessor.processLogFile()`** reads all records and applies them via `table.batchAll()`.

### Key finding: why IRO doesn't fire on standby today

`PhoenixIndexCodec.isEnabled()` (line 154) requires `INDEX_UUID` attribute on the mutation. `LogFileCodec` does **not** serialize mutation attributes, so replicated mutations arrive at the standby without `INDEX_UUID` and IRO never fires.

### Solution: serialize mutation attributes in the replication log codec

Data mutations carry attributes set by the client (`INDEX_UUID`, `SCHEMA_NAME`, `LOGICAL_TABLE_NAME`, etc. via `IndexMetaDataCacheClient` and `ScanUtil.annotateMutationWithMetadataAttributes()`). If we serialize these attributes in `LogFileCodec.RecordEncoder` and restore them in `RecordDecoder`, the mutations arrive at the standby with the same attributes they had on the primary. IRO fires naturally — no changes needed in `ReplicationLogProcessor`.

---

## Implementation

### 1. Primary side — stop replicating index mutations

**File:** `phoenix-core-server/src/main/java/org/apache/phoenix/hbase/index/IndexRegionObserver.java`

**1a.** In `replicateMutations()` (lines 2663–2680): remove the `context.preIndexUpdates` and `context.postIndexUpdates` loops entirely. Only data mutation appends (lines 2650–2661) remain.

**1b.** In `replicateEditOnWALRestore()` (lines 762–764): skip `IndexedKeyValue` entries — just `continue` past them. Data mutation reconstruction (lines 766–803) remains unchanged.

**1c.** In `addGlobalIndexMutationsToWAL()` (lines 1886–1921): skip entirely (early return or remove the method body). The `IndexedKeyValue` WAL entries use `WALEdit.METAFAMILY` which are "not applied on restore" (per source comments lines 1904–1905). They exist solely for the replication extraction path (`preWALRestore` → `replicateEditOnWALRestore()`). Primary crash recovery restores data mutations from WAL; IRO regenerates index mutations during normal write processing.

### 2. Replication log codec — serialize mutation attributes

**File:** `phoenix-core-server/src/main/java/org/apache/phoenix/replication/log/LogFileCodec.java`

**2a.** In `RecordEncoder.write()` (after line 130, after writing mutation timestamp): serialize the mutation's attribute map.

```
ATTRIBUTES SECTION (new, appended after mutation timestamp):
+--------------------------------------------+
| NUMBER OF ATTRIBUTES (vint)                |
+--------------------------------------------+
| PER-ATTRIBUTE (repeated)                   |
|   +--------------------------------------+ |
|   | ATTRIBUTE KEY LENGTH (vint)          | |
|   | ATTRIBUTE KEY (byte[])               | |
|   | ATTRIBUTE VALUE LENGTH (vint)        | |
|   | ATTRIBUTE VALUE (byte[])             | |
|   +--------------------------------------+ |
+--------------------------------------------+
```

Use `mutation.getAttributesMap()` to get the attributes. Serialize all attributes (key as UTF-8 bytes, value as raw bytes).

**2b.** In `RecordDecoder.advance()` (after line 219, after reading mutation timestamp): deserialize attributes and call `mutation.setAttribute(key, value)` for each.

**2c.** No backward compatibility handling needed — feature is still in development. Just add the attributes section to the format.

### 3. WAL — save and restore mutation attributes for crash recovery

The crash recovery path (`preWALRestore` → `replicateEditOnWALRestore()`) reconstructs mutations from WAL cells. These reconstructed mutations need the same attributes so the replication log codec serializes them and IRO fires on the standby.

**File:** `phoenix-core-server/src/main/java/org/apache/phoenix/hbase/index/IndexRegionObserver.java`

**3a.** `appendMutationAttributesToWALKey()` (lines 1984–1997) already saves `MutationMetadataType` attributes (SCHEMA_NAME, LOGICAL_TABLE_NAME, TENANT_ID, etc.) to the WAL key via `key.addExtendedAttribute()`. Additionally save `INDEX_UUID` to the WAL key:

```java
byte[] indexUuid = attrMap.get(PhoenixIndexCodec.INDEX_UUID);
if (indexUuid != null) {
    IndexRegionObserver.appendToWALKey(key, PhoenixIndexCodec.INDEX_UUID, indexUuid);
}
```

**3b.** In `replicateEditOnWALRestore()` (lines 756–805): when constructing Put/Delete mutations from WAL cells, copy the relevant attributes from the WAL key's extended attributes onto each mutation:

```java
// After constructing the Put/Delete, before appending to logGroup:
Map<String, byte[]> walKeyAttrs = getAttributeValuesFromWALKey(logKey);
for (MutationState.MutationMetadataType metadataType : MutationState.MutationMetadataType.values()) {
    byte[] val = walKeyAttrs.get(metadataType.toString());
    if (val != null) {
        mutation.setAttribute(metadataType.toString(), val);
    }
}
byte[] indexUuid = walKeyAttrs.get(PhoenixIndexCodec.INDEX_UUID);
if (indexUuid != null) {
    mutation.setAttribute(PhoenixIndexCodec.INDEX_UUID, indexUuid);
}
```

This ensures crash-recovered mutations carry the same attributes as normal-path mutations, so the codec serializes them and IRO fires on the standby.

### 4. No changes to ReplicationLogProcessor

`ReplicationLogProcessor.processLogFile()` continues to read records and apply mutations via `table.batchAll()`. Because mutations now arrive with their original attributes (including `INDEX_UUID`, `SCHEMA_NAME`, `LOGICAL_TABLE_NAME`), IRO on the standby fires naturally and generates index mutations.

### 5. No infinite replication loop (already safe)

When `ReplicationLogProcessor` calls `table.batchAll()`, the mutations lack `HA_GROUP_NAME_ATTRIB` (not one of the attributes serialized by the client — it's set by the HA layer). So on the standby:
- `getHAGroupFromBatch()` returns `Optional.empty()`
- `replicateMutations()` exits at line 2646
- Index mutations generated by standby IRO are NOT re-replicated

---

## Correctness considerations

| Concern | Resolution |
|---------|-----------|
| Index metadata availability on standby | SYSTEM.CATALOG is replicated through the same replication log pipeline. DDL ordering is preserved. |
| Timestamp consistency | Cell timestamps are serialized in the replication log. IRO uses those timestamps for index mutations. |
| Row state for updates/deletes | Replication log preserves ordering. Single-writer HA model guarantees consistent row state. |
| Primary/standby crash recovery | `IndexedKeyValue` WAL entries (METAFAMILY) are "not applied on restore" (per source comments). HBase WAL replay restores data mutations; IRO regenerates index mutations during normal processing. Skipping `addGlobalIndexMutationsToWAL()` is safe. |
| Tables without indexes | Mutations for non-indexed tables have no `INDEX_UUID` attribute (client doesn't set it). IRO's `isEnabled()` returns false. No change in behavior. |
| Backward compatibility | Feature is still in development; no old-format handling needed. |

---

## Files to modify

1. `phoenix-core-server/src/main/java/org/apache/phoenix/hbase/index/IndexRegionObserver.java` — stop writing index mutations to WAL and replication log; save/restore INDEX_UUID in WAL key; copy attributes to reconstructed mutations in crash recovery path
2. `phoenix-core-server/src/main/java/org/apache/phoenix/replication/log/LogFileCodec.java` — serialize/deserialize mutation attributes in the replication log format
3. `phoenix-core/src/it/java/org/apache/phoenix/replication/ReplicationLogGroupIT.java` — fix existing tests and add new end-to-end test for index regeneration on standby

## Testing

### Fix existing tests

**File:** `phoenix-core/src/it/java/org/apache/phoenix/replication/ReplicationLogGroupIT.java`

- **`testAppendAndSync()`** (line 262–273): Currently expects index mutations in the replication log (e.g., `indexName1 → rowCount * 3`, `indexName2 → rowCount * 2`). After this change, only data table mutations should be in the log. Remove index table expectations. Data table expected count changes since index-related mutations (pre/post) are no longer written.
- **`testWALRestore()`** (line 372–382): Same — remove `indexName` expectation from the verification map.

### New integration test

Add a new test in `ReplicationLogGroupIT` that:
1. Creates a table with global indexes on the primary
2. Inserts data
3. Verifies the replication log contains ONLY data table mutations (no index table mutations)
4. Replays the log on the standby via `ReplicationLogProcessor`
5. Queries the index table on the standby and verifies it has the correct data (generated by local IRO)

This end-to-end test validates that index mutations are regenerated on the standby from data mutations alone.

## Verification

1. Build: `mvn package -pl phoenix-core-server -DskipTests`
2. Existing tests: `mvn verify -pl phoenix-core -Dit.test=ReplicationLogGroupIT` — fix and verify
3. Unit test: verify `LogFileCodec` round-trips mutation attributes correctly
4. Unit test: verify `replicateMutations()` no longer appends index mutations
5. Unit test: verify `replicateEditOnWALRestore()` skips `IndexedKeyValue` entries and restores attributes from WAL key
