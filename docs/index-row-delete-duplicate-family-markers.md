# Duplicate family delete markers in `IndexMaintainer.buildRowDeleteMutation`

## Summary

`IndexMaintainer.buildRowDeleteMutation` emits one family-level delete marker per
**covered column** instead of per **distinct index column family**. Because multiple
covered columns commonly share the same index column family, the same family is
processed repeatedly. On the `SINGLE_VERSION` path this produces genuinely duplicated
tombstones that are serialized into the mutation and flow all the way to the WAL,
memstore, HFiles, and the replication log — pure write amplification for no semantic gain.

## Current code

`phoenix-core-client/src/main/java/org/apache/phoenix/index/IndexMaintainer.java:1687`

```java
public Delete buildRowDeleteMutation(byte[] indexRowKey, DeleteType deleteType, long ts) {
  byte[] emptyCF = emptyKeyValueCFPtr.copyBytesIfNecessary();
  Delete delete = new Delete(indexRowKey);

  for (ColumnReference ref : getCoveredColumns()) {          // one iter per covered COLUMN
    ColumnReference indexColumn = coveredColumnsMap.get(ref);
    // If table delete was single version, then index delete should be as well
    if (deleteType == DeleteType.SINGLE_VERSION) {
      delete.addFamilyVersion(indexColumn.getFamily(), ts);  // appends every call
    } else {
      delete.addFamily(indexColumn.getFamily(), ts);         // clears + re-allocs every call
    }
  }
  if (deleteType == DeleteType.SINGLE_VERSION) {
    delete.addFamilyVersion(emptyCF, ts);
  } else {
    delete.addFamily(emptyCF, ts);
  }
  delete.setDurability(!indexWALDisabled ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
  return delete;
}
```

`getCoveredColumns()` returns `coveredColumnsMap.keySet()` (one entry per covered column,
`IndexMaintainer.java:1769`). Each entry maps to an index-side column via
`coveredColumnsMap.get(ref)`, whose `getFamily()` is the index column family. Covered
columns frequently share a single index CF, so the loop hits the same family many times.

## Why the two branches differ

The bug only bites on the `SINGLE_VERSION` branch. HBase's two APIs behave differently
(`hbase-client` `Delete.java`):

- **`addFamily(family, ts)`** (`Delete.java:184`): calls `list.clear()` on the family's
  cell list first, then adds a single `DeleteFamily` marker. Repeated calls for the same
  family → **net one marker**. Wasteful (redundant `clear()` + `new KeyValue(...)` each
  iteration) but **not incorrect**.

  ```java
  public Delete addFamily(final byte[] family, final long timestamp) {
    ...
    List<Cell> list = getCellList(family);
    if (!list.isEmpty()) {
      list.clear();
    }
    KeyValue kv = new KeyValue(row, family, null, timestamp, KeyValue.Type.DeleteFamily);
    list.add(kv);
    return this;
  }
  ```

- **`addFamilyVersion(family, ts)`** (`Delete.java:203`): does **not** clear — it appends a
  `DeleteFamilyVersion` marker on every call. Repeated calls for the same family+ts →
  **duplicate tombstones accumulate** in the family's cell list.

  ```java
  public Delete addFamilyVersion(final byte[] family, final long timestamp) {
    ...
    List<Cell> list = getCellList(family);
    list.add(new KeyValue(row, family, null, timestamp, KeyValue.Type.DeleteFamilyVersion));
    return this;
  }
  ```

`SINGLE_VERSION` row deletes do reach this path (confirmed), so the duplication is live,
not latent.

## Impact

- **`SINGLE_VERSION`**: N duplicate `DeleteFamilyVersion` markers per shared family (N =
  covered columns in that family). Duplicates are serialized into the mutation and
  propagate to WAL / memstore / HFiles / replication log — write amplification and extra
  work in the read-side delete tracker.
- **default**: no duplicate markers (thanks to `addFamily`'s `clear()`), but N redundant
  `clear()` + `KeyValue` allocations per shared family.

## Why `allColumns` cannot be reused

Considered iterating the cached `allColumns` set (`IndexMaintainer.java:426`) instead of
building a family set. It does not work:

1. **Wrong side of the map.** `allColumns` holds **data-table** `ColumnReference`s
   (`initCachedState`, `IndexMaintainer.java:2308-2336`), so `getFamily()` yields the data
   CF, not the index CF that `buildRowDeleteMutation` needs.
2. **Over-inclusion.** `allColumns` = `indexedColumns` + covered columns. `indexedColumns`
   are encoded into the index **row key**, not stored as cells under their own family, so
   they contribute no families to the index row. Iterating `allColumns` would emit markers
   for families the index row does not have.

The families that need markers come solely from `coveredColumnsMap`'s **values** (index
columns) plus the empty-KV CF.

## Recommended fix

The set of index families is fixed for the life of an `IndexMaintainer`, so precompute the
distinct families once in `initCachedState` and iterate the cached set in
`buildRowDeleteMutation` (zero per-delete allocation). Alternatively, build the set locally
per call — correct but allocates each delete.

Precompute sketch:

```java
// new field
private Set<ImmutableBytesPtr> rowDeleteFamilies;

// in initCachedState(), after emptyKeyValueCFPtr / coveredColumnsMap are populated:
this.rowDeleteFamilies =
    Sets.newLinkedHashSetWithExpectedSize(coveredColumnsMap.size() + 1);
for (ColumnReference indexColumn : coveredColumnsMap.values()) {
  rowDeleteFamilies.add(new ImmutableBytesPtr(indexColumn.getFamily()));
}
rowDeleteFamilies.add(emptyKeyValueCFPtr);
```

```java
// buildRowDeleteMutation()
for (ImmutableBytesPtr family : rowDeleteFamilies) {
  if (deleteType == DeleteType.SINGLE_VERSION) {
    delete.addFamilyVersion(family.copyBytesIfNecessary(), ts);
  } else {
    delete.addFamily(family.copyBytesIfNecessary(), ts);
  }
}
```

### Open items before implementing

1. **`initCachedState` ordering.** `initCachedState` is called from the constructor
   (`IndexMaintainer.java:754`) and both deserialization paths (`1941`, `2083`). Verify
   `emptyKeyValueCFPtr` and `coveredColumnsMap` are populated before the precompute point on
   all three paths.
2. **Test.** Add a unit test asserting the resulting `Delete` carries exactly one marker per
   distinct family (and the correct type) for both branches, with multiple covered columns
   sharing a family.

## Imports

`HashSet` (line 38), `java.util.Set` (line 43), and `ImmutableBytesPtr` (line 79) are
already imported; no import-restriction concerns.