# Indexing Dynamic Columns in Apache Phoenix

**Audience:** Phoenix users wanting indexes on tenant-defined / sparse columns
that aren't present in the base table schema.
**Available from:** Apache Phoenix 5.x (commit hash to be filled in at release).

## TL;DR

```sql
CREATE INDEX idx_extra ON my_table (extra VARCHAR DYNAMIC);

-- Sparse: only rows that supply `extra` produce an index entry.
UPSERT INTO my_table (pk, extra) VALUES ('row1', 'hello');
UPSERT INTO my_table (pk, regular) VALUES ('row2', 'no extra');

-- Hits the index.
SELECT pk FROM my_table WHERE extra = 'hello';

-- Removes both the index AND the virtual column registration
-- (when no other index still references `extra`).
DROP INDEX idx_extra ON my_table;
```

## When to use

- Tenant-defined or schema-on-read columns where most rows omit the column.
- You want index lookups on the column without widening the base schema.
- The column has a stable, declared type — Phoenix rejects UPSERTs that
  declare a conflicting type.

## When NOT to use

- The column will be present on most rows: use `ALTER TABLE ADD COLUMN` +
  `CREATE INDEX`. You give up no flexibility and gain `SELECT *` ergonomics.
- You need different types per row: this is a category mismatch with
  Phoenix's typed index model. Consider a BSON column with path indexes
  (Feature A+B in this codebase).

## Semantics

| Behavior | Detail |
|---|---|
| Visibility in `SELECT *` | Excluded. Reference by explicit name. |
| Sparse rows | UPSERTs that omit the column produce no index entry. |
| Type conflict | `UPSERT INTO t(extra INTEGER)` against a `VARCHAR DYNAMIC` index → `PHOENIX_DYNAMIC_TYPE_CONFLICT`. |
| Encoded tables | Not supported. Use `COLUMN_ENCODED_BYTES=0`. |
| `DROP INDEX` | When the last index referencing the virtual column is dropped, the column is also removed from `SYSTEM.CATALOG`. Cells in HBase remain. |

## Restrictions

- Type must be explicit. `CREATE INDEX idx ON t(extra DYNAMIC)` is rejected.
- Names cannot collide with existing regular columns.
- Sync global indexes only in v1. Local, async, and eventually-consistent
  variants are not yet supported on virtual columns.

## Observability

Three JVM-local counters live in
`org.apache.phoenix.monitoring.DynamicColumnIndexMetrics`:

- `getPromotions()` — virtual columns added to `SYSTEM.CATALOG`.
- `getUnpromotions()` — virtual columns removed via DROP INDEX.
- `getTypeConflictRejects()` — UPSERTs rejected with `PHOENIX_DYNAMIC_TYPE_CONFLICT`.

These are best-effort, JVM-local AtomicLongs. Use for ad-hoc diagnostics
during development and migration; not aggregated reporting.

## Configuration

| Property | Default | Effect |
|---|---|---|
| `phoenix.index.dynamic.enabled` | `true` | Master switch. When false, `CREATE INDEX … DYNAMIC` is rejected. Already-promoted columns remain readable. |
