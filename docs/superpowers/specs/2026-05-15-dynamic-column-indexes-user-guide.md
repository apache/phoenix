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
- `getTypeConflictRejects()` — UPSERTs and CREATE INDEX statements rejected
  with `PHOENIX_DYNAMIC_TYPE_CONFLICT` (mismatch with the registered virtual
  column's declared type).

These are best-effort, JVM-local AtomicLongs. Use for ad-hoc diagnostics
during development and migration; not aggregated reporting.

## Configuration

| Property | Default | Effect |
|---|---|---|
| `phoenix.index.dynamic.enabled` | `true` | Master switch. When false, `CREATE INDEX … DYNAMIC` is rejected. Already-promoted columns remain readable. |

## Known limitations

- **Mixed-version cluster — sparse-skip on old servers.** The sparse-skip
  semantics (no index entry for rows that omit the indexed virtual column)
  rely on a new `IndexMaintainer` proto field (tag 34, optional repeated)
  introduced with this feature. Old region servers receiving an
  `IndexMaintainer` from a new client silently ignore the field and emit
  a regular index entry for every UPSERT — including ones that don't
  supply the virtual column. The result is correct (queries still work)
  but indexes are denser than expected during a rolling upgrade. Wait for
  all region servers to upgrade before relying on sparse-skip economics.
- **`SYSTEM.CATALOG` `IS_VIRTUAL` column backfill on upgrade.** The first
  client connecting to an upgraded cluster runs an
  `ALTER TABLE SYSTEM.CATALOG ADD IF NOT EXISTS IS_VIRTUAL BOOLEAN`. The
  ALTER is idempotent and adds the column lazily; until that first connect
  has run, `CREATE INDEX … DYNAMIC` will fail with a column-not-found
  error against `SYSTEM.CATALOG`. Reconnect once and retry.
- **Atomicity of CREATE INDEX … DYNAMIC.** Promotion and index-create are
  two RPCs: column promotion lands in `SYSTEM.CATALOG` first, then the
  index-create RPC fires. On failure of the second RPC, a compensating
  `ALTER TABLE … DROP COLUMN` runs to un-promote. If the client crashes
  between RPCs, the virtual column is left orphaned until the next
  `CREATE INDEX` (which is idempotent for already-promoted matching-type
  columns) or until the user manually drops it.
- **CDC streams.** Virtual-column cells appear in CDC change images like
  any other cell. There is no special "sparse-aware" CDC behavior in v1.
  If you rely on CDC and add a virtual-column index, expect change
  records for every UPSERT that supplies the column. Not tested in v1.
- **Multi-tenant views.** Views inherit virtual columns from their base
  table and the inherited column remains `isVirtual=true`. Tenants cannot
  promote new virtual columns through a view (CREATE INDEX … DYNAMIC on
  a view is not yet validated). Not tested in v1.
- **Transactional tables.** Feature C is not tested against transactional
  tables. CREATE INDEX … DYNAMIC against a transactional table may behave
  unpredictably. Recommend non-transactional tables until v2.
- **ALTER TABLE ADD COLUMN collision.** If a user runs `ALTER TABLE t ADD
  extra VARCHAR` against a table where `extra` is already a promoted
  virtual column, Phoenix's standard `IF NOT EXISTS` semantics apply: the
  ALTER is a no-op and the column remains virtual. Use `DROP INDEX … ON t`
  first to un-promote, then re-add as a regular column.

