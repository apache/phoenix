# BSON Path Functional Indexes — User Guide

This is a short companion to the design spec at
`docs/superpowers/specs/2026-05-05-bson-path-functional-indexes-design.md`.

## What you can do today

Define a secondary index on a path inside a `BSON` column:

    CREATE TABLE orders (
      id   VARCHAR PRIMARY KEY,
      doc  BSON
    );

    CREATE INDEX idx_orders_customer
      ON orders (BSON_VALUE(doc, '$.customer.id', 'VARCHAR'));

Queries that name the same canonical BSON path will use the index automatically:

    SELECT id FROM orders WHERE BSON_VALUE(doc, '$.customer.id', 'VARCHAR') = 'C-42';
    SELECT id FROM orders WHERE BSON_VALUE(doc, 'customer.id', 'VARCHAR')   = 'C-42';
    SELECT id FROM orders
       WHERE BSON_VALUE(doc, '$.customer.id', 'VARCHAR') IN ('C-42', 'C-43');

Both forms canonicalize to `BSON_VALUE(DOC, '$.customer.id', 'VARCHAR')` and hit the index.

## Sparse semantics

If a row's BSON document does not contain the indexed path, **no index entry is written for
that row** (sparse index). Consequence: you cannot use a BSON path index to find missing-path
rows via `IS NULL`.

## Type contract

`BSON_VALUE`'s third argument fixes the SQL type of the indexed key. Match the WHERE clause to
the same type: index built `AS BIGINT` requires the predicate to be a numeric literal, not a
string. v1 does not yet rewrite `CAST(BSON_VALUE(...) AS BIGINT) = 1` for you.

## Predicate forms that hit the index

| Form | Uses index? |
|---|---|
| `BSON_VALUE(doc, p, 'VARCHAR') = 'x'` | Yes |
| `BSON_VALUE(doc, p, 'VARCHAR') IN (...)` | Yes |
| `BSON_VALUE(doc, p, 'VARCHAR') BETWEEN ...` | Yes |
| `BSON_VALUE(doc, p, 'VARCHAR') > 'x'` | Yes |
| `UPPER(BSON_VALUE(doc, p, 'VARCHAR')) = 'X'` | No |
| `BSON_VALUE(doc, p, 'VARCHAR') LIKE 'a%'` | No |
| `BSON_VALUE(doc, p, 'VARCHAR') IS NULL` | No (sparse) |

## Path language supported in v1

| Form | Example | Supported |
|---|---|---|
| Dot | `$.a.b.c` | Yes |
| Array index | `$.a[0]`, `$.a[10][3]` | Yes |
| Quoted key | `$['weird key']`, `$["odd"]` | Yes |
| Bare path | `a.b`, `a[0]` (canonicalized to `$.a.b`) | Yes |
| Wildcards | `$.*`, `$[*]` | No |
| Filters | `$[?(@.x>1)]` | No |
| Recursive descent | `$..x` | No |
| Slice | `$[0:2]` | No |

## Feature flags

| Flag | Default | Effect when `false` |
|---|---|---|
| `phoenix.index.bson.enabled` | `true` | `CREATE INDEX` on BSON paths is rejected |
| `phoenix.index.bson.rewrite.enabled` | `true` | Indexes still maintained; queries don't use them |

## Observability

Client-process counters in `org.apache.phoenix.monitoring.BsonPathMetrics`:

- `getSparseSkips()` — number of UPSERT rows that hit a missing-path branch and were
  skipped from the index.
- `getRewriteHits()` — number of WHERE-clause sub-expressions that matched a BSON path index
  after canonicalization.
- `getRewriteMisses()` — number of BSON-path WHERE expressions that did not match any indexed
  expression (typically: wrapped LHS, or no relevant index defined).

## What's not yet supported

- Multi-valued (GIN-style) BSON path indexes — DDL keyword `USING PATH` is reserved but not
  implemented.
- Local BSON path indexes, async-build, eventually-consistent BSON path indexes.
- `IS NULL` rewrite, `LIKE`, function-wrapped LHS.
- `->` / `->>` operator sugar.
- Coprocessor / server-side metric publication — counters are client-process only and not
  promoted to Phoenix's `MetricInfo` enum yet.
