# Phase 4 — DDL Ergonomics + `USING PATH` Reservation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Polish error messages around BSON-path indexes, and reserve the `USING PATH` keyword
for a future multi-valued (GIN-style) variant so we don't break grammar compatibility later.

**Architecture:** ANTLR grammar changes only. Add `PATH` as a soft keyword token, add a `USING
PATH` clause to `create_index_node` that — for v1 — only emits a `SQLException` with a
"reserved for future release" message. No backend change.

**Tech Stack:** ANTLR3 (Phoenix grammar), Java.

---

## Calibration vs. spec

- The original spec said "make `AS <type>` mandatory on BSON-path index expressions." Phoenix's
  `BSON_VALUE(doc, '$.a.b', 'VARCHAR')` already takes the type as the third argument, so this is
  **already enforced** by the function signature; missing the type arg fails to compile because
  `BSON_VALUE`'s minimum arity is 3 (`BsonValueFunction.java:66-73`). No grammar change needed for
  this requirement.
- `AS <type>` *grammar sugar* on indexed columns (so the user could write
  `CREATE INDEX idx ON t (BSON_VALUE(doc, '$.a.b') AS VARCHAR)`) is a much bigger change to the
  expression grammar and is **out of scope for v1**. We document the existing surface in Phase 5.
- Therefore Phase 4 is intentionally small: only `USING PATH` reservation and error-message
  polish for unsupported BSON-path features.

---

## File Structure

- **Modify** `phoenix-core-client/src/main/antlr3/PhoenixSQL.g` — add `PATH` token (soft);
  extend `create_index_node` with optional `USING PATH` clause; on match, raise a runtime exception
  pointing the user at the v1 limitation.
- **Modify** `phoenix-core-client/src/main/java/org/apache/phoenix/exception/SQLExceptionCode.java`
  — add `BSON_PATH_INDEX_NOT_SUPPORTED`.
- **Create** `phoenix-core/src/test/java/org/apache/phoenix/parse/BsonPathDDLReservedTest.java` —
  unit test asserting `USING PATH` is reserved.

---

## Task 1: Add `BSON_PATH_INDEX_NOT_SUPPORTED` exception code

**Files:**
- Modify: `phoenix-core-client/src/main/java/org/apache/phoenix/exception/SQLExceptionCode.java`

- [ ] **Step 1: Add entry**

Near the `BSON_INDEX_DISABLED` entry added in Phase 2, add:

```java
  BSON_PATH_INDEX_NOT_SUPPORTED(546, "42922",
      "Multi-valued BSON path indexes (USING PATH) are reserved for a future release."),
```

(Increment the numeric error code from whatever Phase 2 used.)

- [ ] **Step 2: Compile**

```
mvn -pl phoenix-core-client -am -DskipTests compile
```

- [ ] **Step 3: Commit**

```
git add phoenix-core-client/src/main/java/org/apache/phoenix/exception/SQLExceptionCode.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: reserve BSON_PATH_INDEX_NOT_SUPPORTED error code"
```

---

## Task 2: Reserve `USING PATH` in the grammar

**Files:**
- Modify: `phoenix-core-client/src/main/antlr3/PhoenixSQL.g`

- [ ] **Step 1: Inspect the existing `create_index_node` rule**

The rule lives around L568-590:

```
create_index_node returns [CreateIndexStatement ret]
    :   CREATE u=UNCOVERED? l=LOCAL? INDEX (IF NOT ex=EXISTS)? i=index_name ON t=from_table_name
        (LPAREN ik=ik_constraint RPAREN)
        (in=INCLUDE (LPAREN icrefs=column_names RPAREN))?
        (WHERE where=expression)?
        (async=ASYNC)?
        (p=fam_properties)?
        (SPLIT ON v=value_expression_list)?
        ...
```

We want to optionally accept a `USING PATH` clause between `ON t=from_table_name` and `(LPAREN
ik=ik_constraint RPAREN)`, and immediately throw on match.

- [ ] **Step 2: Add tokens and rule modification**

Note: ANTLR3 grammars in Phoenix use `=` for token aliases. `USING` does not exist today; check
the grammar's token declarations (top of file). If absent, add:

```
    USING='using';
    PATH='path';
```

Modify the rule to insert a check; the simplest is:

```
create_index_node returns [CreateIndexStatement ret]
    :   CREATE u=UNCOVERED? l=LOCAL? INDEX (IF NOT ex=EXISTS)? i=index_name ON t=from_table_name
        (using=USING using_path=PATH)?
        (LPAREN ik=ik_constraint RPAREN)
        (in=INCLUDE (LPAREN icrefs=column_names RPAREN))?
        ...
        {
            if (using != null) {
                throw new RuntimeException(new SQLExceptionInfo.Builder(
                    SQLExceptionCode.BSON_PATH_INDEX_NOT_SUPPORTED).build().buildException());
            }
            if (u !=null && in != null) { ... existing checks ... }
            ...
        }
```

If `using` is not null, the action throws before constructing the statement.

- [ ] **Step 3: Regenerate ANTLR sources**

```
mvn -pl phoenix-core-client process-sources
```

This will rebuild the lexer/parser. If ANTLR rejects the modified grammar (e.g., because `PATH`
or `USING` clashes with another rule), reduce the change to introduce only the `USING PATH`
sequence as a single semantic-predicate-checked optional. (`PATH` is not a reserved word in
Phoenix today, so it will likely tokenize as an identifier without explicit declaration.)

- [ ] **Step 4: Compile**

```
mvn -pl phoenix-core-client -am -DskipTests compile
```

- [ ] **Step 5: Commit**

```
git add phoenix-core-client/src/main/antlr3/PhoenixSQL.g
git commit --no-gpg-sign -m "PHOENIX BsonPath: reserve USING PATH clause on CREATE INDEX (v1 rejects)"
```

---

## Task 3: Unit test — `USING PATH` is reserved

**Files:**
- Create: `phoenix-core/src/test/java/org/apache/phoenix/parse/BsonPathDDLReservedTest.java`

- [ ] **Step 1: Write test**

```java
package org.apache.phoenix.parse;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BsonPathDDLReservedTest {

  @Test
  public void usingPathIsReserved() {
    String sql = "CREATE INDEX idx ON mytable USING PATH (BSON_VALUE(doc, '$.a', 'VARCHAR'))";
    try {
      new SQLParser(sql).parseStatement();
      fail("expected reserved-keyword error for USING PATH");
    } catch (Exception e) {
      // Either the parser surfaces the wrapped SQLException directly, or the runtime exception
      // contains the marker message — accept both.
      String msg = String.valueOf(e.getMessage()) + " " + (e.getCause() == null ? ""
          : String.valueOf(e.getCause().getMessage()));
      assertTrue("error must mention reserved/USING PATH; got: " + msg,
          msg.toLowerCase().contains("path") || msg.toLowerCase().contains("reserved"));
    }
  }

  @Test
  public void plainCreateIndexStillWorks() throws Exception {
    String sql = "CREATE INDEX idx ON mytable (col1)";
    new SQLParser(sql).parseStatement();
  }
}
```

- [ ] **Step 2: Run**

```
mvn -pl phoenix-core -am -Dtest=BsonPathDDLReservedTest test
```

Expected: 2 tests, PASS.

- [ ] **Step 3: Commit**

```
git add phoenix-core/src/test/java/org/apache/phoenix/parse/BsonPathDDLReservedTest.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: parser test for USING PATH reservation"
```

---

## Local testing plan for Phase 4

| What | Command |
|---|---|
| Regenerate ANTLR | `mvn -pl phoenix-core-client process-sources` |
| Compile | `mvn -pl phoenix-core-client -am -DskipTests install` |
| New unit test | `mvn -pl phoenix-core -Dtest=BsonPathDDLReservedTest test` |
| Existing parser tests sanity | `mvn -pl phoenix-core -Dtest='*ParseTest,*ParserTest' test` |
| Phase 2/3 tests still pass | `mvn -pl phoenix-core -Dit.test='BsonPathIndex*IT' verify` |

---

## Self-review checklist

- [ ] `USING PATH` reserved at parse time with a clear "future release" error.
- [ ] No existing CREATE INDEX form regressed (verified by `*ParserTest` suite).
- [ ] Phase 2/3 ITs still green.
- [ ] No unintended changes to `create_index_node` flag combinations (UNCOVERED/LOCAL/INCLUDE).
