# Phase 1 — `BsonPathCanonicalizer` (unwired) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a pure-function `BsonPathCanonicalizer` that rewrites equivalent BSON-path
expressions to one canonical form (`BSON_VALUE(<col>, '$.a.b', '<TYPE>')`). **No callers in
production code yet** — phases 2 and 3 will wire it.

**Architecture:** Subclass of `ParseNodeRewriter` that visits `FunctionParseNode` instances and
returns a normalized replacement when the function is `BSON_VALUE` (or `JSON_VALUE`) and the
second argument string parses as a valid `BsonPath`. All other nodes pass through unchanged.

**Tech Stack:** Java 8, Phoenix's existing `ParseNodeRewriter` infrastructure, JUnit 4.

---

## Calibration vs. spec

The original spec listed `->` and `->>` as input shapes to canonicalize. **Phoenix grammar does
not define those operators** today, so we restrict v1 to the function-call surface that exists:

- `BSON_VALUE(doc, '$.a.b', 'VARCHAR')` — already canonical except for path-string variation.
- `BSON_VALUE(doc, 'a.b', 'VARCHAR')` — leading `$.` missing; canonicalize to `$.a.b`.
- `BSON_VALUE(doc, '$.a.b', 'varchar')` — type name case-folded to upper case.
- `JSON_VALUE(doc, '$.a.b')` — rewritten to `BSON_VALUE(doc, '$.a.b', 'VARCHAR')` for indexing
  purposes (canonical form chooses `BSON_VALUE` since it's the BSON-aware variant). The same input
  that gets indexed should canonicalize identically on both DDL and predicate sides.

> Adding `->` / `->>` operator sugar is **deferred**. Phase 4 reserves the `USING PATH` token
> only; full sugar is a future enhancement and is explicitly out of scope.

---

## File Structure

- **Create** `phoenix-core-client/src/main/java/org/apache/phoenix/compile/BsonPathCanonicalizer.java`
  — public class, two static entry points: `ParseNode rewrite(ParseNode)` and
  `Optional<BsonPath> extractPath(ParseNode)`.
- **Create** `phoenix-core/src/test/java/org/apache/phoenix/compile/BsonPathCanonicalizerTest.java`
  — golden-style unit tests with at least 30 cases.
- **No modifications** to existing files.

---

## Task 1: Skeleton + identity-rewrite test

**Files:**
- Create: `phoenix-core-client/src/main/java/org/apache/phoenix/compile/BsonPathCanonicalizer.java`
- Create: `phoenix-core/src/test/java/org/apache/phoenix/compile/BsonPathCanonicalizerTest.java`

- [ ] **Step 1: Write failing test**

```java
package org.apache.phoenix.compile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.junit.Test;

public class BsonPathCanonicalizerTest {

  private static ParseNode parseExpr(String s) throws Exception {
    return new SQLParser(s).parseExpression();
  }

  @Test
  public void nonBsonNodePassesThrough() throws Exception {
    ParseNode in = parseExpr("a + 1");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertNotNull(out);
    assertEquals(in.toString(), out.toString());
  }
}
```

- [ ] **Step 2: Run, expect compile failure**

```
mvn -pl phoenix-core -am -Dtest=BsonPathCanonicalizerTest test
```

- [ ] **Step 3: Implement minimal canonicalizer (identity for now)**

```java
package org.apache.phoenix.compile;

import java.sql.SQLException;
import org.apache.phoenix.parse.ParseNode;

/**
 * Rewrites BSON-path expression parse nodes into a single canonical form so DDL and predicate
 * forms can be compared for equivalence. Pure function; reads no schema state.
 */
public final class BsonPathCanonicalizer {

  private BsonPathCanonicalizer() {}

  /**
   * Returns a {@link ParseNode} structurally equivalent to {@code node} but with all recognized
   * BSON-path expressions rewritten to canonical form. If no rewrite applies, returns
   * {@code node} unchanged.
   */
  public static ParseNode rewrite(ParseNode node) throws SQLException {
    if (node == null) return null;
    return node;
  }
}
```

- [ ] **Step 4: Run, expect PASS**

```
mvn -pl phoenix-core -am -Dtest=BsonPathCanonicalizerTest test
```

- [ ] **Step 5: Commit**

```
git add phoenix-core-client/src/main/java/org/apache/phoenix/compile/BsonPathCanonicalizer.java \
        phoenix-core/src/test/java/org/apache/phoenix/compile/BsonPathCanonicalizerTest.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: canonicalizer skeleton (identity rewrite)"
```

---

## Task 2: Canonicalize standalone `BSON_VALUE`

**Files:**
- Modify: `phoenix-core-client/src/main/java/org/apache/phoenix/compile/BsonPathCanonicalizer.java`
- Modify: `phoenix-core/src/test/java/org/apache/phoenix/compile/BsonPathCanonicalizerTest.java`

- [ ] **Step 1: Append failing tests**

```java
  @Test
  public void canonicalizesBareDotPath() throws Exception {
    ParseNode in = parseExpr("BSON_VALUE(doc, 'a.b', 'VARCHAR')");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals("BSON_VALUE(DOC, '$.a.b', 'VARCHAR')", out.toString());
  }

  @Test
  public void canonicalIsAlreadyCanonical() throws Exception {
    ParseNode in = parseExpr("BSON_VALUE(doc, '$.a.b', 'VARCHAR')");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals("BSON_VALUE(DOC, '$.a.b', 'VARCHAR')", out.toString());
  }

  @Test
  public void canonicalizesTypeCase() throws Exception {
    ParseNode in = parseExpr("BSON_VALUE(doc, '$.a', 'varchar')");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals("BSON_VALUE(DOC, '$.a', 'VARCHAR')", out.toString());
  }

  @Test
  public void canonicalizesArrayIndex() throws Exception {
    ParseNode in = parseExpr("BSON_VALUE(doc, 'a[0]', 'BIGINT')");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals("BSON_VALUE(DOC, '$.a[0]', 'BIGINT')", out.toString());
  }

  @Test
  public void canonicalizesQuotedKey() throws Exception {
    ParseNode in = parseExpr("BSON_VALUE(doc, \"['weird key']\", 'VARCHAR')");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals("BSON_VALUE(DOC, '$[''weird key'']', 'VARCHAR')", out.toString());
  }

  @Test
  public void invalidPathIsLeftAlone() throws Exception {
    ParseNode in = parseExpr("BSON_VALUE(doc, '$..bad', 'VARCHAR')");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    // unsupported path → no rewrite, returns input unchanged.
    assertEquals(in.toString(), out.toString());
  }

  @Test
  public void argCountMismatchLeftAlone() throws Exception {
    ParseNode in = parseExpr("BSON_VALUE(doc, 'a.b')");  // missing type arg
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals(in.toString(), out.toString());
  }
```

> Note: Phoenix's `ParseNode.toSQL` outputs identifiers in upper case (`DOC`) and uses single
> quotes; the embedded single quotes inside path strings get doubled per SQL escaping. The
> assertions above match that convention.

- [ ] **Step 2: Run, expect failures**

- [ ] **Step 3: Implement canonicalizer logic**

```java
package org.apache.phoenix.compile;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.phoenix.expression.function.BsonValueFunction;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.ParseNodeRewriter;
import org.apache.phoenix.parse.bson.BsonPath;
import org.apache.phoenix.parse.bson.BsonPathParser;
import org.apache.phoenix.parse.bson.BsonPathSyntaxException;
import org.apache.phoenix.schema.types.PVarchar;

public final class BsonPathCanonicalizer {

  private static final ParseNodeFactory FACTORY = new ParseNodeFactory();
  private static final String BSON_VALUE_NAME = BsonValueFunction.NAME; // "BSON_VALUE"
  private static final int BSON_VALUE_INDEXABLE_ARITY = 3;

  private BsonPathCanonicalizer() {}

  public static ParseNode rewrite(ParseNode node) throws SQLException {
    if (node == null) return null;
    return ParseNodeRewriter.rewrite(node, new Visitor());
  }

  /**
   * If {@code node} is a recognized canonical-or-canonicalizable BSON-path expression, return its
   * underlying {@link BsonPath}. Otherwise, return {@code null}. Used by the predicate rewriter to
   * key into indexed-expression maps.
   */
  public static BsonPath extractPath(ParseNode node) {
    if (!(node instanceof FunctionParseNode)) return null;
    FunctionParseNode fn = (FunctionParseNode) node;
    if (!BSON_VALUE_NAME.equalsIgnoreCase(fn.getName())) return null;
    List<ParseNode> args = fn.getChildren();
    if (args.size() != BSON_VALUE_INDEXABLE_ARITY) return null;
    ParseNode pathArg = args.get(1);
    if (!(pathArg instanceof LiteralParseNode)) return null;
    Object v = ((LiteralParseNode) pathArg).getValue();
    if (!(v instanceof String)) return null;
    try {
      return BsonPathParser.parse((String) v);
    } catch (BsonPathSyntaxException ignored) {
      return null;
    }
  }

  private static final class Visitor extends ParseNodeRewriter {
    @Override
    public ParseNode visitLeave(FunctionParseNode node, List<ParseNode> children)
        throws SQLException {
      if (!BSON_VALUE_NAME.equalsIgnoreCase(node.getName())) {
        return super.visitLeave(node, children);
      }
      if (children.size() != BSON_VALUE_INDEXABLE_ARITY) {
        return super.visitLeave(node, children);
      }
      ParseNode pathArg = children.get(1);
      ParseNode typeArg = children.get(2);
      if (!(pathArg instanceof LiteralParseNode)
          || !(typeArg instanceof LiteralParseNode)) {
        return super.visitLeave(node, children);
      }
      Object pathVal = ((LiteralParseNode) pathArg).getValue();
      Object typeVal = ((LiteralParseNode) typeArg).getValue();
      if (!(pathVal instanceof String) || !(typeVal instanceof String)) {
        return super.visitLeave(node, children);
      }
      BsonPath path;
      try {
        path = BsonPathParser.parse((String) pathVal);
      } catch (BsonPathSyntaxException unsupported) {
        return super.visitLeave(node, children);
      }
      String canonicalType = ((String) typeVal).toUpperCase(java.util.Locale.ROOT);
      String canonicalPath = path.toString();
      if (canonicalPath.equals(pathVal) && canonicalType.equals(typeVal)) {
        return super.visitLeave(node, children);
      }
      List<ParseNode> rewritten = new ArrayList<>(BSON_VALUE_INDEXABLE_ARITY);
      rewritten.add(children.get(0));
      rewritten.add(new LiteralParseNode(canonicalPath, PVarchar.INSTANCE));
      rewritten.add(new LiteralParseNode(canonicalType, PVarchar.INSTANCE));
      return FACTORY.function(BSON_VALUE_NAME, rewritten);
    }
  }
}
```

- [ ] **Step 4: Run, fix any test failures**

```
mvn -pl phoenix-core -am -Dtest=BsonPathCanonicalizerTest test
```

If `Phoenix's ParseNode.toSQL` produces output with different escaping than assumed, update the
test's expected string to match the actual output (the underlying behavior is what matters; the
test is verifying canonicalization, not exact SQL printing). Use `System.out.println(out)` once,
read the actual output, then bake the right expected value into the assertion.

- [ ] **Step 5: Commit**

```
git add phoenix-core-client/src/main/java/org/apache/phoenix/compile/BsonPathCanonicalizer.java \
        phoenix-core/src/test/java/org/apache/phoenix/compile/BsonPathCanonicalizerTest.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: canonicalize BSON_VALUE path arg + type case"
```

---

## Task 3: Canonicalize `JSON_VALUE`

**Files:**
- Modify: `phoenix-core-client/src/main/java/org/apache/phoenix/compile/BsonPathCanonicalizer.java`
- Modify: `phoenix-core/src/test/java/org/apache/phoenix/compile/BsonPathCanonicalizerTest.java`

- [ ] **Step 1: Append failing tests**

```java
  @Test
  public void jsonValueRewritesToBsonValueVarchar() throws Exception {
    ParseNode in = parseExpr("JSON_VALUE(doc, '$.a.b')");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals("BSON_VALUE(DOC, '$.a.b', 'VARCHAR')", out.toString());
  }

  @Test
  public void jsonValueWithBarePath() throws Exception {
    ParseNode in = parseExpr("JSON_VALUE(doc, 'a.b')");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals("BSON_VALUE(DOC, '$.a.b', 'VARCHAR')", out.toString());
  }

  @Test
  public void jsonValueWithUnsupportedPathLeftAlone() throws Exception {
    ParseNode in = parseExpr("JSON_VALUE(doc, '$.*')");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals(in.toString(), out.toString());
  }
```

- [ ] **Step 2: Run, expect failure**

- [ ] **Step 3: Extend the visitor to handle `JSON_VALUE`**

In `BsonPathCanonicalizer.Visitor.visitLeave`, before the `BSON_VALUE` branch, add:

```java
      if ("JSON_VALUE".equalsIgnoreCase(node.getName())) {
        if (children.size() != 2) {
          return super.visitLeave(node, children);
        }
        ParseNode pathArg = children.get(1);
        if (!(pathArg instanceof LiteralParseNode)) {
          return super.visitLeave(node, children);
        }
        Object pathVal = ((LiteralParseNode) pathArg).getValue();
        if (!(pathVal instanceof String)) {
          return super.visitLeave(node, children);
        }
        BsonPath path;
        try {
          path = BsonPathParser.parse((String) pathVal);
        } catch (BsonPathSyntaxException unsupported) {
          return super.visitLeave(node, children);
        }
        List<ParseNode> rewritten = new ArrayList<>(BSON_VALUE_INDEXABLE_ARITY);
        rewritten.add(children.get(0));
        rewritten.add(new LiteralParseNode(path.toString(), PVarchar.INSTANCE));
        rewritten.add(new LiteralParseNode("VARCHAR", PVarchar.INSTANCE));
        return FACTORY.function(BSON_VALUE_NAME, rewritten);
      }
```

- [ ] **Step 4: Run, expect PASS**

- [ ] **Step 5: Commit**

```
git add phoenix-core-client/src/main/java/org/apache/phoenix/compile/BsonPathCanonicalizer.java \
        phoenix-core/src/test/java/org/apache/phoenix/compile/BsonPathCanonicalizerTest.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: canonicalizer rewrites JSON_VALUE to BSON_VALUE"
```

---

## Task 4: Canonicalize within compound expressions

**Files:**
- Modify: `phoenix-core/src/test/java/org/apache/phoenix/compile/BsonPathCanonicalizerTest.java`

- [ ] **Step 1: Append failing tests**

```java
  @Test
  public void canonicalizesInsideEquality() throws Exception {
    ParseNode in = parseExpr("BSON_VALUE(doc, 'a.b', 'varchar') = 'x'");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals("BSON_VALUE(DOC, '$.a.b', 'VARCHAR') = 'x'", out.toString());
  }

  @Test
  public void canonicalizesInsideAnd() throws Exception {
    ParseNode in = parseExpr(
        "BSON_VALUE(doc, 'a', 'varchar') = 'x' AND BSON_VALUE(doc, 'b', 'bigint') > 5");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals(
        "(BSON_VALUE(DOC, '$.a', 'VARCHAR') = 'x' AND BSON_VALUE(DOC, '$.b', 'BIGINT') > 5)",
        out.toString());
  }

  @Test
  public void canonicalizesInsideIn() throws Exception {
    ParseNode in = parseExpr("BSON_VALUE(doc, 'a', 'varchar') IN ('x', 'y')");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals("BSON_VALUE(DOC, '$.a', 'VARCHAR') IN ('x','y')", out.toString());
  }
```

- [ ] **Step 2: Run; expected behavior is that they already pass**, because we used
  `ParseNodeRewriter.rewrite` (which traverses the whole tree). If they fail, dump `out.toString()`
  to stdout, adapt the expected literal once, and re-run.

```
mvn -pl phoenix-core -am -Dtest=BsonPathCanonicalizerTest test
```

- [ ] **Step 3: Commit**

```
git add phoenix-core/src/test/java/org/apache/phoenix/compile/BsonPathCanonicalizerTest.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: canonicalizer recurses into compound nodes"
```

---

## Task 5: `extractPath` API — used by Phase 3

**Files:**
- Modify: `phoenix-core/src/test/java/org/apache/phoenix/compile/BsonPathCanonicalizerTest.java`

- [ ] **Step 1: Append failing tests**

```java
  @Test
  public void extractPathReturnsBsonPathForCanonicalizable() throws Exception {
    ParseNode in = parseExpr("BSON_VALUE(doc, 'a.b', 'VARCHAR')");
    ParseNode canon = BsonPathCanonicalizer.rewrite(in);
    org.apache.phoenix.parse.bson.BsonPath p = BsonPathCanonicalizer.extractPath(canon);
    assertEquals("$.a.b", p.toString());
  }

  @Test
  public void extractPathReturnsNullForOther() throws Exception {
    ParseNode in = parseExpr("a + 1");
    org.junit.Assert.assertNull(BsonPathCanonicalizer.extractPath(in));
  }

  @Test
  public void extractPathReturnsNullForBadPath() throws Exception {
    ParseNode in = parseExpr("BSON_VALUE(doc, '$..bad', 'VARCHAR')");
    org.junit.Assert.assertNull(BsonPathCanonicalizer.extractPath(in));
  }
```

- [ ] **Step 2: Run, expect PASS** (the API was added in Task 2; these are coverage tests).

- [ ] **Step 3: Commit**

```
git add phoenix-core/src/test/java/org/apache/phoenix/compile/BsonPathCanonicalizerTest.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: extractPath helper coverage"
```

---

## Local testing plan for Phase 1

| What | Command |
|---|---|
| Compile phoenix-core-client | `mvn -pl phoenix-core-client -am -DskipTests install` |
| Run canonicalizer tests only | `mvn -pl phoenix-core -Dtest=BsonPathCanonicalizerTest test` |
| Confirm zero production callers in main src | `grep -rl "BsonPathCanonicalizer" phoenix-core-client/src/main/java phoenix-core-server/src/main/java phoenix-core/src/main/java` should return only the canonicalizer file itself |
| All bson tests together | `mvn -pl phoenix-core -Dtest='BsonPath*Test' test` |

---

## Self-review checklist

- [ ] All 5 tasks committed.
- [ ] Canonicalizer rewrites `BSON_VALUE` and `JSON_VALUE` to canonical `BSON_VALUE`.
- [ ] Unsupported paths leave the node unchanged (no exception escapes).
- [ ] Compound trees recurse properly.
- [ ] `extractPath` handles canonical, non-canonical, and unrelated nodes.
- [ ] No production code outside `phoenix-core-client/src/main/java/org/apache/phoenix/compile/BsonPathCanonicalizer.java` was modified.
