# Phase 0 — `BsonPath` Value Type + Parser Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce an internal, immutable, structural `BsonPath` value class and a `BsonPathParser` for the JSONPath subset we plan to support. **Zero production callers in Phase 0.** Wiring happens in later phases.

**Architecture:** A pure value type plus a recursive-descent parser. Both live in
`phoenix-core-client` and are package-public so later phases (canonicalizer, compile,
IndexMaintainer) can use them. No Phoenix runtime is touched.

**Tech Stack:** Java 8 (Phoenix targets 1.8 source), JUnit 4 (Phoenix's existing test framework).

---

## Calibration vs. spec

Verified against the codebase before writing this plan:

- The blanket "JSON fragment" guard in `MetaDataClient` (`isJsonFragment`) **only fires for
  `JsonQueryParseNode` / `JsonModifyParseNode`** (`ExpressionCompiler.java:313`). It does **not**
  fire for `BSON_VALUE` or `JSON_VALUE`, so those indexes are **not** blocked today. Phase 2 will
  verify and lock down behavior on top of that.
- Phoenix grammar does **not** define Postgres-style `->` / `->>` operators today. Phase 1's
  canonicalizer will target the function-call surface that exists: `BSON_VALUE(doc, '$.a.b',
  'VARCHAR')`, `BSON_VALUE(doc, 'a.b', 'VARCHAR')`, and `JSON_VALUE(doc, '$.a.b')`. Adding `->`
  /`->>` is deferred (out of scope for this feature).
- `BSON_VALUE`'s third argument already carries the SQL type name. The spec's "mandatory `AS
  <type>`" requirement is therefore satisfied by the existing `BSON_VALUE` arity. Phase 4 adds
  optional grammar sugar; v1 reuses the existing function call shape.

Phase 0 itself does not depend on any of the above — but later phases do. Carrying the calibration
here so the implementer can follow the chain.

---

## File Structure

- **Create** `phoenix-core-client/src/main/java/org/apache/phoenix/parse/bson/BsonPath.java` —
  immutable value class. Holds an ordered list of `BsonPath.Segment` objects, structural equality,
  canonical `toString`, deterministic `hashCode`.
- **Create** `phoenix-core-client/src/main/java/org/apache/phoenix/parse/bson/BsonPath.java`
  inner classes `Segment`, `FieldSegment`, `IndexSegment`. (Same file — keep the path domain object
  cohesive.)
- **Create** `phoenix-core-client/src/main/java/org/apache/phoenix/parse/bson/BsonPathParser.java` —
  recursive-descent parser. Public method: `static BsonPath parse(String input) throws
  BsonPathSyntaxException`.
- **Create** `phoenix-core-client/src/main/java/org/apache/phoenix/parse/bson/BsonPathSyntaxException.java`
  — checked exception with `int errorOffset` and `String message`.
- **Create** `phoenix-core/src/test/java/org/apache/phoenix/parse/bson/BsonPathParserTest.java` —
  JUnit 4 unit tests (positive + negative + fuzz).
- **Create** `phoenix-core/src/test/java/org/apache/phoenix/parse/bson/BsonPathTest.java` — equality,
  `toString` round-trip, hashing tests.

**No file modifications in this phase.** Pure additions.

**Build verification:** `mvn -pl phoenix-core-client -am -DskipTests install` should compile
cleanly. Tests live in `phoenix-core` (the place where unit tests for client code live in this
repo — see `phoenix-core/src/test/java/org/apache/phoenix/parse/IndexConsistencyParseTest.java`).

---

## Path language (v1 supported subset)

Accepted:
- Optional leading `$` then `.` segment
- Dot field segments: `$.a`, `$.a.b.c`. Field name must match `[A-Za-z_][A-Za-z0-9_]*`.
- Bracketed array indices: `$.a[0]`, `$.a[10][3]`. Index must be a non-negative decimal
  integer (`[0-9]+`).
- Bracketed quoted field segments: `$.a['weird key']`, `$["odd"]`. Quotes are single (`'`) or
  double (`"`). Backslash-escapes inside quoted segments: `\\`, `\'`, `\"`.

Rejected (with `BsonPathSyntaxException`):
- Wildcards: `$.*`, `$[*]`
- Filters: `$[?(...)]`
- Recursive descent: `$..x`
- Slice: `$[0:2]`
- Empty path, trailing `.`, mismatched `[` / `]`, unterminated quoted segment, segment with
  invalid characters.
- Leading `.` without `$`. (Path can be `$.a.b`, `$.a`, or — for compatibility with `BSON_VALUE`'s
  pre-existing input form — bare `a.b` and `a` and `a[0]`. The parser MUST accept the bare form
  too, normalizing it to start with `$.`.)

---

## Task 1: Test scaffolding & `BsonPathSyntaxException`

**Files:**
- Create: `phoenix-core-client/src/main/java/org/apache/phoenix/parse/bson/BsonPathSyntaxException.java`
- Create: `phoenix-core/src/test/java/org/apache/phoenix/parse/bson/BsonPathParserTest.java`

- [ ] **Step 1: Write the failing skeleton test**

```java
package org.apache.phoenix.parse.bson;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class BsonPathParserTest {

  @Test
  public void exceptionTypeIsCheckedAndCarriesOffset() {
    BsonPathSyntaxException e = new BsonPathSyntaxException("bad", 3);
    assertNotNull(e.getMessage());
    org.junit.Assert.assertEquals(3, e.getErrorOffset());
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

```
mvn -pl phoenix-core -am -Dtest=BsonPathParserTest test
```

Expected: compile error (`BsonPathSyntaxException` does not exist).

- [ ] **Step 3: Write the exception class**

```java
package org.apache.phoenix.parse.bson;

/** Thrown by {@link BsonPathParser} when input does not match the supported JSONPath subset. */
public class BsonPathSyntaxException extends Exception {
  private static final long serialVersionUID = 1L;
  private final int errorOffset;

  public BsonPathSyntaxException(String message, int errorOffset) {
    super(message + " (at offset " + errorOffset + ")");
    this.errorOffset = errorOffset;
  }

  public int getErrorOffset() {
    return errorOffset;
  }
}
```

- [ ] **Step 4: Run test, expect pass**

```
mvn -pl phoenix-core -am -Dtest=BsonPathParserTest test
```

Expected: 1 test, PASS.

- [ ] **Step 5: Commit**

```
git add phoenix-core-client/src/main/java/org/apache/phoenix/parse/bson/BsonPathSyntaxException.java \
        phoenix-core/src/test/java/org/apache/phoenix/parse/bson/BsonPathParserTest.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: add exception type for path parser (Phase 0/1)"
```

---

## Task 2: `BsonPath` value type with structural equality

**Files:**
- Create: `phoenix-core-client/src/main/java/org/apache/phoenix/parse/bson/BsonPath.java`
- Create: `phoenix-core/src/test/java/org/apache/phoenix/parse/bson/BsonPathTest.java`

- [ ] **Step 1: Write failing tests**

```java
package org.apache.phoenix.parse.bson;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import org.apache.phoenix.parse.bson.BsonPath.FieldSegment;
import org.apache.phoenix.parse.bson.BsonPath.IndexSegment;
import org.apache.phoenix.parse.bson.BsonPath.Segment;
import org.junit.Test;

public class BsonPathTest {

  @Test
  public void equalsIsStructural() {
    BsonPath a = new BsonPath(Arrays.<Segment>asList(new FieldSegment("a"), new FieldSegment("b")));
    BsonPath b = new BsonPath(Arrays.<Segment>asList(new FieldSegment("a"), new FieldSegment("b")));
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void differentSegmentTypesAreNotEqual() {
    BsonPath f = new BsonPath(Arrays.<Segment>asList(new FieldSegment("0")));
    BsonPath i = new BsonPath(Arrays.<Segment>asList(new IndexSegment(0)));
    assertNotEquals(f, i);
  }

  @Test
  public void canonicalToStringForSimpleDotPath() {
    BsonPath p = new BsonPath(Arrays.<Segment>asList(new FieldSegment("a"), new FieldSegment("b")));
    assertEquals("$.a.b", p.toString());
  }

  @Test
  public void canonicalToStringEscapesQuotedSegment() {
    BsonPath p = new BsonPath(Arrays.<Segment>asList(new FieldSegment("weird key")));
    assertEquals("$['weird key']", p.toString());
  }

  @Test
  public void canonicalToStringMixesArrayIndex() {
    BsonPath p = new BsonPath(Arrays.<Segment>asList(
        new FieldSegment("a"), new IndexSegment(3), new FieldSegment("b")));
    assertEquals("$.a[3].b", p.toString());
  }

  @Test
  public void quotedSegmentEscapesSingleQuoteAndBackslash() {
    BsonPath p = new BsonPath(Arrays.<Segment>asList(new FieldSegment("it's \\ tricky")));
    assertTrue(p.toString().contains("['it\\'s \\\\ tricky']"));
  }
}
```

- [ ] **Step 2: Run, expect compile failures**

```
mvn -pl phoenix-core -am -Dtest=BsonPathTest test
```

Expected: compile errors (`BsonPath` not found).

- [ ] **Step 3: Implement `BsonPath` and segment classes**

```java
package org.apache.phoenix.parse.bson;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/** Immutable structural JSONPath value (subset). Created via {@link BsonPathParser}. */
public final class BsonPath {

  private static final Pattern UNQUOTED_FIELD = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  public abstract static class Segment {
    /** Append the canonical form of this segment to {@code out}. */
    abstract void appendCanonical(StringBuilder out);
  }

  public static final class FieldSegment extends Segment {
    private final String name;

    public FieldSegment(String name) {
      this.name = Objects.requireNonNull(name, "name");
    }

    public String name() {
      return name;
    }

    @Override
    void appendCanonical(StringBuilder out) {
      if (UNQUOTED_FIELD.matcher(name).matches()) {
        out.append('.').append(name);
      } else {
        out.append("['");
        for (int i = 0; i < name.length(); i++) {
          char c = name.charAt(i);
          if (c == '\\' || c == '\'') {
            out.append('\\');
          }
          out.append(c);
        }
        out.append("']");
      }
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof FieldSegment && ((FieldSegment) o).name.equals(name);
    }

    @Override
    public int hashCode() {
      return name.hashCode() * 31 + 1;
    }
  }

  public static final class IndexSegment extends Segment {
    private final int index;

    public IndexSegment(int index) {
      if (index < 0) {
        throw new IllegalArgumentException("index must be >= 0");
      }
      this.index = index;
    }

    public int index() {
      return index;
    }

    @Override
    void appendCanonical(StringBuilder out) {
      out.append('[').append(index).append(']');
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof IndexSegment && ((IndexSegment) o).index == index;
    }

    @Override
    public int hashCode() {
      return Integer.hashCode(index) * 31 + 2;
    }
  }

  private final List<Segment> segments;
  private final String canonical;

  public BsonPath(List<Segment> segments) {
    if (segments == null || segments.isEmpty()) {
      throw new IllegalArgumentException("segments must be non-empty");
    }
    this.segments = Collections.unmodifiableList(new ArrayList<>(segments));
    StringBuilder sb = new StringBuilder("$");
    for (Segment s : this.segments) {
      s.appendCanonical(sb);
    }
    this.canonical = sb.toString();
  }

  public List<Segment> segments() {
    return segments;
  }

  /** Canonical `$.a.b[0]['weird key']` form. */
  @Override
  public String toString() {
    return canonical;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof BsonPath && ((BsonPath) o).canonical.equals(canonical);
  }

  @Override
  public int hashCode() {
    return canonical.hashCode();
  }
}
```

- [ ] **Step 4: Run, expect pass**

```
mvn -pl phoenix-core -am -Dtest=BsonPathTest test
```

Expected: all `BsonPathTest` tests PASS.

- [ ] **Step 5: Commit**

```
git add phoenix-core-client/src/main/java/org/apache/phoenix/parse/bson/BsonPath.java \
        phoenix-core/src/test/java/org/apache/phoenix/parse/bson/BsonPathTest.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: add immutable BsonPath value type"
```

---

## Task 3: `BsonPathParser` happy-path tests + impl

**Files:**
- Create: `phoenix-core-client/src/main/java/org/apache/phoenix/parse/bson/BsonPathParser.java`
- Modify: `phoenix-core/src/test/java/org/apache/phoenix/parse/bson/BsonPathParserTest.java`

- [ ] **Step 1: Append happy-path tests**

```java
  // ----- positive cases -----

  @Test
  public void parsesSingleFieldDot() throws Exception {
    org.junit.Assert.assertEquals("$.a", BsonPathParser.parse("$.a").toString());
  }

  @Test
  public void parsesNestedDot() throws Exception {
    org.junit.Assert.assertEquals("$.a.b.c", BsonPathParser.parse("$.a.b.c").toString());
  }

  @Test
  public void parsesArrayIndex() throws Exception {
    org.junit.Assert.assertEquals("$.a[0]", BsonPathParser.parse("$.a[0]").toString());
    org.junit.Assert.assertEquals("$.a[10][3]", BsonPathParser.parse("$.a[10][3]").toString());
  }

  @Test
  public void parsesBracketedQuoted() throws Exception {
    org.junit.Assert.assertEquals("$['weird key']",
        BsonPathParser.parse("$['weird key']").toString());
    org.junit.Assert.assertEquals("$['weird key']",
        BsonPathParser.parse("$[\"weird key\"]").toString());
  }

  @Test
  public void parsesBareDotPath() throws Exception {
    org.junit.Assert.assertEquals("$.a.b", BsonPathParser.parse("a.b").toString());
  }

  @Test
  public void parsesBareSingleField() throws Exception {
    org.junit.Assert.assertEquals("$.a", BsonPathParser.parse("a").toString());
  }

  @Test
  public void parsesBareWithIndex() throws Exception {
    org.junit.Assert.assertEquals("$.a[0]", BsonPathParser.parse("a[0]").toString());
  }

  @Test
  public void parsesQuotedWithEscapes() throws Exception {
    BsonPath p = BsonPathParser.parse("$['it\\'s \\\\ tricky']");
    org.junit.Assert.assertEquals("$['it\\'s \\\\ tricky']", p.toString());
  }

  @Test
  public void parsesMixedSegmentTypes() throws Exception {
    org.junit.Assert.assertEquals("$.a[3].b['x y']",
        BsonPathParser.parse("$.a[3].b['x y']").toString());
  }
```

- [ ] **Step 2: Run, expect compile failures**

Expected: `BsonPathParser` not found.

- [ ] **Step 3: Implement parser**

```java
package org.apache.phoenix.parse.bson;

import java.util.ArrayList;
import java.util.List;
import org.apache.phoenix.parse.bson.BsonPath.FieldSegment;
import org.apache.phoenix.parse.bson.BsonPath.IndexSegment;
import org.apache.phoenix.parse.bson.BsonPath.Segment;

/**
 * Recursive-descent parser for the JSONPath subset used by Phoenix BSON path indexes.
 * Accepted forms: {@code $.a.b}, {@code $.a[0]}, {@code $['key']}, {@code $["key"]},
 * and the bare equivalents {@code a.b}, {@code a}, {@code a[0]}.
 * Rejects wildcards, filters, recursive descent, slices.
 */
public final class BsonPathParser {

  private final String input;
  private int pos;

  private BsonPathParser(String input) {
    this.input = input;
    this.pos = 0;
  }

  public static BsonPath parse(String input) throws BsonPathSyntaxException {
    if (input == null || input.isEmpty()) {
      throw new BsonPathSyntaxException("path must be non-empty", 0);
    }
    BsonPathParser p = new BsonPathParser(input);
    return p.parsePath();
  }

  private BsonPath parsePath() throws BsonPathSyntaxException {
    List<Segment> segments = new ArrayList<>();
    if (peek() == '$') {
      pos++;
      // After '$', either end (illegal — empty path), '.', or '['.
      if (pos == input.length()) {
        throw new BsonPathSyntaxException("path must have at least one segment after '$'", pos);
      }
    }
    boolean first = true;
    while (pos < input.length()) {
      char c = input.charAt(pos);
      if (c == '.') {
        pos++;
        if (pos < input.length() && input.charAt(pos) == '.') {
          throw new BsonPathSyntaxException("recursive descent ($..) is not supported", pos);
        }
        segments.add(parseDotField());
      } else if (c == '[') {
        segments.add(parseBracketSegment());
      } else if (first) {
        // Bare leading field, e.g. "a.b" or "a[0]".
        segments.add(parseDotField());
      } else {
        throw new BsonPathSyntaxException("unexpected char '" + c + "'", pos);
      }
      first = false;
    }
    if (segments.isEmpty()) {
      throw new BsonPathSyntaxException("path is empty", 0);
    }
    return new BsonPath(segments);
  }

  private FieldSegment parseDotField() throws BsonPathSyntaxException {
    int start = pos;
    if (pos == input.length()) {
      throw new BsonPathSyntaxException("expected field name", pos);
    }
    char c0 = input.charAt(pos);
    if (c0 == '*') {
      throw new BsonPathSyntaxException("wildcards are not supported", pos);
    }
    if (!isIdStart(c0)) {
      throw new BsonPathSyntaxException("invalid field name start '" + c0 + "'", pos);
    }
    pos++;
    while (pos < input.length() && isIdPart(input.charAt(pos))) {
      pos++;
    }
    return new FieldSegment(input.substring(start, pos));
  }

  private Segment parseBracketSegment() throws BsonPathSyntaxException {
    if (input.charAt(pos) != '[') {
      throw new BsonPathSyntaxException("expected '['", pos);
    }
    int openPos = pos;
    pos++;
    if (pos == input.length()) {
      throw new BsonPathSyntaxException("unterminated '['", openPos);
    }
    char first = input.charAt(pos);
    if (first == '*') {
      throw new BsonPathSyntaxException("wildcards are not supported", pos);
    }
    if (first == '?') {
      throw new BsonPathSyntaxException("filter expressions are not supported", pos);
    }
    Segment seg;
    if (first == '\'' || first == '"') {
      seg = parseQuotedSegment(first);
    } else if (first >= '0' && first <= '9') {
      seg = parseIndexSegment(openPos);
    } else {
      throw new BsonPathSyntaxException("expected quoted key or array index", pos);
    }
    if (pos >= input.length() || input.charAt(pos) != ']') {
      throw new BsonPathSyntaxException("expected ']'", pos);
    }
    pos++;
    return seg;
  }

  private FieldSegment parseQuotedSegment(char quote) throws BsonPathSyntaxException {
    pos++;
    StringBuilder sb = new StringBuilder();
    while (pos < input.length()) {
      char c = input.charAt(pos);
      if (c == '\\') {
        if (pos + 1 >= input.length()) {
          throw new BsonPathSyntaxException("dangling backslash in quoted segment", pos);
        }
        char esc = input.charAt(pos + 1);
        if (esc == '\\' || esc == quote) {
          sb.append(esc);
          pos += 2;
        } else {
          throw new BsonPathSyntaxException("invalid escape '\\" + esc + "'", pos);
        }
      } else if (c == quote) {
        pos++;
        return new FieldSegment(sb.toString());
      } else {
        sb.append(c);
        pos++;
      }
    }
    throw new BsonPathSyntaxException("unterminated quoted segment", pos);
  }

  private IndexSegment parseIndexSegment(int openPos) throws BsonPathSyntaxException {
    int start = pos;
    while (pos < input.length() && Character.isDigit(input.charAt(pos))) {
      pos++;
    }
    if (pos < input.length() && input.charAt(pos) == ':') {
      throw new BsonPathSyntaxException("array slice is not supported", pos);
    }
    int idx;
    try {
      idx = Integer.parseInt(input.substring(start, pos));
    } catch (NumberFormatException nfe) {
      throw new BsonPathSyntaxException("invalid array index", openPos);
    }
    return new IndexSegment(idx);
  }

  private char peek() {
    return pos < input.length() ? input.charAt(pos) : '\0';
  }

  private static boolean isIdStart(char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
  }

  private static boolean isIdPart(char c) {
    return isIdStart(c) || (c >= '0' && c <= '9');
  }
}
```

- [ ] **Step 4: Run, expect pass**

```
mvn -pl phoenix-core -am -Dtest=BsonPathParserTest test
```

Expected: all positive-case tests PASS.

- [ ] **Step 5: Commit**

```
git add phoenix-core-client/src/main/java/org/apache/phoenix/parse/bson/BsonPathParser.java \
        phoenix-core/src/test/java/org/apache/phoenix/parse/bson/BsonPathParserTest.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: add JSONPath-subset parser (happy path)"
```

---

## Task 4: Negative-path tests for parser

**Files:**
- Modify: `phoenix-core/src/test/java/org/apache/phoenix/parse/bson/BsonPathParserTest.java`

- [ ] **Step 1: Append negative tests**

```java
  // ----- negative cases -----

  private static void expectFail(String s) {
    try {
      BsonPathParser.parse(s);
      org.junit.Assert.fail("expected BsonPathSyntaxException for input: " + s);
    } catch (BsonPathSyntaxException ok) {
      // expected
    }
  }

  @Test public void rejectsEmpty() { expectFail(""); }
  @Test public void rejectsNullThrows() {
    try {
      BsonPathParser.parse(null);
      org.junit.Assert.fail("expected exception for null");
    } catch (BsonPathSyntaxException ok) {
      // expected
    }
  }
  @Test public void rejectsLeadingDot() { expectFail("."); }
  @Test public void rejectsTrailingDot() { expectFail("$.a."); }
  @Test public void rejectsBareLeadingDot() { expectFail(".a"); }
  @Test public void rejectsDoubleDot() { expectFail("$..a"); }
  @Test public void rejectsRecursiveDescent() { expectFail("$..b"); }
  @Test public void rejectsWildcardField() { expectFail("$.*"); }
  @Test public void rejectsWildcardBracket() { expectFail("$[*]"); }
  @Test public void rejectsFilter() { expectFail("$[?(@.x>1)]"); }
  @Test public void rejectsSlice() { expectFail("$[0:2]"); }
  @Test public void rejectsUnterminatedBracket() { expectFail("$.a["); }
  @Test public void rejectsUnterminatedQuoted() { expectFail("$['oops"); }
  @Test public void rejectsBadIdentifier() { expectFail("$.1bad"); }
  @Test public void rejectsLoneDollar() { expectFail("$"); }
  @Test public void rejectsTrailingChars() { expectFail("$.a junk"); }
  @Test public void rejectsNegativeIndexLooksLikeWildcard() { expectFail("$.a[-1]"); }
```

- [ ] **Step 2: Run; some may already pass, run them all anyway**

```
mvn -pl phoenix-core -am -Dtest=BsonPathParserTest test
```

Expected: `rejectsTrailingChars` may pass or fail depending on whether the parser ate the trailing
chars; `rejectsLoneDollar` should pass already; `rejectsNegativeIndexLooksLikeWildcard` likely
fails because `-` triggers an `expected ']'` after the digit-loop.

- [ ] **Step 3: Tighten parser to make all negatives pass**

Update `BsonPathParser.parsePath()` to handle the leading-bare case explicitly: reject `null`,
empty, leading `.`, etc. If any test from Step 2 fails, fix the parser to make it pass without
breaking earlier tests. Common fix: reject `pos < input.length()` after the main loop only when
input is consumed.

Specifically, before `parsePath()` returns, if `pos != input.length()`, raise:
`throw new BsonPathSyntaxException("unexpected trailing input", pos);`. But this is already
covered because the loop only exits when `pos == input.length()`. The `rejectsTrailingChars`
test is therefore sensitive to whitespace/space handling — your parser will hit ' ' inside
`parseDotField` because ' ' is not `isIdPart`, so the loop ends at the space. Then the outer
`while` loop sees ' ' which is not `.` or `[`, so it falls to the `else` branch and throws — already
correct.

For `rejectsNegativeIndexLooksLikeWildcard`, `parseBracketSegment` sees `-`, which isn't `*`, `?`,
quote, or digit. Falls into the final `else`, raising "expected quoted key or array index". Good.

If any test still fails, add the offending case to the parser's switch.

- [ ] **Step 4: Run; expect all PASS**

```
mvn -pl phoenix-core -am -Dtest=BsonPathParserTest test
```

- [ ] **Step 5: Commit**

```
git add phoenix-core-client/src/main/java/org/apache/phoenix/parse/bson/BsonPathParser.java \
        phoenix-core/src/test/java/org/apache/phoenix/parse/bson/BsonPathParserTest.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: parser rejects unsupported JSONPath features"
```

---

## Task 5: Fuzz test

**Files:**
- Modify: `phoenix-core/src/test/java/org/apache/phoenix/parse/bson/BsonPathParserTest.java`

- [ ] **Step 1: Append fuzz test**

```java
  @Test
  public void fuzzNoCrashes() {
    java.util.Random rng = new java.util.Random(0xCAFEBABEL);
    String alphabet = "$.[]'\"_abcXY0123456789* ?\\:";
    int n = 5000;
    int crashes = 0;
    for (int i = 0; i < n; i++) {
      int len = rng.nextInt(20);
      StringBuilder sb = new StringBuilder(len);
      for (int j = 0; j < len; j++) {
        sb.append(alphabet.charAt(rng.nextInt(alphabet.length())));
      }
      try {
        BsonPathParser.parse(sb.toString());
      } catch (BsonPathSyntaxException ok) {
        // expected for most random inputs
      } catch (RuntimeException re) {
        crashes++;
      }
    }
    org.junit.Assert.assertEquals("parser must reject only via BsonPathSyntaxException", 0,
        crashes);
  }
```

- [ ] **Step 2: Run; expect PASS**

```
mvn -pl phoenix-core -am -Dtest=BsonPathParserTest test
```

If a random input triggers a `RuntimeException` (e.g., `StringIndexOutOfBoundsException`), fix
the parser by making the failing branch raise `BsonPathSyntaxException` instead. Re-run.

- [ ] **Step 3: Commit**

```
git add phoenix-core/src/test/java/org/apache/phoenix/parse/bson/BsonPathParserTest.java \
        phoenix-core-client/src/main/java/org/apache/phoenix/parse/bson/BsonPathParser.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: parser fuzz test (5k random inputs, no crashes)"
```

---

## Task 6: Compile-clean verification of phoenix-core-client

- [ ] **Step 1: Build phoenix-core-client without tests, then run unit tests in phoenix-core for the parse.bson package**

```
mvn -pl phoenix-core-client -am -DskipTests install
mvn -pl phoenix-core -Dtest='BsonPath*Test' test
```

Expected: BUILD SUCCESS for both. All `BsonPathTest` and `BsonPathParserTest` tests pass. Zero
production callers exist yet (verify with `grep -r "BsonPath\b" phoenix-core-client/src/main/java |
grep -v "/parse/bson/"` — should return only imports inside the new package).

- [ ] **Step 2: Final commit (if anything was tweaked)** — otherwise skip.

---

## Local testing plan for Phase 0

| What | Command |
|---|---|
| Compile | `mvn -pl phoenix-core-client -am -DskipTests install` |
| Unit tests for `BsonPath` only | `mvn -pl phoenix-core -Dtest='BsonPath*Test' test` |
| All unit tests in phoenix-core (sanity) | `mvn -pl phoenix-core -DskipITs test` |
| Confirm zero production wiring | `grep -rl "import org.apache.phoenix.parse.bson" phoenix-core-client/src/main/java phoenix-core-server/src/main/java` should return only files inside `parse/bson/`. |

---

## Self-review checklist (run before declaring Phase 0 done)

- [ ] All 6 tasks committed in order.
- [ ] No file outside `phoenix-core-client/src/main/java/org/apache/phoenix/parse/bson/` and the new
      test files was modified.
- [ ] `BsonPath` is final, immutable, has structural `equals`/`hashCode`, and `toString` returns the
      canonical form.
- [ ] `BsonPathParser.parse(null)` and `parse("")` throw `BsonPathSyntaxException`.
- [ ] All rejected JSONPath features (wildcard, filter, recursive descent, slice) have tests.
- [ ] Fuzz test passes deterministically (seed pinned).
- [ ] Zero compile warnings introduced in modified packages.
