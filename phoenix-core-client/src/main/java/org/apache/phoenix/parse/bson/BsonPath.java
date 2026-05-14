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
