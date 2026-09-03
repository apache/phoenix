/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.compile.keyspace.algebra;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Abstract WHERE-expression tree over PK columns. Three node kinds:
 * <ul>
 * <li>{@link Pred} — leaf predicate: {@code (dim, op, value)}. Only PK-column predicates
 * are modeled; non-PK predicates reach the oracle as {@code TRUE} leaves or simply aren't
 * part of the input.</li>
 * <li>{@link And} — children are AND'd.</li>
 * <li>{@link Or} — children are OR'd.</li>
 * </ul>
 * This is intentionally narrower than Phoenix's {@code Expression} hierarchy. The oracle's
 * job is the key-range extraction algorithm, not expression normalization — so RVC
 * inequalities should be lex-expanded by the caller (or by a small utility) before being
 * handed to the oracle, matching how production's {@code ExpressionNormalizer} behaves.
 */
public abstract class AbstractExpression {

  public abstract boolean evaluate(List<Object> row);

  public static Pred pred(int dim, Op op, Comparable<?> value) {
    return new Pred(dim, op, value);
  }

  public static AbstractExpression and(AbstractExpression... children) {
    return And.of(Arrays.asList(children));
  }

  public static AbstractExpression or(AbstractExpression... children) {
    return Or.of(Arrays.asList(children));
  }

  public static AbstractExpression unknown(String reason) {
    return new Unknown(reason);
  }

  /** Comparison operators on a PK-column leaf. NOT_EQUAL is deliberately omitted — not keyable. */
  public enum Op { EQ, LT, LE, GT, GE }

  /**
   * A leaf we can't analyze precisely (non-PK predicate, scalar function, NOT_EQUAL, etc).
   * Treated as a sound over-approximation: {@code evaluate} returns {@code true} for every
   * row, and the oracle's translation step maps {@code Unknown} to {@code everything(n)} —
   * i.e. an Unknown contributes no narrowing to the scan range.
   * <p>
   * Why over-approximation is safe: soundness check is
   * {@code rows(expr) ⊆ rows(emit)}. If Unknown is treated as {@code true}, we're replacing
   * the real predicate {@code P} with {@code true}, which widens {@code rows(expr)}. That
   * widening matters when we assert {@code rows(expr) ⊆ rows(V2.emit)} — a larger
   * {@code rows(expr)} makes the soundness check stricter on V2, not looser. So Unknown
   * handling is a safe over-approximation for finding V2 bugs: if V2 drops a predicate to
   * its residual filter, the oracle (via Unknown) also treats it as "all rows match," and
   * they agree. If V2 wrongly narrows based on something it can't actually enforce, the
   * oracle will catch that because the oracle's wider view includes rows V2 excluded.
   */
  public static final class Unknown extends AbstractExpression {
    public final String reason;

    Unknown(String reason) {
      this.reason = reason;
    }

    @Override
    public boolean evaluate(List<Object> row) {
      return true;
    }

    @Override
    public String toString() {
      return "UNKNOWN(" + reason + ")";
    }
  }

  /** {@code dim <op> value}. */
  public static final class Pred extends AbstractExpression {
    public final int dim;
    public final Op op;
    public final Comparable<?> value;

    Pred(int dim, Op op, Comparable<?> value) {
      this.dim = dim;
      this.op = op;
      this.value = Objects.requireNonNull(value);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public boolean evaluate(List<Object> row) {
      Object lhs = row.get(dim);
      if (lhs == null) return false;
      int c = ((Comparable) lhs).compareTo(value);
      switch (op) {
        case EQ: return c == 0;
        case LT: return c < 0;
        case LE: return c <= 0;
        case GT: return c > 0;
        case GE: return c >= 0;
        default: throw new IllegalStateException();
      }
    }

    @Override
    public String toString() {
      return "d" + dim + " " + op + " " + value;
    }
  }

  public static final class And extends AbstractExpression {
    public final List<AbstractExpression> children;

    private And(List<AbstractExpression> children) {
      this.children = Collections.unmodifiableList(children);
    }

    public static AbstractExpression of(List<AbstractExpression> children) {
      if (children.isEmpty()) {
        throw new IllegalArgumentException("AND of nothing is not allowed");
      }
      if (children.size() == 1) return children.get(0);
      return new And(children);
    }

    @Override
    public boolean evaluate(List<Object> row) {
      for (AbstractExpression c : children) {
        if (!c.evaluate(row)) return false;
      }
      return true;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("(");
      for (int i = 0; i < children.size(); i++) {
        if (i > 0) sb.append(" AND ");
        sb.append(children.get(i));
      }
      return sb.append(')').toString();
    }
  }

  public static final class Or extends AbstractExpression {
    public final List<AbstractExpression> children;

    private Or(List<AbstractExpression> children) {
      this.children = Collections.unmodifiableList(children);
    }

    public static AbstractExpression of(List<AbstractExpression> children) {
      if (children.isEmpty()) {
        throw new IllegalArgumentException("OR of nothing is not allowed");
      }
      if (children.size() == 1) return children.get(0);
      return new Or(children);
    }

    @Override
    public boolean evaluate(List<Object> row) {
      for (AbstractExpression c : children) {
        if (c.evaluate(row)) return true;
      }
      return false;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("(");
      for (int i = 0; i < children.size(); i++) {
        if (i > 0) sb.append(" OR ");
        sb.append(children.get(i));
      }
      return sb.append(')').toString();
    }
  }
}
