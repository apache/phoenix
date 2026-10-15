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
package org.apache.phoenix.parse;

import java.util.List;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.schema.types.PInteger;

/**
 * Node representing an ORDER BY clause (including asc/desc and nulls first/last) in SQL
 * @since 0.1
 */
public final class OrderByNode {
  private final ParseNode child;
  private final boolean nullsLast;
  private final boolean orderAscending;
  // True if the item has no NULLS FIRST or NULLS LAST clause.
  private final boolean nullsDefault;

  OrderByNode(ParseNode child, boolean nullsLast, boolean orderAscending) {
    this(child, nullsLast, orderAscending, false);
  }

  OrderByNode(ParseNode child, boolean orderAscending) {
    this(child, false, orderAscending, true);
  }

  private OrderByNode(ParseNode child, boolean nullsLast, boolean orderAscending,
    boolean nullsDefault) {
    this.child = child;
    this.nullsLast = nullsLast;
    this.orderAscending = orderAscending;
    this.nullsDefault = nullsDefault;
  }

  /**
   * Returns true if nulls sort last. Without a NULLS clause, an ascending vector distance sorts
   * nulls last, because a nearest neighbor search ranks rows without a distance after all scored
   * rows. All other items without a NULLS clause sort nulls first.
   */
  public boolean isNullsLast() {
    return nullsLast
      || nullsDefault && orderAscending && child instanceof DistanceFunctionParseNode;
  }

  /**
   * Returns true if the item has no NULLS FIRST or NULLS LAST clause. Compilation uses this to
   * apply the distance default to an item, such as an ordinal, that only compilation resolves to a
   * vector distance.
   */
  public boolean isNullsDefault() {
    return nullsDefault;
  }

  public boolean isAscending() {
    return orderAscending;
  }

  public ParseNode getNode() {
    return child;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((child == null) ? 0 : child.hashCode());
    result = prime * result + (isNullsLast() ? 1231 : 1237);
    result = prime * result + (orderAscending ? 1231 : 1237);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    OrderByNode other = (OrderByNode) obj;
    if (child == null) {
      if (other.child != null) return false;
    } else if (!child.equals(other.child)) return false;
    if (isNullsLast() != other.isNullsLast()) return false;
    if (orderAscending != other.orderAscending) return false;
    return true;
  }

  @Override
  public String toString() {
    return child.toString() + (orderAscending ? " asc" : " desc") + " nulls "
      + (isNullsLast() ? "last" : "first");
  }

  public void toSQL(ColumnResolver resolver, StringBuilder buf) {
    child.toSQL(resolver, buf);
    if (!orderAscending) buf.append(" DESC");
    if (isNullsLast()) buf.append(" NULLS LAST ");
    else if (!nullsDefault && orderAscending && child instanceof DistanceFunctionParseNode)
      buf.append(" NULLS FIRST ");
  }

  public boolean isIntegerLiteral() {
    return child instanceof LiteralParseNode
      && ((LiteralParseNode) child).getType() == PInteger.INSTANCE;
  }

  public Integer getValueIfIntegerLiteral() {
    if (!isIntegerLiteral()) {
      return null;
    }
    return (Integer) ((LiteralParseNode) child).getValue();
  }

  /**
   * Returns the projected node that an ordinal item refers to. Returns null if the item is not a
   * valid ordinal. Also returns null if a wildcard comes before that position, because the position
   * is then not known until compilation.
   */
  public ParseNode getOrdinalSelectNode(List<AliasedNode> select) {
    Integer ordinal = getValueIfIntegerLiteral();
    if (ordinal == null || ordinal < 1 || ordinal > select.size()) {
      return null;
    }
    for (int i = 0; i < ordinal - 1; i++) {
      ParseNode node = select.get(i).getNode();
      if (
        node instanceof WildcardParseNode || node instanceof TableWildcardParseNode
          || node instanceof FamilyWildcardParseNode
      ) {
        return null;
      }
    }
    return select.get(ordinal - 1).getNode();
  }
}
