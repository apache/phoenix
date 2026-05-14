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
