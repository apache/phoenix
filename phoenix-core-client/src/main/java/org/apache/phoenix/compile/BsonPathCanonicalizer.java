package org.apache.phoenix.compile;

import java.sql.SQLException;
import java.util.ArrayList;
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

/**
 * Rewrites BSON-path expression parse nodes into a single canonical form so DDL and predicate
 * forms can be compared for equivalence. Pure function; reads no schema state.
 */
public final class BsonPathCanonicalizer {

  private static final ParseNodeFactory FACTORY = new ParseNodeFactory();
  private static final String BSON_VALUE_NAME = BsonValueFunction.NAME; // "BSON_VALUE"
  private static final int BSON_VALUE_INDEXABLE_ARITY = 3;

  private BsonPathCanonicalizer() {}

  /**
   * Returns a {@link ParseNode} structurally equivalent to {@code node} but with all recognized
   * BSON-path expressions rewritten to canonical form. If no rewrite applies, returns
   * {@code node} unchanged.
   */
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
