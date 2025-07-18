package org.apache.phoenix.parse;

import org.apache.phoenix.compile.ColumnResolver;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * Parse Node to help determine whether the document field is of a given type.
 */
public class DocumentFieldTypeParseNode extends CompoundParseNode {

    DocumentFieldTypeParseNode(ParseNode fieldKey, ParseNode value) {
        super(Arrays.asList(fieldKey, value));
    }

    @Override
    public <T> T accept(ParseNodeVisitor<T> visitor) throws SQLException {
        List<T> l = java.util.Collections.emptyList();
        if (visitor.visitEnter(this)) {
            l = acceptChildren(visitor);
        }
        return visitor.visitLeave(this, l);
    }

    @Override
    public void toSQL(ColumnResolver resolver, StringBuilder buf) {
        List<ParseNode> children = getChildren();
        buf.append("field_type(");
        children.get(0).toSQL(resolver, buf);
        buf.append(", ");
        children.get(1).toSQL(resolver, buf);
        buf.append(")");
    }

    public ParseNode getFieldKey() {
        return getChildren().get(0);
    }

    public ParseNode getValue() {
        return getChildren().get(1);
    }
}
