package org.apache.phoenix.parse;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.phoenix.compile.ColumnResolver;

public class JsonSubsetParseNode extends BinaryParseNode {

	JsonSubsetParseNode(ParseNode lhs, ParseNode rhs) {
		super(lhs, rhs);
		// TODO Auto-generated constructor stub
	}

	@Override
    public <T> T accept(ParseNodeVisitor<T> visitor) throws SQLException {
        List<T> l = Collections.emptyList();
        if (visitor.visitEnter(this)) {
            l = acceptChildren(visitor);
        }
        return visitor.visitLeave(this, l);
    }

	@Override
	public void toSQL(ColumnResolver resolver, StringBuilder buf) {
		List<ParseNode> children = getChildren();
        children.get(0).toSQL(resolver, buf);
        buf.append(" <@ ");
        children.get(1).toSQL(resolver, buf);
	}

}
