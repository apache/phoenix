package org.apache.phoenix.parse;

import java.sql.SQLException;


public class SequenceValueParseNode extends TerminalParseNode {
    public enum Op {NEXT_VALUE, CURRENT_VALUE};
	private final TableName tableName;
	private final Op op;

	public SequenceValueParseNode(TableName tableName, Op op) {
		this.tableName = tableName;
		this.op = op;
	}

	@Override
	public <T> T accept(ParseNodeVisitor<T> visitor) throws SQLException {
		return visitor.visit(this);
	}

	public TableName getTableName() {
		return tableName;
	}

    @Override
    public boolean isStateless() {
        return true;
    }

    public Op getOp() {
        return op;
    }
}