package org.apache.phoenix.expression;

import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;

public abstract class BaseJSONExpression extends BaseCompoundExpression {
	public BaseJSONExpression(List<Expression> children) {
        super(children);
    }
	public BaseJSONExpression() {
    }
	public abstract boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr);
	public abstract <T> T accept(ExpressionVisitor<T> visitor);
	public abstract PDataType getDataType();
	public PDataType getRealDataType(){
		return null;
	}
}
