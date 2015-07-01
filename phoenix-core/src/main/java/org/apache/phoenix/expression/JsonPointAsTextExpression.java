package org.apache.phoenix.expression;

import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.json.PhoenixJson;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PJson;
import org.apache.phoenix.schema.types.PVarchar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JsonPointAsTextExpression extends BaseCompoundExpression{

	private static final Logger logger = LoggerFactory.getLogger(JsonPointAsTextExpression.class);
	
	public JsonPointAsTextExpression(List<Expression> children){
		super(children);
	}
	public JsonPointAsTextExpression(){
	}
	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		if (!children.get(0).evaluate(tuple, ptr)) {
            if (logger.isDebugEnabled()) {
                logger.debug("->> left value is null");
            }
            return false;
        }
		 PhoenixJson source =(PhoenixJson) PJson.INSTANCE.toObject(ptr, children.get(0).getSortOrder());
		if (!children.get(1).evaluate(tuple, ptr)) {
            if (logger.isDebugEnabled()) {
                logger.debug("->> right value is null");
            }
            return false;
        }
		String key = (String) PVarchar.INSTANCE.toObject(ptr, children.get(1).getSortOrder());
		PhoenixJson jsonValue=source.getValue(key);
		if(jsonValue!=null)
		{
			ptr.set(jsonValue.toBytes());
			return true;
		}
		return false;
	}
	@Override
	public final <T> T accept(ExpressionVisitor<T> visitor) {
		List<T> l = acceptChildren(visitor, visitor.visitEnter(this));
	    T t = visitor.visitLeave(this, l);
	    if (t == null) {
	    	t = visitor.defaultReturn(this, l);
	    }
	    return t;
	}
	@Override
	public PDataType getDataType() {
		return PVarchar.INSTANCE;
	}
}
