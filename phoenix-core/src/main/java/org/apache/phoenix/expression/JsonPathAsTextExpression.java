package org.apache.phoenix.expression;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.json.PhoenixJson;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PJson;
import org.apache.phoenix.schema.types.PVarchar;


public class JsonPathAsTextExpression  extends BaseJSONExpression{
	public JsonPathAsTextExpression(List<Expression> children) {
        super(children);
    }
	public JsonPathAsTextExpression() {
    }
	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr)  {
		if (!children.get(1).evaluate(tuple, ptr)) {
            return false;
        }
		String[] pattern =decodePath((String) PVarchar.INSTANCE.toObject(ptr));
		if (!children.get(0).evaluate(tuple, ptr)) {
	        return false;
	    }
		PhoenixJson value = (PhoenixJson) PJson.INSTANCE.toObject(ptr, children.get(0).getSortOrder());
		try{
				PhoenixJson jsonValue=value.getPhoenixJson(pattern);
				ptr.set(jsonValue.toBytes());
				return true;
		}catch(SQLException e)
		{
			return false;
		}
		
	}
	private String[] decodePath(String path)
	{
		String data=path.substring(1, path.length()-1);
		return data.split(",");
	}
	@Override
	public <T> T accept(ExpressionVisitor<T> visitor) 
	{
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
	@Override
	public PDataType getRealDataType(){
		 return PVarchar.INSTANCE;
	}
}
