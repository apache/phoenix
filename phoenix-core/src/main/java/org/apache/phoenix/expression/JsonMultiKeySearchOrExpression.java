package org.apache.phoenix.expression;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.phoenix.util.JSONutil;

public class JsonMultiKeySearchOrExpression extends BaseJSONExpression{
	public JsonMultiKeySearchOrExpression(List<Expression> children) {
        super(children);
    }
	public JsonMultiKeySearchOrExpression() {
    }
	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr){
		if (!children.get(1).evaluate(tuple, ptr)) {
            return false;
        }
		PhoenixArray pattern =(PhoenixArray)PVarcharArray.INSTANCE.toObject(ptr);
		if (!children.get(0).evaluate(tuple, ptr)) {
	        return false;
	    }
		String value = (String) PVarchar.INSTANCE.toObject(ptr);
		try
		{
			JSONutil util=new JSONutil();
			Object o=null;
			for(int i=0;i<pattern.getDimensions();i++){
				if((o=util.mapJSON(pattern.getElement(i), value))!=null){
					ptr.set(PDataType.TRUE_BYTES);
					break;
				}
			}
			if(o==null) ptr.set(PDataType.FALSE_BYTES);
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
        return true;
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
		 return PBoolean.INSTANCE;
	}
}
