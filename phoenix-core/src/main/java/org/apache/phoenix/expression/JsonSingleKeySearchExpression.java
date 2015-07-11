package org.apache.phoenix.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.json.PhoenixJson;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PJson;
import org.apache.phoenix.schema.types.PVarchar;

public class JsonSingleKeySearchExpression extends BaseCompoundExpression {
	
	public JsonSingleKeySearchExpression(List<Expression> children) {
        super(children);
    }
	public JsonSingleKeySearchExpression() {
    }
	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr)  {
		if (!children.get(1).evaluate(tuple, ptr)) {
            return false;
        }
		String pattern = (String) PVarchar.INSTANCE.toObject(ptr);
		if(children.get(0) instanceof BaseJSONExpression){
			if(((BaseJSONExpression)children.get(0)).getRealDataType()!=PJson.INSTANCE)
			{
				ptr.set(PDataType.FALSE_BYTES);
				return true;
			}
		}
		if (!children.get(0).evaluate(tuple, ptr)) {
	        return false;
	    }
		PhoenixJson value = (PhoenixJson) PJson.INSTANCE.toObject(ptr, children.get(0).getSortOrder());
		ptr.set(value.getValue(pattern)!=null? PDataType.TRUE_BYTES : PDataType.FALSE_BYTES);
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
	@Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
    }
	@Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
    }
}
