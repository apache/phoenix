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
import org.apache.phoenix.schema.types.PVarcharArray;
import org.apache.phoenix.schema.types.PhoenixArray;


public class JsonMultiKeySearchAndExpression extends BaseJSONExpression{
	public JsonMultiKeySearchAndExpression(List<Expression> children) {
        super(children);
    }
	public JsonMultiKeySearchAndExpression() {
    }
	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr)  {
		if (!children.get(1).evaluate(tuple, ptr)) {
            return false;
        }
		PhoenixArray pattern =(PhoenixArray)PVarcharArray.INSTANCE.toObject(ptr);
		if (!children.get(0).evaluate(tuple, ptr)) {
	        return false;
	    }
		PhoenixJson value = (PhoenixJson) PJson.INSTANCE.toObject(ptr, children.get(0).getSortOrder());
			for(int i=0;i<pattern.getDimensions();i++){
				if(!value.hasKey((String)pattern.getElement(i)))
						{
							ptr.set(PDataType.FALSE_BYTES);
							return true;
						}
			}
		ptr.set(PDataType.TRUE_BYTES);
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
