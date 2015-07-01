package org.apache.phoenix.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.json.PhoenixJson;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonPointForArrayAsElementExpression extends BaseJSONExpression{

	private static final Logger logger = LoggerFactory.getLogger(JsonPointForArrayAsElementExpression.class);
	
	public JsonPointForArrayAsElementExpression(List<Expression> children)
	{
		super(children);
	}
	public JsonPointForArrayAsElementExpression()
	{
		
	}
	private static PDataType defaultType=PJson.INSTANCE;
	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		if (!children.get(0).evaluate(tuple, ptr)) {
            if (logger.isDebugEnabled()) {
                logger.debug("-> left value is null");
            }
            return false;
        }
		PhoenixJson source = (PhoenixJson) PJson.INSTANCE.toObject(ptr, children.get(0).getSortOrder());
		if (!children.get(1).evaluate(tuple, ptr)) {
            if (logger.isDebugEnabled()) {
                logger.debug("-> right value is null");
            }
            return false;
        }
		int key = children.get(1).getDataType().getCodec().decodeInt(ptr, children.get(1).getSortOrder());
		PhoenixJson jsonValue=source.getValue(key);
		if(jsonValue!=null)
		{
			defaultType=jsonValue.getValueAsPDataType();
			ptr.set(jsonValue.valueWrapToBytes());
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
	    public void readFields(DataInput input) throws IOException {
	        super.readFields(input);
	    }

	    @Override
	    public void write(DataOutput output) throws IOException {
	        super.write(output);
	    }
	    
		@Override
		public PDataType getDataType() {
			return PJson.INSTANCE;
		}
		
		@Override
		public PDataType getRealDataType(){
			 return defaultType;
		}
}
