package org.apache.phoenix.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PJson;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.JSONutil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonPointAsElementExpression extends BaseJSONExpression{

	private static final Logger logger = LoggerFactory.getLogger(JsonPointAsElementExpression.class);
	
	public JsonPointAsElementExpression(List<Expression> children)
	{
		super(children);
	}
	public JsonPointAsElementExpression()
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
		String source = (String) PJson.INSTANCE.toObject(ptr, children.get(0).getSortOrder());
		if (!children.get(1).evaluate(tuple, ptr)) {
            if (logger.isDebugEnabled()) {
                logger.debug("-> right value is null");
            }
            return false;
        }
		String key = (String) PVarchar.INSTANCE.toObject(ptr, children.get(1).getSortOrder());
		try {
			JSONutil jsonUtil=new JSONutil();
			JsonNode jsonTree=jsonUtil.getJsonNode(source);
				if(jsonTree.has(key))
				{
					JsonNode result=jsonTree.get(key);
					if(result.isInt())
					{
						defaultType=PInteger.INSTANCE;
						ptr.set(PInteger.INSTANCE.toBytes(result.asInt(), SortOrder.getDefault()));
						return true;
					}
					else if(result.isLong())
					{
						defaultType=PLong.INSTANCE;
						ptr.set(PLong.INSTANCE.toBytes(result.asLong(), SortOrder.getDefault()));
						return true;
					}
					else if(result.isDouble())
					{
						defaultType=PDouble.INSTANCE;
						ptr.set(PDouble.INSTANCE.toBytes(result.asDouble(), SortOrder.getDefault()));
						return true;
					}
					else if(result.isBoolean())
					{
						defaultType=PBoolean.INSTANCE;
						ptr.set(Bytes.toBytes(result.asBoolean()));
						return true;
					}
					else if(result.isTextual())
					{
						defaultType=PVarchar.INSTANCE;
						ptr.set(PVarchar.INSTANCE.toBytes(result.asText(), SortOrder.getDefault()));
						return true;
					}
					else
					{
						//use byte string 
						ptr.set(PVarchar.INSTANCE.toBytes(result.toString(), SortOrder.getDefault()));
						return true;
					}
				}
				else
				{
					return false;
				}
		} catch (JsonParseException e) {
			e.printStackTrace();
			return false;
		} catch (JsonMappingException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
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
