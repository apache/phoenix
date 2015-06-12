package org.apache.phoenix.expression;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PJson;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.JSONutil;
import org.apache.hadoop.hbase.util.Bytes;

import com.fasterxml.jackson.databind.JsonNode;

public class JsonPathAsElementExpression extends BaseJSONExpression{
	private PDataType datatype=null;
	public JsonPathAsElementExpression(List<Expression> children) {
        super(children);
    }
	public JsonPathAsElementExpression() {
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
		String value = (String) PVarchar.INSTANCE.toObject(ptr);
		JSONutil util=new JSONutil();
		try{
			JsonNode node=util.getJsonNode(value);
			for(int i=0;i<pattern.length;i++){
				if(node.isValueNode()){
					ptr.set(PDataType.NULL_BYTES);
					return false;
				}
				else if(node.isArray()){
					//determine path value whether it is a int
					if(pattern[i].matches("\\d+")){
						node=util.enterJsonNodeArray(node,Integer.valueOf(pattern[i]));
					}
					else{
						ptr.set(PDataType.NULL_BYTES);
						return false;
					}
				}
				else{
					node=util.enterJsonTreeNode(node,pattern[i]);
				}
			}
			if(node!=null){
				if(node.isInt()){
					datatype=PInteger.INSTANCE;
					ptr.set(PInteger.INSTANCE.toBytes(node.intValue(), SortOrder.getDefault()));
				}
				else if(node.isBoolean()){
					datatype=PBoolean.INSTANCE;
					ptr.set(Bytes.toBytes(node.asBoolean()));
				}
				else if(node.isDouble()){
					datatype=PDouble.INSTANCE;
					ptr.set(PDouble.INSTANCE.toBytes(node.asDouble(),SortOrder.getDefault()));
				}
				else{
					datatype=PVarchar.INSTANCE;
					ptr.set(PVarchar.INSTANCE.toBytes(node.asText(),SortOrder.getDefault()));
				}
			}
			else{
				ptr.set(PDataType.NULL_BYTES);
			}
		}
		catch(IOException e){
			e.printStackTrace();
		}
        return true;
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
		 return PJson.INSTANCE;
	}
	@Override
	public PDataType getRealDataType(){
		 return datatype;
	}
}
