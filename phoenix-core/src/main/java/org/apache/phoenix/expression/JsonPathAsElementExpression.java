package org.apache.phoenix.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.tuple.Tuple;

import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PJson;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.JSONutil;
import org.apache.hadoop.hbase.util.Bytes;

import com.fasterxml.jackson.databind.JsonNode;

public class JsonPathAsElementExpression extends BaseCompoundExpression{
	public JsonPathAsElementExpression(List<Expression> children) {
        super(children);
    }
	public JsonPathAsElementExpression() {
    }
	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr)  {
		if (!getPatternExpression().evaluate(tuple, ptr)) {
            return false;
        }
		String[] pattern =decodePath((String) PVarchar.INSTANCE.toObject(ptr, getPatternExpression().getSortOrder()));
		if (!getStrExpression().evaluate(tuple, ptr)) {
	        return false;
	    }
		String value = (String) PVarchar.INSTANCE.toObject(ptr, getStrExpression().getSortOrder());
		JSONutil util=new JSONutil();
		try{
			JsonNode node=util.gerateJsonTree(value);
			for(int i=0;i<pattern.length;i++){
				node=util.enterJsonTreeNode(node,pattern[i]);
			}
			if(node!=null){
				if(node.isInt()){
					ptr.set(Bytes.toBytes(node.asInt()));
				}
				else if(node.isBoolean()){
					ptr.set(Bytes.toBytes(node.asBoolean()));
				}
				else{
					ptr.set(Bytes.toBytes(node.asText()));
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
	private Expression getStrExpression() {
        return children.get(0);
    }
	private Expression getPatternExpression() {
        return children.get(1);
	}
	private String[] decodePath(String path)
	{
		String data=path.substring(1, path.length()-1);
		return data.split(",");
	}
	@Override
	public <T> T accept(ExpressionVisitor<T> visitor) 
	{
		/*
		 List<T> l = acceptChildren(visitor, visitor.visitEnter(this));
	        T t = visitor.visitLeave(this, l);
	        if (t == null) {
	            t = visitor.defaultReturn(this, l);
	        }
	        */
	        return null;
	}
	@Override
	public PDataType getDataType() {
		 return PJson.INSTANCE;
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
