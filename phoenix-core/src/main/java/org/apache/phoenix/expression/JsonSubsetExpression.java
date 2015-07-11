package org.apache.phoenix.expression;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.json.PhoenixJson;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PJson;

public class JsonSubsetExpression extends BaseCompoundExpression{
	public JsonSubsetExpression(List<Expression> children) {
        super(children);
    }
	public JsonSubsetExpression() {  
    }
	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		if (!children.get(1).evaluate(tuple, ptr)) {
            return false;
        }
		PhoenixJson pattern = (PhoenixJson) PJson.INSTANCE.toObject(ptr, children.get(1).getSortOrder());
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
		//null value
		if(value==null){
			ptr.set(PDataType.FALSE_BYTES);
			return true;
		}
		//empty set
		if(pattern.getNodeSize()==0){
			ptr.set(PDataType.TRUE_BYTES);
			return true;
		}
		if(pattern.getNodeSize()>value.getNodeSize()){
			ptr.set(PDataType.FALSE_BYTES);
			return true;
		}
		Iterator<String> fildnames=pattern.getFieldNames();
		while(fildnames.hasNext())
		{
			String rhsKey=fildnames.next();
			if(!value.hasKey(rhsKey)||!value.getValue(rhsKey).getValueAsString().equals(pattern.getValue(rhsKey).getValueAsString())){
				ptr.set(PDataType.FALSE_BYTES);
				return true;
			}
		}
		ptr.set(PDataType.TRUE_BYTES);
		return true;
	}
	@Override
	public <T> T accept(ExpressionVisitor<T> visitor) {
		 
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
