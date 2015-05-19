package org.apache.phoenix.expression;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.JSONutil;

public class JsonSupersetExpression extends BaseJSONExpression{
	public JsonSupersetExpression(List<Expression> children) {
        super(children);
    }
	public JsonSupersetExpression() {
        
    }
	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		if (!children.get(1).evaluate(tuple, ptr)) {
            return false;
        }
		String pattern = (String) PVarchar.INSTANCE.toObject(ptr);
		if (!children.get(0).evaluate(tuple, ptr)) {
	        return false;
	    }
		String value = (String) PVarchar.INSTANCE.toObject(ptr);
		//null col
		if(value.equals("")){
			ptr.set(PDataType.FALSE_BYTES);
			return true;
		}
		JSONutil util=new JSONutil();
		try{
		Map<String, Object> patternmap=util.getStringMap(pattern);
		Map<String, Object> valuemap=util.getStringMap(value);
		//empty set
		if(valuemap.size()==0){
			ptr.set(PDataType.TRUE_BYTES);
			return true;
		}
		Set<String> key = valuemap.keySet();
		Iterator<String> iter = key.iterator();
		Object o=null;
	    while (iter.hasNext()) {
	    	String s=iter.next();
	    	if((o=patternmap.get(s))==null||!(o.equals(valuemap.get(s)))){
	    		ptr.set(PDataType.FALSE_BYTES);
	    		return true;
	    	}
	    }
	    	ptr.set(PDataType.TRUE_BYTES);
		}
		catch(IOException e){
			
		}
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
