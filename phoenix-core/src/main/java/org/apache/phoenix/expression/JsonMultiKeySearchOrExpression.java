/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.expression;

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

public class JsonMultiKeySearchOrExpression extends BaseCompoundExpression{
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
		if(children.get(0).getDataType()!=PJson.INSTANCE)
		{
			ptr.set(PDataType.FALSE_BYTES);
			return true;
		}
		if (!children.get(0).evaluate(tuple, ptr)) {
	        return false;
	    }
		PhoenixJson value = (PhoenixJson) PJson.INSTANCE.toObject(ptr);
			for(int i=0;i<pattern.getDimensions();i++){
				if(value.hasKey((String)pattern.getElement(i)))
						{
							ptr.set(PDataType.TRUE_BYTES);
							return true;
						}
			}
		ptr.set(PDataType.FALSE_BYTES);
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
