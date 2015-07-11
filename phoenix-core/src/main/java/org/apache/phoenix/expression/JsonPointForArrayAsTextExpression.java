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
import org.apache.phoenix.schema.types.PVarchar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JsonPointForArrayAsTextExpression extends BaseJSONExpression {

	private static final Logger logger = LoggerFactory.getLogger(JsonPointForArrayAsTextExpression.class);
	
	public JsonPointForArrayAsTextExpression(List<Expression> children) {
		super(children);
	}
	public JsonPointForArrayAsTextExpression() {
		
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		if (!children.get(0).evaluate(tuple, ptr)) {
            if (logger.isDebugEnabled()) {
                logger.debug("->> left value is null");
            }
            return false;
        }
		 PhoenixJson source = (PhoenixJson) PJson.INSTANCE.toObject(ptr, children.get(0).getSortOrder());
		if (!children.get(1).evaluate(tuple, ptr)) {
            if (logger.isDebugEnabled()) {
                logger.debug("->> right value is null");
            }
            return false;
        }
		String key = (String) PVarchar.INSTANCE.toObject(ptr, children.get(1).getSortOrder());
		PhoenixJson jsonValue=source.getValue(key);
		if(jsonValue!=null)
		{
			ptr.set(jsonValue.toBytes());
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
		return PVarchar.INSTANCE;
	}

}
