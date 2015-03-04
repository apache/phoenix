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
import java.util.Collection;
import java.util.Collections;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.SchemaUtil;

public class ProjectedColumnExpression extends ColumnExpression {
	private KeyValueSchema schema;
	ValueBitSet bitSet;
	private int position;
	private String displayName;
	private final Collection<PColumn> columns;
	
	public ProjectedColumnExpression() {
        this.columns = Collections.emptyList();
	}

	public ProjectedColumnExpression(PColumn column, PTable table, String displayName) {
		this(column, table.getColumns(), column.getPosition() - table.getPKColumns().size(), displayName);
	}
    
    public ProjectedColumnExpression(PColumn column, Collection<PColumn> columns, int position, String displayName) {
        super(column);
        this.columns = columns;
        this.position = position;
        this.displayName = displayName;
    }
    
    public static KeyValueSchema buildSchema(Collection<PColumn> columns) {
        KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
        for (PColumn column : columns) {
            if (!SchemaUtil.isPKColumn(column)) {
                builder.addField(column);
            }
        }
        return builder.build();
    }
    
    public KeyValueSchema getSchema() {
        if (this.schema == null) {
            this.schema = buildSchema(columns);
            this.bitSet = ValueBitSet.newInstance(schema);            
        }
    	return schema;
    }
    
    public int getPosition() {
    	return position;
    }
    
    @Override
    public String toString() {
        return displayName;
    }
	
	@Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + position;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!super.equals(obj)) return false;
        if (getClass() != obj.getClass()) return false;
        ProjectedColumnExpression other = (ProjectedColumnExpression)obj;
        if (position != other.position) return false;
        return true;
    }

    @Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        try {
            KeyValueSchema schema = getSchema();
            TupleProjector.decodeProjectedValue(tuple, ptr);
            bitSet.clear();
            bitSet.or(ptr);
            int maxOffset = ptr.getOffset() + ptr.getLength() - bitSet.getEstimatedLength();
            schema.iterator(ptr, position, bitSet);
            Boolean hasValue = schema.next(ptr, position, maxOffset, bitSet);
            if (hasValue == null || !hasValue.booleanValue())
                return false;
        } catch (IOException e) {
            return false;
        }
		
		return true;
	}

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        schema = new KeyValueSchema();
        schema.readFields(input);
        bitSet = ValueBitSet.newInstance(schema);
        position = input.readInt();
        displayName = input.readUTF();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        getSchema().write(output);
        output.writeInt(position);
        output.writeUTF(displayName);
    }

    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

}
