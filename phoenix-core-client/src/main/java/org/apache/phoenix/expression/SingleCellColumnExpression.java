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

import static org.apache.phoenix.query.QueryConstants.SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.compile.CreateTableCompiler.ViewWhereExpressionVisitor;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.ColumnValueDecoder;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.SchemaUtil;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

/**
 * 
 * Class to access a column that is stored in a Cell that contains all
 * columns for a given column family (stored in a serialized array).
 *
 */
public class SingleCellColumnExpression extends KeyValueColumnExpression {
    
    private int decodedColumnQualifier;
    private String arrayColDisplayName;
    private KeyValueColumnExpression keyValueColumnExpression;
    private QualifierEncodingScheme encodingScheme;
    private ImmutableStorageScheme immutableStorageScheme;

    public SingleCellColumnExpression() {
    }

    public SingleCellColumnExpression(ImmutableStorageScheme immutableStorageScheme) {
        this.immutableStorageScheme = immutableStorageScheme;
    }

    public SingleCellColumnExpression(PDatum column, byte[] cf, byte[] cq,
            QualifierEncodingScheme encodingScheme, ImmutableStorageScheme immutableStorageScheme) {
        super(column, cf, SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES);
        this.immutableStorageScheme = immutableStorageScheme;
        Preconditions.checkNotNull(encodingScheme);
        Preconditions.checkArgument(encodingScheme != NON_ENCODED_QUALIFIERS);
        this.decodedColumnQualifier = encodingScheme.decode(cq);
        this.encodingScheme = encodingScheme;
        setKeyValueExpression();
    }
    
    public SingleCellColumnExpression(PColumn column, String displayName, QualifierEncodingScheme encodingScheme, ImmutableStorageScheme immutableStorageScheme) {
        super(column, column.getFamilyName().getBytes(), SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES);
        this.immutableStorageScheme = immutableStorageScheme;
        Preconditions.checkNotNull(encodingScheme);
        Preconditions.checkArgument(encodingScheme != NON_ENCODED_QUALIFIERS);
        this.arrayColDisplayName = displayName;
        this.decodedColumnQualifier = encodingScheme.decode(column.getColumnQualifierBytes());
        this.encodingScheme = encodingScheme;
        setKeyValueExpression();
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
    	if (!super.evaluate(tuple, ptr)) {
            return false;
        } else if (ptr.getLength() == 0) { 
        	return true; 
        }
        // the first position is reserved and we offset maxEncodedColumnQualifier by
        // ENCODED_CQ_COUNTER_INITIAL_VALUE (which is the minimum encoded column qualifier)
        int index = decodedColumnQualifier - QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE + 1;
        // Given a ptr to the entire array, set ptr to point to a particular element
        // within that array
    	ColumnValueDecoder encoderDecoder = immutableStorageScheme.getDecoder();
    	return encoderDecoder.decode(ptr, index);
    }

    @Override
    public boolean evaluateUnsafe(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!super.evaluateUnsafe(tuple, ptr)) {
            return false;
        } else if (ptr.getLength() == 0) {
            return true;
        }
        // the first position is reserved and we offset maxEncodedColumnQualifier by
        // ENCODED_CQ_COUNTER_INITIAL_VALUE (which is the minimum encoded column qualifier)
        int index = decodedColumnQualifier - QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE + 1;
        // Given a ptr to the entire array, set ptr to point to a particular element
        // within that array
        ColumnValueDecoder encoderDecoder = immutableStorageScheme.getDecoder();
        return encoderDecoder.decode(ptr, index);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        this.decodedColumnQualifier = WritableUtils.readVInt(input);
        int serializedEncodingScheme = WritableUtils.readVInt(input);
        // prior to PHOENIX-4432 we weren't writing out the immutableStorageScheme in write(),
        // so we use the decodedColumnQualifier sign to determine whether it's there
        if (Integer.signum(serializedEncodingScheme) == -1) {
            this.immutableStorageScheme =
                    ImmutableStorageScheme
                            .fromSerializedValue((byte) WritableUtils.readVInt(input));
            serializedEncodingScheme = -serializedEncodingScheme;
        } else {
            this.immutableStorageScheme = ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS;
        }
        this.encodingScheme = QualifierEncodingScheme.values()[serializedEncodingScheme];
        setKeyValueExpression();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeVInt(output, decodedColumnQualifier);
        WritableUtils.writeVInt(output, -encodingScheme.ordinal()); //negative since PHOENIX-4432
        WritableUtils.writeVInt(output, immutableStorageScheme.getSerializedMetadataValue());
    }
    
    public KeyValueColumnExpression getKeyValueExpression() {
        return keyValueColumnExpression;
    }
    
    private void setKeyValueExpression() {
        final boolean isNullable = isNullable();
        final SortOrder sortOrder = getSortOrder();
        final Integer scale = getScale();
        final Integer maxLength = getMaxLength();
        final PDataType datatype = getDataType();
    	this.keyValueColumnExpression = new KeyValueColumnExpression(new PDatum() {
			@Override
			public boolean isNullable() {
				return isNullable;
			}
			
			@Override
			public SortOrder getSortOrder() {
				return sortOrder;
			}
			
			@Override
			public Integer getScale() {
				return scale;
			}
			
			@Override
			public Integer getMaxLength() {
				return maxLength;
			}
			
			@Override
			public PDataType getDataType() {
				return datatype;
			}
		}, getColumnFamily(), getPositionInArray());
    }
    
    @Override
    public String toString() {
        if (arrayColDisplayName == null) {
            arrayColDisplayName = SchemaUtil.getColumnDisplayName(getColumnFamily(), getColumnQualifier());
        }
        return arrayColDisplayName;
    }
    
    public byte[] getPositionInArray() {
        return encodingScheme.encode(decodedColumnQualifier);
    }
    
    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        //FIXME: this is ugly but can't think of a good solution.
        if (visitor instanceof ViewWhereExpressionVisitor) {
            return visitor.visit(this);
        } else {
            return super.accept(visitor);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass() != SingleCellColumnExpression.class) return false;
        return keyValueColumnExpression.equals(((SingleCellColumnExpression)obj).getKeyValueExpression());
    }

    @Override
    public int hashCode() {
        return keyValueColumnExpression.hashCode();
    }

}
