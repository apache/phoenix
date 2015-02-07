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
package org.apache.phoenix.schema;

import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.SizedUtil;

import com.google.common.base.Preconditions;
import com.google.protobuf.HBaseZeroCopyByteString;

public class PColumnImpl implements PColumn {
    private PName name;
    private PName familyName;
    private PDataType dataType;
    private Integer maxLength;
    private Integer scale;
    private boolean nullable;
    private int position;
    private SortOrder sortOrder;
    private Integer arraySize;
    private byte[] viewConstant;
    private boolean isViewReferenced;
    private String expressionStr;
    
    public PColumnImpl() {
    }

    public PColumnImpl(PName name,
                       PName familyName,
                       PDataType dataType,
                       Integer maxLength,
                       Integer scale,
                       boolean nullable,
                       int position,
                       SortOrder sortOrder, Integer arrSize, byte[] viewConstant, boolean isViewReferenced, String expressionStr) {
        init(name, familyName, dataType, maxLength, scale, nullable, position, sortOrder, arrSize, viewConstant, isViewReferenced, expressionStr);
    }

    public PColumnImpl(PColumn column, int position) {
        this(column.getName(), column.getFamilyName(), column.getDataType(), column.getMaxLength(),
                column.getScale(), column.isNullable(), position, column.getSortOrder(), column.getArraySize(), column.getViewConstant(), column.isViewReferenced(), column.getExpressionStr());
    }

    private void init(PName name,
            PName familyName,
            PDataType dataType,
            Integer maxLength,
            Integer scale,
            boolean nullable,
            int position,
            SortOrder sortOrder,
            Integer arrSize,
            byte[] viewConstant, boolean isViewReferenced, String expressionStr) {
    	Preconditions.checkNotNull(sortOrder);
        this.dataType = dataType;
        if (familyName == null) {
            // Allow nullable columns in PK, but only if they're variable length.
            // Variable length types may be null, since we use a null-byte terminator
            // (which is a disallowed character in variable length types). However,
            // fixed width types do not have a way of representing null.
            // TODO: we may be able to allow this for columns at the end of the PK
            Preconditions.checkArgument(!nullable || !dataType.isFixedWidth(), 
                    "PK columns may not be both fixed width and nullable: " + name.getString());
        }
        this.name = name;
        this.familyName = familyName == null ? null : familyName;
        this.maxLength = maxLength;
        this.scale = scale;
        this.nullable = nullable;
        this.position = position;
        this.sortOrder = sortOrder;
        this.arraySize = arrSize;
        this.viewConstant = viewConstant;
        this.isViewReferenced = isViewReferenced;
        this.expressionStr = expressionStr;
    }

    @Override
    public int getEstimatedSize() {
        return SizedUtil.OBJECT_SIZE + SizedUtil.POINTER_SIZE * 8 + SizedUtil.INT_OBJECT_SIZE * 3 + SizedUtil.INT_SIZE + 
                name.getEstimatedSize() + (familyName == null ? 0 : familyName.getEstimatedSize()) +
                (viewConstant == null ? 0 : (SizedUtil.ARRAY_SIZE + viewConstant.length));
    }

    @Override
    public PName getName() {
        return name;
    }

    @Override
    public PName getFamilyName() {
        return familyName;
    }

    @Override
    public PDataType getDataType() {
        return dataType;
    }

    @Override
    public Integer getMaxLength() {
        return maxLength;
    }

    @Override
    public Integer getScale() {
        return scale;
    }
    
    @Override
    public String getExpressionStr() {
        return expressionStr;
    }

    @Override
    public boolean isNullable() {
        // Only PK columns can be NOT NULL. We prevent this in the
        // CREATE TABLE statement now (PHOENIX-1266), but this extra
        // check for familyName != null will ensure that for existing
        // tables we never treat key value columns as NOT NULL.
        return nullable || familyName != null;
    }

    @Override
    public int getPosition() {
        return position;
    }
    
    @Override
    public SortOrder getSortOrder() {
    	return sortOrder;
    }

    @Override
    public String toString() {
        return (familyName == null ? "" : familyName.toString() + QueryConstants.NAME_SEPARATOR) + name.toString();
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((familyName == null) ? 0 : familyName.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        PColumnImpl other = (PColumnImpl)obj;
        if (familyName == null) {
            if (other.familyName != null) return false;
        } else if (!familyName.equals(other.familyName)) return false;
        if (name == null) {
            if (other.name != null) return false;
        } else if (!name.equals(other.name)) return false;
        return true;
    }

    @Override
    public Integer getArraySize() {
        return arraySize;
    }

    @Override
    public byte[] getViewConstant() {
        return viewConstant;
    }

    @Override
    public boolean isViewReferenced() {
        return isViewReferenced;
    }

    /**
     * Create a PColumn instance from PBed PColumn instance
     * 
     * @param column
     */
    public static PColumn createFromProto(PTableProtos.PColumn column) {
        byte[] columnNameBytes = column.getColumnNameBytes().toByteArray();
        PName columnName = PNameFactory.newName(columnNameBytes);
        PName familyName = null;
        if (column.hasFamilyNameBytes()) {
            familyName = PNameFactory.newName(column.getFamilyNameBytes().toByteArray());
        }
        PDataType dataType = PDataType.fromSqlTypeName(column.getDataType());
        Integer maxLength = null;
        if (column.hasMaxLength()) {
            maxLength = column.getMaxLength();
        }
        Integer scale = null;
        if (column.hasScale()) {
            scale = column.getScale();
        }
        boolean nullable = column.getNullable();
        int position = column.getPosition();
        SortOrder sortOrder = SortOrder.fromSystemValue(column.getSortOrder());
        Integer arraySize = null;
        if (column.hasArraySize()) {
            arraySize = column.getArraySize();
        }
        byte[] viewConstant = null;
        if (column.hasViewConstant()) {
            viewConstant = column.getViewConstant().toByteArray();
        }
        boolean isViewReferenced = false;
        if (column.hasViewReferenced()) {
            isViewReferenced = column.getViewReferenced();
        }
        String expressionStr = null;
        if (column.hasExpression()) {
	        expressionStr = column.getExpression();
        }
        return new PColumnImpl(columnName, familyName, dataType, maxLength, scale, nullable, position, sortOrder,
                arraySize, viewConstant, isViewReferenced, expressionStr);
    }

    public static PTableProtos.PColumn toProto(PColumn column) {
        PTableProtos.PColumn.Builder builder = PTableProtos.PColumn.newBuilder();
        builder.setColumnNameBytes(HBaseZeroCopyByteString.wrap(column.getName().getBytes()));
        if (column.getFamilyName() != null) {
            builder.setFamilyNameBytes(HBaseZeroCopyByteString.wrap(column.getFamilyName().getBytes()));
        }
        builder.setDataType(column.getDataType().getSqlTypeName());
        if (column.getMaxLength() != null) {
            builder.setMaxLength(column.getMaxLength());
        }
        if (column.getScale() != null) {
            builder.setScale(column.getScale());
        }
        builder.setNullable(column.isNullable());
        builder.setPosition(column.getPosition());
        builder.setSortOrder(column.getSortOrder().getSystemValue());
        if (column.getArraySize() != null) {
            builder.setArraySize(column.getArraySize());
        }
        if (column.getViewConstant() != null) {
            builder.setViewConstant(HBaseZeroCopyByteString.wrap(column.getViewConstant()));
        }
        builder.setViewReferenced(column.isViewReferenced());
        
        if (column.getExpressionStr() != null) {
            builder.setExpression(column.getExpressionStr());
        }
        return builder.build();
    }
}
