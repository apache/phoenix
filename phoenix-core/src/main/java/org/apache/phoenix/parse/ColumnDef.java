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
package org.apache.phoenix.parse;

import java.sql.SQLException;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.base.Preconditions;


/**
 *
 * Represents a column definition during DDL
 *
 *
 * @since 0.1
 */
public class ColumnDef {
    private final ColumnName columnDefName;
    private final PDataType dataType;
    private final Boolean isNull;
    private final Integer maxLength;
    private final Integer scale;
    private final boolean isPK;
    private final SortOrder sortOrder;
    private final boolean isArray;
    private final Integer arrSize;
    private final String expressionStr;
    private final boolean isRowTimestamp;

    ColumnDef(ColumnName columnDefName, String sqlTypeName, boolean isArray, Integer arrSize, Boolean isNull, Integer maxLength,
    		            Integer scale, boolean isPK, SortOrder sortOrder, String expressionStr, boolean isRowTimestamp) {
   	 try {
         Preconditions.checkNotNull(sortOrder);
         Preconditions.checkNotNull(sqlTypeName);

         this.columnDefName = columnDefName;
         this.isArray = isArray;

         PDataType dataType = PDataType.fromSqlTypeName(SchemaUtil.normalizeIdentifier(sqlTypeName));
         PDataType baseType;
         // TODO : Add correctness check for arrSize.  Should this be ignored as in postgres
         // Also add what is the limit that we would support.  Are we going to support a
         //  fixed size or like postgres allow infinite.  May be the data types max limit can
         // be used for the array size (May be too big)
         if (isArray) {
             if (dataType == PVarbinary.INSTANCE) {
                 throw new SQLExceptionInfo.Builder(SQLExceptionCode.VARBINARY_ARRAY_NOT_SUPPORTED)
                 .setColumnName(columnDefName.getColumnName()).build().buildException();
             }
             baseType = dataType;
             dataType = PDataType.toArrayType(dataType);
             this.arrSize = arrSize; // Can only be non negative based on parsing
         } else {
             baseType = dataType.isArrayType() ? PDataType.toBaseType(dataType) : dataType;
             this.arrSize = null;
         }
         maxLength = baseType.validateMaxLength(columnDefName, maxLength);
         scale = baseType.validateScale(columnDefName, maxLength, scale);

         this.dataType = dataType;
         this.isNull = isNull;
         this.maxLength = maxLength;
         this.scale = scale;
         this.isPK = isPK;
         this.sortOrder = sortOrder;
         this.expressionStr = expressionStr;
         this.isRowTimestamp = isRowTimestamp;
     } catch (SQLException e) {
         throw new ParseException(e);
     }
    }

    ColumnDef(ColumnName columnDefName, String sqlTypeName, Boolean isNull, Integer maxLength,
            Integer scale, boolean isPK, SortOrder sortOrder, String expressionStr, boolean isRowTimestamp) {
    	this(columnDefName, sqlTypeName, false, 0, isNull, maxLength, scale, isPK, sortOrder, expressionStr, isRowTimestamp);
    }

    ColumnDef(ColumnName columnDefName) {
        this.columnDefName = columnDefName;
        this.dataType = null;
        this.isNull = true;
        this.maxLength = null;
        this.scale = null;
        this.isPK = false;
        this.sortOrder = SortOrder.getDefault();
        this.expressionStr = null;
        this.isRowTimestamp = false;
        this.isArray = false;
        this.arrSize = null;
    }

    public ColumnName getColumnDefName() {
        return columnDefName;
    }

    public PDataType getDataType() {
        return dataType;
    }

    public boolean isNull() {
        // null or Boolean.TRUE means NULL
        // Boolean.FALSE means NOT NULL
        return !Boolean.FALSE.equals(isNull);
    }

    public boolean isNullSet() {
        return isNull != null;
    }

    public Integer getMaxLength() {
        return maxLength;
    }

    public Integer getScale() {
        return scale;
    }

    public boolean isPK() {
        return isPK;
    }

    public SortOrder getSortOrder() {
    	return sortOrder;
    }

	public boolean isArray() {
		return isArray;
	}

	public Integer getArraySize() {
		return arrSize;
	}

	public String getExpression() {
		return expressionStr;
	}

	public boolean isRowTimestamp() {
	    return isRowTimestamp;
	}
	@Override
    public String toString() {
	    StringBuilder buf = new StringBuilder(columnDefName.getColumnNode().toString());
	    buf.append(' ');
        buf.append(dataType.getSqlTypeName());
        if (maxLength != null) {
            buf.append('(');
            buf.append(maxLength);
            if (scale != null) {
              buf.append(',');
              buf.append(scale); // has both max length and scale. For ex- decimal(10,2)
            }
            buf.append(')');
       }
        if (isArray) {
            buf.append(' ');
            buf.append(PDataType.ARRAY_TYPE_SUFFIX);
            buf.append(' ');
        }
	    return buf.toString();
	}
}
