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
package org.apache.phoenix.jdbc;

import java.sql.ParameterMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.parse.BindParseNode;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TypeMismatchException;



/**
 * 
 * Implementation of ParameterMetaData for Phoenix
 *
 * 
 * @since 0.1
 */
public class PhoenixParameterMetaData implements ParameterMetaData {
    private final PDatum[] params;
    private static final PDatum EMPTY_DATUM = new PDatum() {
        @Override
        public boolean isNullable() {
            return false;
        }
        
        @Override
        public Integer getScale() {
            return null;
        }
        
        @Override
        public Integer getMaxLength() {
            return null;
        }
        
        @Override
        public PDataType getDataType() {
            return null;
        }
        
        @Override
        public SortOrder getSortOrder() {
            return SortOrder.getDefault();
        }
    };
    public static final PhoenixParameterMetaData EMPTY_PARAMETER_META_DATA = new PhoenixParameterMetaData(0);
    public PhoenixParameterMetaData(int paramCount) {
        params = new PDatum[paramCount];
        //initialize the params array with the empty_datum marker value.
        for(int i = 0; i < paramCount; i++) {
            params[i] = EMPTY_DATUM;
        }
    }
 
    private PDatum getParam(int index) throws SQLException {
        if (index <= 0 || index > params.length) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.PARAM_INDEX_OUT_OF_BOUND)
                .setMessage("The index is " + index + ". Must be between 1 and " + params.length)
                .build().buildException();
        }
        PDatum param = params[index-1];
        
        if (param == EMPTY_DATUM) {
            //value at params[index-1] was never set.
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.PARAM_VALUE_UNBOUND)
                .setMessage("Parameter at index " + index + " is unbound").build().buildException();
        }
        return param;
    }
    @Override
    public String getParameterClassName(int index) throws SQLException {
        PDatum datum = getParam(index);
        PDataType type = datum == null ? null : datum.getDataType();
        return type == null ? null : type.getJavaClassName();
    }

    @Override
    public int getParameterCount() throws SQLException {
        return params.length;
    }

    @Override
    public int getParameterMode(int index) throws SQLException {
        return ParameterMetaData.parameterModeIn;
    }

    @Override
    public int getParameterType(int index) throws SQLException {
        return getParam(index).getDataType().getSqlType();
    }

    @Override
    public String getParameterTypeName(int index) throws SQLException {
        return getParam(index).getDataType().getSqlTypeName();
    }

    @Override
    public int getPrecision(int index) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int index) throws SQLException {
        return 0;
    }

    @Override
    public int isNullable(int index) throws SQLException {
        return getParam(index).isNullable() ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls;
    }

    @Override
    public boolean isSigned(int index) throws SQLException {
        @SuppressWarnings("rawtypes")
		Class clazz = getParam(index).getDataType().getJavaClass();
        return Number.class.isInstance(clazz);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!iface.isInstance(this)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CLASS_NOT_UNWRAPPABLE)
                .setMessage(this.getClass().getName() + " not unwrappable from " + iface.getName())
                .build().buildException();
        }
        return (T)this;
    }

    public void addParam(BindParseNode bind, PDatum datum) throws SQLException {
        PDatum bindDatum = params[bind.getIndex()];
        if (bindDatum != null && bindDatum.getDataType() != null && !datum.getDataType().isCoercibleTo(bindDatum.getDataType())) {
            throw TypeMismatchException.newException(datum.getDataType(), bindDatum.getDataType());
        }
        params[bind.getIndex()] = datum;
    }
}
