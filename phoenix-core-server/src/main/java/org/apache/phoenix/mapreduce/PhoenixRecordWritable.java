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
package org.apache.phoenix.mapreduce;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.ColumnInfo;
import org.joda.time.DateTime;


public class PhoenixRecordWritable implements DBWritable {

    private final List<Object> upsertValues = new ArrayList<>();
    private final Map<String, Object> resultMap = new LinkedHashMap<>();
    private List<ColumnInfo> columnMetaDataList; 

    /** For serialization; do not use. */
    public PhoenixRecordWritable() {
        this(new ArrayList<ColumnInfo>());
    }

    public PhoenixRecordWritable(List<ColumnInfo> columnMetaDataList) {
        this.columnMetaDataList = columnMetaDataList;
    }

    /**
     * Helper method to create a {@link Array} for a specific {@link PDataType}, and set it on
     * the provided {@code stmt}.
     */
    private static void setArrayInStatement(PreparedStatement stmt, PDataType<?> type,
            Object[] obj, int position) throws SQLException {
        Array sqlArray = stmt.getConnection().createArrayOf(
                PDataType.arrayBaseType(type).getSqlTypeName(), obj);
        stmt.setArray(position, sqlArray);
    }

    private static Object[] primativeArrayToObjectArray(byte[] a) {
        final Byte[] ret = new Byte[a.length];
        for (int i = 0; i < a.length; i++) {
            ret[i] = a[i];
        }
        return ret;
    }

    private static Object[] primativeArrayToObjectArray(short[] a) {
        final Short[] ret = new Short[a.length];
        for (int i = 0; i < a.length; i++) {
            ret[i] = a[i];
        }
        return ret;
    }

    private static Object[] primativeArrayToObjectArray(int[] a) {
        final Integer[] ret = new Integer[a.length];
        for (int i = 0; i < a.length; i++) {
            ret[i] = a[i];
        }
        return ret;
    }

    private static Object[] primativeArrayToObjectArray(float[] a) {
        final Float[] ret = new Float[a.length];
        for (int i = 0; i < a.length; i++) {
            ret[i] = a[i];
        }
        return ret;
    }

    private static Object[] primativeArrayToObjectArray(double[] a) {
        final Double[] ret = new Double[a.length];
        for (int i = 0; i < a.length; i++) {
            ret[i] = a[i];
        }
        return ret;
    }

    private static Object[] primativeArrayToObjectArray(long[] a) {
        final Long[] ret = new Long[a.length];
        for (int i = 0; i < a.length; i++) {
            ret[i] = a[i];
        }
        return ret;
    }

    @Override public void write(PreparedStatement statement) throws SQLException {
        // make sure we at least line up in size
        if (upsertValues.size() != columnMetaDataList.size()) {
            throw new UnsupportedOperationException("Provided " + upsertValues.size()
                    + " upsert values, but column metadata expects " + columnMetaDataList.size()
                    + " columns.");
        }

        // correlate each value (v) to a column type (c) and an index (i)
        for (int i = 0; i < upsertValues.size(); i++) {
            Object v = upsertValues.get(i);
            ColumnInfo c = columnMetaDataList.get(i);

            if (v == null) {
                statement.setNull(i + 1, c.getSqlType());
                continue;
            }

            // both Java and Joda dates used to work in 4.2.3, but now they must be java.sql.Date
            // can override any other types here as needed
            final Object finalObj;
            final PDataType<?> finalType;
            if (v instanceof DateTime) {
                finalObj = new java.sql.Date(((DateTime) v).getMillis());
                finalType = PDate.INSTANCE;
            } else if (v instanceof java.util.Date) {
                finalObj = new java.sql.Date(((java.util.Date) v).getTime());
                finalType = PDate.INSTANCE;
            } else {
                finalObj = v;
                finalType = c.getPDataType();
            }

            if (finalObj instanceof Object[]) {
                setArrayInStatement(statement, finalType, (Object[]) finalObj, i + 1);
            } else if (finalObj instanceof byte[]) {
                // PVarbinary and PBinary are provided as byte[] but are treated as SQL objects
                if (PDataType.equalsAny(finalType, PVarbinary.INSTANCE, PBinary.INSTANCE)) {
                    statement.setObject(i + 1, finalObj);
                } else {
                    // otherwise set as array type
                    setArrayInStatement(statement, finalType, primativeArrayToObjectArray((byte[]) finalObj), i + 1);
                }
            } else if (finalObj instanceof short[]) {
                setArrayInStatement(statement, finalType, primativeArrayToObjectArray((short[]) finalObj), i + 1);
            } else if (finalObj instanceof int[]) {
                setArrayInStatement(statement, finalType, primativeArrayToObjectArray((int[]) finalObj), i + 1);
            } else if (finalObj instanceof long[]) {
                setArrayInStatement(statement, finalType, primativeArrayToObjectArray((long[]) finalObj), i + 1);
            } else if (finalObj instanceof float[]) {
                setArrayInStatement(statement, finalType, primativeArrayToObjectArray((float[]) finalObj), i + 1);
            } else if (finalObj instanceof double[]) {
                setArrayInStatement(statement, finalType, primativeArrayToObjectArray((double[]) finalObj), i + 1);
            } else {
                statement.setObject(i + 1, finalObj);
            }
        }
    }

    @Override public void readFields(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            // return the contents of a PhoenixArray, if necessary
            Object value = resultSet.getObject(i);
            // put a (ColumnLabel -> value) entry into the result map
            resultMap.put(metaData.getColumnLabel(i), value);
        }
    }

    /** Append an object to the list of values to upsert. */
    public void add(Object value) {
        upsertValues.add(value);
    }

    /** @return an immutable view on the {@link ResultSet} content. */
    public Map<String, Object> getResultMap() {
        return Collections.unmodifiableMap(resultMap);
    }
}
