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

package org.apache.phoenix.pig.util;

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.phoenix.mapreduce.PhoenixRecordWritable;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PTinyint;
import org.apache.phoenix.schema.types.PUnsignedDate;
import org.apache.phoenix.schema.types.PUnsignedDouble;
import org.apache.phoenix.schema.types.PUnsignedFloat;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PUnsignedLong;
import org.apache.phoenix.schema.types.PUnsignedSmallint;
import org.apache.phoenix.schema.types.PUnsignedTime;
import org.apache.phoenix.schema.types.PUnsignedTimestamp;
import org.apache.phoenix.schema.types.PUnsignedTinyint;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.hadoop.hbase.HBaseBinaryConverter;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.joda.time.DateTime;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public final class TypeUtil {

    private static final Log LOG = LogFactory.getLog(TypeUtil.class);
    private static final HBaseBinaryConverter BINARY_CONVERTER = new HBaseBinaryConverter();
    private static final ImmutableMap<PDataType, Byte> PHOENIX_TO_PIG_TYPE = init();
    private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
    
    private TypeUtil() {}

    /**
     * @return map of Phoenix to Pig data types.
     */
    private static ImmutableMap<PDataType, Byte> init() {
        final ImmutableMap.Builder<PDataType, Byte> builder = new Builder<PDataType, Byte>();
        builder.put(PLong.INSTANCE, DataType.LONG);
        builder.put(PVarbinary.INSTANCE, DataType.BYTEARRAY);
        builder.put(PChar.INSTANCE, DataType.CHARARRAY);
        builder.put(PVarchar.INSTANCE, DataType.CHARARRAY);
        builder.put(PDouble.INSTANCE, DataType.DOUBLE);
        builder.put(PFloat.INSTANCE, DataType.FLOAT);
        builder.put(PInteger.INSTANCE, DataType.INTEGER);
        builder.put(PTinyint.INSTANCE, DataType.INTEGER);
        builder.put(PSmallint.INSTANCE, DataType.INTEGER);
        builder.put(PDecimal.INSTANCE, DataType.BIGDECIMAL);
        builder.put(PTime.INSTANCE, DataType.DATETIME);
        builder.put(PTimestamp.INSTANCE, DataType.DATETIME);
        builder.put(PBoolean.INSTANCE, DataType.BOOLEAN);
        builder.put(PDate.INSTANCE, DataType.DATETIME);
        builder.put(PUnsignedDate.INSTANCE, DataType.DATETIME);
        builder.put(PUnsignedDouble.INSTANCE, DataType.DOUBLE);
        builder.put(PUnsignedFloat.INSTANCE, DataType.FLOAT);
        builder.put(PUnsignedInt.INSTANCE, DataType.INTEGER);
        builder.put(PUnsignedLong.INSTANCE, DataType.LONG);
        builder.put(PUnsignedSmallint.INSTANCE, DataType.INTEGER);
        builder.put(PUnsignedTime.INSTANCE, DataType.DATETIME);
        builder.put(PUnsignedTimestamp.INSTANCE, DataType.DATETIME);
        builder.put(PUnsignedTinyint.INSTANCE, DataType.INTEGER);
        return builder.build();
    }

    /**
     * This method returns the most appropriate PDataType associated with the incoming Pig type. Note for Pig DataType
     * DATETIME, returns DATE as inferredSqlType. This is later used to make a cast to targetPhoenixType accordingly.
     * See {@link #castPigTypeToPhoenix(Object, byte, PDataType)}
     * 
     * @param obj
     * @return PDataType
     */
    public static PDataType getType(Object obj, byte type) {
        if (obj == null) { return null; }
        PDataType sqlType;

        switch (type) {
        case DataType.BYTEARRAY:
            sqlType = PVarbinary.INSTANCE;
            break;
        case DataType.CHARARRAY:
            sqlType = PVarchar.INSTANCE;
            break;
        case DataType.DOUBLE:
        case DataType.BIGDECIMAL:
            sqlType = PDouble.INSTANCE;
            break;
        case DataType.FLOAT:
            sqlType = PFloat.INSTANCE;
            break;
        case DataType.INTEGER:
            sqlType = PInteger.INSTANCE;
            break;
        case DataType.LONG:
        case DataType.BIGINTEGER:
            sqlType = PLong.INSTANCE;
            break;
        case DataType.BOOLEAN:
            sqlType = PBoolean.INSTANCE;
            break;
        case DataType.DATETIME:
            sqlType = PDate.INSTANCE;
            break;
        case DataType.BYTE:
            sqlType = PTinyint.INSTANCE;
            break;
        default:
            throw new RuntimeException("Unknown type " + obj.getClass().getName() + " passed to PhoenixHBaseStorage");
        }

        return sqlType;

    }

    /**
     * This method encodes a value with Phoenix data type. It begins with checking whether an object is TUPLE. A {@link Tuple} is mapped
     * to a {@link PArrayDataType} .  It then checks if it is BINARY and makes
     * a call to {@link #castBytes(Object, PDataType)} to convert bytes to targetPhoenixType. It returns a {@link RuntimeException}
     * when object can not be coerced.
     * 
     * @param o
     * @param targetPhoenixType
     * @return Object
     * @throws SQLException 
     */
    public static Object castPigTypeToPhoenix(Object o, byte objectType, PDataType targetPhoenixType) throws SQLException {
        
        if(DataType.TUPLE == objectType) {
            Tuple tuple = (Tuple)o;
            List<Object> data = tuple.getAll();
            return data.toArray();
        }
        
        PDataType inferredPType = getType(o, objectType);

        if (inferredPType == null) { return null; }

        if (inferredPType == PVarbinary.INSTANCE) {
            try {
                o = castBytes(o, targetPhoenixType);
                if (targetPhoenixType != PVarbinary.INSTANCE && targetPhoenixType != PBinary.INSTANCE) {
                    inferredPType = getType(o, DataType.findType(o));
                }
            } catch (IOException e) {
                throw new RuntimeException("Error while casting bytes for object " + o);
            }
        }
        if (inferredPType == PDate.INSTANCE) {
            int inferredSqlType = targetPhoenixType.getSqlType();

            if (inferredSqlType == Types.DATE) { return new Date(((DateTime)o).getMillis()); }
            if (inferredSqlType == Types.TIME) { return new Time(((DateTime)o).getMillis()); }
            if (inferredSqlType == Types.TIMESTAMP) { return new Timestamp(((DateTime)o).getMillis()); }
        }

        if (targetPhoenixType == inferredPType || inferredPType.isCoercibleTo(targetPhoenixType)) { return inferredPType
                .toObject(o, targetPhoenixType); }

        throw new RuntimeException(o.getClass().getName() + " cannot be coerced to " + targetPhoenixType.toString());
    }

    /**
     * This method converts bytes to the target type required for Phoenix. It uses {@link HBaseBinaryConverter} for the
     * conversion.
     * 
     * @param o
     * @param targetPhoenixType
     * @return Object
     * @throws IOException
     */
    private static Object castBytes(Object o, PDataType targetPhoenixType) throws IOException {
        byte[] bytes = ((DataByteArray)o).get();

        if (PDataType.equalsAny(targetPhoenixType, PChar.INSTANCE, PVarchar.INSTANCE)) {
            return BINARY_CONVERTER.bytesToCharArray(bytes);
        } else if (PDataType.equalsAny(targetPhoenixType, PUnsignedSmallint.INSTANCE, PSmallint.INSTANCE)) {
            return BINARY_CONVERTER.bytesToInteger(bytes).shortValue();
        } else if (PDataType.equalsAny(targetPhoenixType, PUnsignedTinyint.INSTANCE, PTinyint.INSTANCE)) {
            return BINARY_CONVERTER.bytesToInteger(bytes).byteValue();
        } else if (PDataType.equalsAny(targetPhoenixType, PUnsignedInt.INSTANCE, PInteger.INSTANCE)) {
            return BINARY_CONVERTER.bytesToInteger(bytes);
        } else if (targetPhoenixType.equals(PBoolean.INSTANCE)) {
            return BINARY_CONVERTER.bytesToBoolean(bytes);
        } else if (PDataType.equalsAny(targetPhoenixType, PFloat.INSTANCE, PUnsignedFloat.INSTANCE)) {
            return BINARY_CONVERTER.bytesToFloat(bytes);
        } else if (PDataType.equalsAny(targetPhoenixType, PDouble.INSTANCE, PUnsignedDouble.INSTANCE)) {
            return BINARY_CONVERTER.bytesToDouble(bytes);
        } else if (PDataType.equalsAny(targetPhoenixType, PUnsignedLong.INSTANCE, PLong.INSTANCE)) {
            return BINARY_CONVERTER.bytesToLong(bytes);
        } else if (PDataType.equalsAny(targetPhoenixType, PVarbinary.INSTANCE, PBinary.INSTANCE)) {
            return bytes;
        } else {
            return o;
        }
    }

    /**
     * Transforms the PhoenixRecord to Pig {@link Tuple}.
     * 
     * @param record
     * @param projectedColumns
     * @return
     * @throws IOException
     */
    public static Tuple transformToTuple(final PhoenixRecordWritable record, final ResourceFieldSchema[] projectedColumns) 
            throws IOException {

        Map<String, Object> columnValues = record.getResultMap();
        
        if (columnValues == null || columnValues.size() == 0 || projectedColumns == null
                || projectedColumns.length != columnValues.size()) { return null; }
        int numColumns = columnValues.size();
        Tuple tuple = TUPLE_FACTORY.newTuple(numColumns);
        try {
            int i = 0;
            for (Map.Entry<String,Object> entry : columnValues.entrySet()) {
                final ResourceFieldSchema fieldSchema = projectedColumns[i];
                Object object = entry.getValue();
                if (object == null) {
                    tuple.set(i++, null);
                    continue;
                }

                switch (fieldSchema.getType()) {
                case DataType.BYTEARRAY:
                    byte[] bytes = PDataType.fromTypeId(PBinary.INSTANCE.getSqlType()).toBytes(object);
                    tuple.set(i, new DataByteArray(bytes, 0, bytes.length));
                    break;
                case DataType.CHARARRAY:
                    tuple.set(i, DataType.toString(object));
                    break;
                case DataType.DOUBLE:
                    tuple.set(i, DataType.toDouble(object));
                    break;
                case DataType.FLOAT:
                    tuple.set(i, DataType.toFloat(object));
                    break;
                case DataType.INTEGER:
                    tuple.set(i, DataType.toInteger(object));
                    break;
                case DataType.LONG:
                    tuple.set(i, DataType.toLong(object));
                    break;
                case DataType.BOOLEAN:
                    tuple.set(i, DataType.toBoolean(object));
                    break;
                case DataType.DATETIME:
                    if (object instanceof java.sql.Timestamp)
                        tuple.set(i,new DateTime(((java.sql.Timestamp)object).getTime()));
                    else
                        tuple.set(i,new DateTime(object));
                    break;
                case DataType.BIGDECIMAL:
                    tuple.set(i, DataType.toBigDecimal(object));
                    break;
                case DataType.BIGINTEGER:
                    tuple.set(i, DataType.toBigInteger(object));
                    break;
                case DataType.TUPLE:
                {
                    PhoenixArray array = (PhoenixArray)object;
                    Tuple t = TUPLE_FACTORY.newTuple(array.getDimensions());;
                    for(int j = 0 ; j < array.getDimensions() ; j++) {
                        t.set(j,array.getElement(j));
                    }
                    tuple.set(i, t);
                    break;
                }
                default:
                    throw new RuntimeException(String.format(" Not supported [%s] pig type", fieldSchema));
                }
                i++;
            }
        } catch (Exception ex) {
            final String errorMsg = String.format(" Error transforming PhoenixRecord to Tuple [%s] ", ex.getMessage());
            LOG.error(errorMsg);
            throw new PigException(errorMsg);
        }
        return tuple;
    }
    
    /**
     * Returns the mapping pig data type for a given phoenix data type.
     * 
     * @param phoenixDataType
     * @return
     */
    public static Byte getPigDataTypeForPhoenixType(final PDataType phoenixDataType) {
        Preconditions.checkNotNull(phoenixDataType);
        if(phoenixDataType instanceof PArrayDataType) {
            return DataType.TUPLE;
        }
        final Byte pigDataType = PHOENIX_TO_PIG_TYPE.get(phoenixDataType);
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format(" For PhoenixDataType [%s] , pigDataType is [%s] ",
                    phoenixDataType.getSqlTypeName(), DataType.findTypeName(pigDataType)));
        }
        return pigDataType;
    }

}
