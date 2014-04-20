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
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.phoenix.pig.hadoop.PhoenixRecord;
import org.apache.phoenix.schema.PDataType;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.builtin.Utf8StorageConverter;
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
	private static final Utf8StorageConverter utf8Converter = new Utf8StorageConverter();
	private static final ImmutableMap<PDataType,Byte> phoenixTypeToPigDataType = init();	
	
	private TypeUtil(){
	}
	
	/**
	 * A map of Phoenix to Pig data types.
	 * @return
	 */
	private static ImmutableMap<PDataType, Byte> init() {
        final ImmutableMap.Builder<PDataType,Byte> builder = new Builder<PDataType,Byte> ();
        builder.put(PDataType.LONG,DataType.LONG);
        builder.put(PDataType.VARBINARY,DataType.BYTEARRAY);
        builder.put(PDataType.CHAR,DataType.CHARARRAY);
        builder.put(PDataType.VARCHAR,DataType.CHARARRAY);
        builder.put(PDataType.DOUBLE,DataType.DOUBLE);
        builder.put(PDataType.FLOAT,DataType.FLOAT);
        builder.put(PDataType.INTEGER,DataType.INTEGER);
        builder.put(PDataType.TINYINT,DataType.INTEGER);
        builder.put(PDataType.SMALLINT,DataType.INTEGER);
        builder.put(PDataType.DECIMAL,DataType.BIGDECIMAL);
        builder.put(PDataType.TIME,DataType.DATETIME);
        builder.put(PDataType.TIMESTAMP,DataType.DATETIME);
        builder.put(PDataType.BOOLEAN,DataType.BOOLEAN);
        builder.put(PDataType.DATE,DataType.DATETIME);
        builder.put(PDataType.UNSIGNED_DATE,DataType.DATETIME);
        builder.put(PDataType.UNSIGNED_DOUBLE,DataType.DOUBLE);
        builder.put(PDataType.UNSIGNED_FLOAT,DataType.FLOAT);
        builder.put(PDataType.UNSIGNED_INT,DataType.INTEGER);
        builder.put(PDataType.UNSIGNED_LONG,DataType.LONG);
        builder.put(PDataType.UNSIGNED_SMALLINT,DataType.INTEGER);
        builder.put(PDataType.UNSIGNED_TIME,DataType.DATETIME);
        builder.put(PDataType.UNSIGNED_TIMESTAMP,DataType.DATETIME);
        builder.put(PDataType.UNSIGNED_TINYINT,DataType.INTEGER);
        return builder.build();
    }
    /**
	 * This method returns the most appropriate PDataType associated with 
	 * the incoming Pig type. Note for Pig DataType DATETIME, returns DATE as 
	 * inferredSqlType. 
	 * 
	 * This is later used to make a cast to targetPhoenixType accordingly. See
	 * {@link #castPigTypeToPhoenix(Object, byte, PDataType)}
	 * 
	 * @param obj
	 * @return PDataType
	 */
	public static PDataType getType(Object obj, byte type) {
		if (obj == null) {
			return null;
		}
	
		PDataType sqlType;

		switch (type) {
		case DataType.BYTEARRAY:
			sqlType = PDataType.VARBINARY;
			break;
		case DataType.CHARARRAY:
			sqlType = PDataType.VARCHAR;
			break;
		case DataType.DOUBLE:
			sqlType = PDataType.DOUBLE;
			break;
		case DataType.FLOAT:
			sqlType = PDataType.FLOAT;
			break;
		case DataType.INTEGER:
			sqlType = PDataType.INTEGER;
			break;
		case DataType.LONG:
			sqlType = PDataType.LONG;
			break;
		case DataType.BOOLEAN:
			sqlType = PDataType.BOOLEAN;
			break;
		case DataType.DATETIME:
			sqlType = PDataType.DATE;
			break;
		default:
			throw new RuntimeException("Unknown type " + obj.getClass().getName()
					+ " passed to PhoenixHBaseStorage");
		}

		return sqlType;

	}

	/**
	 * This method encodes a value with Phoenix data type. It begins
	 * with checking whether an object is BINARY and makes a call to
	 * {@link #castBytes(Object, PDataType)} to convery bytes to
	 * targetPhoenixType
	 * 
	 * @param o
	 * @param targetPhoenixType
	 * @return Object
	 */
	public static Object castPigTypeToPhoenix(Object o, byte objectType, PDataType targetPhoenixType) {
		PDataType inferredPType = getType(o, objectType);
		
		if(inferredPType == null) {
			return null;
		}
		
		if(inferredPType == PDataType.VARBINARY && targetPhoenixType != PDataType.VARBINARY) {
			try {
				o = castBytes(o, targetPhoenixType);
				inferredPType = getType(o, DataType.findType(o));
			} catch (IOException e) {
				throw new RuntimeException("Error while casting bytes for object " +o);
			}
		}

		if(inferredPType == PDataType.DATE) {
			int inferredSqlType = targetPhoenixType.getSqlType();

			if(inferredSqlType == Types.DATE) {
				return new Date(((DateTime)o).getMillis());
			} 
			if(inferredSqlType == Types.TIME) {
				return new Time(((DateTime)o).getMillis());
			}
			if(inferredSqlType == Types.TIMESTAMP) {
				return new Timestamp(((DateTime)o).getMillis());
			}
		}
		
		if (targetPhoenixType == inferredPType || inferredPType.isCoercibleTo(targetPhoenixType)) {
			return inferredPType.toObject(o, targetPhoenixType);
		}
		
		throw new RuntimeException(o.getClass().getName()
				+ " cannot be coerced to "+targetPhoenixType.toString());
	}
	
	/**
	 * This method converts bytes to the target type required
	 * for Phoenix. It uses {@link Utf8StorageConverter} for
	 * the conversion.
	 * 
	 * @param o
	 * @param targetPhoenixType
	 * @return Object
	 * @throws IOException
	 */
    public static Object castBytes(Object o, PDataType targetPhoenixType) throws IOException {
        byte[] bytes = ((DataByteArray)o).get();
        
        switch(targetPhoenixType) {
        case CHAR:
        case VARCHAR:
            return utf8Converter.bytesToCharArray(bytes);
        case UNSIGNED_SMALLINT:
        case SMALLINT:
            return utf8Converter.bytesToInteger(bytes).shortValue();
        case UNSIGNED_TINYINT:
        case TINYINT:
            return utf8Converter.bytesToInteger(bytes).byteValue();
        case UNSIGNED_INT:
        case INTEGER:
            return utf8Converter.bytesToInteger(bytes);
        case BOOLEAN:
            return utf8Converter.bytesToBoolean(bytes);
        case DECIMAL:
            return utf8Converter.bytesToBigDecimal(bytes);
        case FLOAT:
        case UNSIGNED_FLOAT:
            return utf8Converter.bytesToFloat(bytes);
        case DOUBLE:
        case UNSIGNED_DOUBLE:
            return utf8Converter.bytesToDouble(bytes);
        case UNSIGNED_LONG:
        case LONG:
            return utf8Converter.bytesToLong(bytes);
        case TIME:
        case TIMESTAMP:
        case DATE:
        case UNSIGNED_TIME:
        case UNSIGNED_TIMESTAMP:
        case UNSIGNED_DATE:
        	return utf8Converter.bytesToDateTime(bytes);
        default:
        	return o;
        }        
    }
    
    /**
     * Transforms the PhoenixRecord to Pig {@link Tuple}.
     * @param record
     * @param projectedColumns
     * @return
     * @throws IOException
     */
    public static Tuple transformToTuple(final PhoenixRecord record, final ResourceFieldSchema[] projectedColumns) throws IOException {
        
        List<Object> columnValues = record.getValues();
        if(columnValues == null || columnValues.size() == 0 || projectedColumns == null || projectedColumns.length != columnValues.size()) {
            return null;
        }
        int columns = columnValues.size();
        Tuple tuple = TupleFactory.getInstance().newTuple(columns);
        try {
            for(int i = 0 ; i < columns ; i++) {
                final ResourceFieldSchema fieldSchema = projectedColumns[i];
                Object object = columnValues.get(i);
                if (object == null) {
                    tuple.set(i, null);
                    continue;
                }
                
                switch(fieldSchema.getType()) {
                    case DataType.BYTEARRAY:
                        byte[] bytes = PDataType.fromTypeId(PDataType.BINARY.getSqlType()).toBytes(object);
                        tuple.set(i,new DataByteArray(bytes,0,bytes.length));
                        break;
                    case DataType.CHARARRAY:
                        tuple.set(i,DataType.toString(object));
                        break;
                    case DataType.DOUBLE:
                        tuple.set(i,DataType.toDouble(object));
                        break;
                    case DataType.FLOAT:
                        tuple.set(i,DataType.toFloat(object));
                        break;
                    case DataType.INTEGER:
                        tuple.set(i,DataType.toInteger(object));
                        break;
                    case DataType.LONG:
                        tuple.set(i,DataType.toLong(object));
                        break;
                    case DataType.BOOLEAN:
                        tuple.set(i,DataType.toBoolean(object));
                        break;
                    case DataType.DATETIME:
                        tuple.set(i,DataType.toDateTime(object));
                        break;
                    default:
                        throw new RuntimeException(String.format(" Not supported [%s] pig type" , fieldSchema));
                }
            }
        } catch( Exception ex) {
            final String errorMsg = String.format(" Error transforming PhoenixRecord to Tuple [%s] ", ex.getMessage());
            LOG.error(errorMsg);
            throw new PigException(errorMsg);
        }
          return tuple;
    }
    
    /**
     * Returns the mapping pig data type for a given phoenix data type.
     * @param phoenixDataType
     * @return
     */
    public static Byte getPigDataTypeForPhoenixType(final PDataType phoenixDataType) {
        Preconditions.checkNotNull(phoenixDataType);
        final Byte pigDataType = phoenixTypeToPigDataType.get(phoenixDataType);
        if(LOG.isDebugEnabled()) {
            LOG.debug(String.format(" For PhoenixDataType [%s] , pigDataType is [%s] " , phoenixDataType.getSqlTypeName() , DataType.findTypeName(pigDataType)));    
        }
        return pigDataType;
    }

}
