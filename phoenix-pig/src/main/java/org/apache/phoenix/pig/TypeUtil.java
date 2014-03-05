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

package org.apache.phoenix.pig;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.joda.time.DateTime;

import org.apache.phoenix.schema.PDataType;

public class TypeUtil {
	
	private static final Utf8StorageConverter utf8Converter = new Utf8StorageConverter();
	
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

}
