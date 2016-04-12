/*
 * Copyright 2010 The Apache Software Foundation Licensed to the Apache Software Foundation (ASF)
 * under one or more contributor license agreements. See the NOTICE filedistributed with this work
 * for additional information regarding copyright ownership. The ASF licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you maynot use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicablelaw or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.apache.phoenix.hive.util;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PDecimal;

/**
 * HiveTypeUtil
 * Utility Class to convert Hive Type to Pheonix and vise versa
 *
 */

public class HiveTypeUtil {
    private static final Log LOG = LogFactory.getLog(HiveTypeUtil.class);

    private HiveTypeUtil() {
    }

    /**
     * This method returns an array of most appropriates PDataType associated with a list of
     * incoming hive types.
     * @param columnTypes of TypeInfo
     * @return Array PDataType
     */
    public static PDataType[] hiveTypesToSqlTypes(List<TypeInfo> columnTypes) throws SerDeException {
        final PDataType[] result = new PDataType[columnTypes.size()];
        for (int i = 0; i < columnTypes.size(); i++) {
            result[i] = HiveType2PDataType(columnTypes.get(i));
        }
        return result;
    }

    /**
     * This method returns the most appropriate PDataType associated with the incoming primitive
     * hive type.
     * @param hiveType
     * @return PDataType
     */
    public static PDataType HiveType2PDataType(TypeInfo hiveType) throws SerDeException {
        switch (hiveType.getCategory()) {
        /* Integrate Complex types like Array */
        case PRIMITIVE:
            return HiveType2PDataType(hiveType.getTypeName());
        default:
            throw new SerDeException("Phoenix unsupported column type: "
                    + hiveType.getCategory().name());
        }
    }

    /**
     * This method returns the most appropriate PDataType associated with the incoming hive type
     * name.
     * @param hiveType
     * @return PDataType
     */
    public static PDataType HiveType2PDataType(String hiveType) throws SerDeException {
        final String lctype = hiveType.toLowerCase();
        if ("string".equals(lctype)) {
            return PVarchar.INSTANCE;
        }else if (lctype.startsWith("varchar")) {
            return PVarchar.INSTANCE;   
        }else if ("char".equals(lctype)) {
            return PChar.INSTANCE;    
        } else if ("float".equals(lctype)) {
            return PFloat.INSTANCE;
        } else if ("double".equals(lctype)) {
            return PDouble.INSTANCE;
        } else if ("boolean".equals(lctype)) {
            return PBoolean.INSTANCE;
        } else if ("tinyint".equals(lctype)) {
            return PSmallint.INSTANCE;
        } else if ("smallint".equals(lctype)) {
            return PSmallint.INSTANCE;
        } else if ("int".equals(lctype)) {
            return PInteger.INSTANCE;
        } else if ("bigint".equals(lctype)) {
            return PLong.INSTANCE;
        } else if ("timestamp".equals(lctype)) {
            return PTimestamp.INSTANCE;
        } else if ("binary".equals(lctype)) {
            return PBinary.INSTANCE;
        } else if ("decimal".equals(lctype)) {
            return PDecimal.INSTANCE;
        } else if ("date".equals(lctype)) {
            return PDate.INSTANCE;
        }

        throw new SerDeException("Phoenix unrecognized column type: " + hiveType);
    }

    /**
     * This method returns the most appropriate Writable associated with the incoming sql type name.
     * @param hiveType,Object
     * @return Writable
     */
    // TODO awkward logic revisit
    public static Writable SQLType2Writable(String hiveType, Object o) throws SerDeException {
        String lctype = hiveType.toLowerCase();
        if(lctype.startsWith("varchar")) return new HiveVarcharWritable(new HiveVarchar(o
                .toString(),o.toString().length())); //Need to support the length
        if ("string".equals(lctype)) return new Text(o.toString());
        if ("char".equals(lctype)) return new HiveCharWritable(new HiveChar(o.toString(),o.toString().length()));
        if ("float".equals(lctype)) return new FloatWritable(((Float) o).floatValue());
        if ("double".equals(lctype)) return new DoubleWritable(((Double) o).doubleValue());
        if ("boolean".equals(lctype)) return new BooleanWritable(((Boolean) o).booleanValue());
        if ("tinyint".equals(lctype)) return new ShortWritable(((Integer) o).shortValue());
        if ("smallint".equals(lctype)) return new ShortWritable(((Integer) o).shortValue());
        if ("int".equals(lctype)) return new IntWritable(((Integer) o).intValue());
        if ("bigint".equals(lctype)) return new LongWritable(((Long) o).longValue());
        if ("timestamp".equals(lctype)) return new TimestampWritable((Timestamp)o);
        if ("binary".equals(lctype)) return new Text(o.toString());
        if ("date".equals(lctype)) return new DateWritable(new Date((long) o));
        if ("array".equals(lctype))
        ;
        throw new SerDeException("Phoenix unrecognized column type: " + hiveType);
    }
}
