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
package org.apache.phoenix.hive.util;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
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
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.PhoenixRuntime;

/**
 *
 */
public class HiveConfigurationUtil {
    static Log LOG = LogFactory.getLog(HiveConfigurationUtil.class.getName());

    public static final String TABLE_NAME = "phoenix.hbase.table.name";
    public static final String ZOOKEEPER_QUORUM = "phoenix.zookeeper.quorum";
    public static final String ZOOKEEPER_PORT = "phoenix.zookeeper.client.port";
    public static final String ZOOKEEPER_PARENT = "phoenix.zookeeper.znode.parent";
    public static final String ZOOKEEPER_QUORUM_DEFAULT = "localhost";
    public static final String ZOOKEEPER_PORT_DEFAULT = "2181";
    public static final String ZOOKEEPER_PARENT_DEFAULT = "/hbase-unsecure";

    public static final String COLUMN_MAPPING = "phoenix.column.mapping";
    public static final String AUTOCREATE = "autocreate";
    public static final String AUTODROP = "autodrop";
    public static final String AUTOCOMMIT = "autocommit";
    public static final String PHOENIX_ROWKEYS = "phoenix.rowkeys";
    public static final String SALT_BUCKETS = "saltbuckets";
    public static final String COMPRESSION = "compression";
    public static final String VERSIONS = "versions";
    public static final int VERSIONS_NUM = 5;
    public static final String SPLIT = "split";
    public static final String REDUCE_SPECULATIVE_EXEC =
            "mapred.reduce.tasks.speculative.execution";
    public static final String MAP_SPECULATIVE_EXEC = "mapred.map.tasks.speculative.execution";

    public static void setProperties(Properties tblProps, Map<String, String> jobProperties) {
        String quorum = tblProps.getProperty(HiveConfigurationUtil.ZOOKEEPER_QUORUM) != null ?
                        tblProps.getProperty(HiveConfigurationUtil.ZOOKEEPER_QUORUM) :
                        HiveConfigurationUtil.ZOOKEEPER_QUORUM_DEFAULT;
        String znode = tblProps.getProperty(HiveConfigurationUtil.ZOOKEEPER_PARENT) != null ?
                        tblProps.getProperty(HiveConfigurationUtil.ZOOKEEPER_PARENT) :
                        HiveConfigurationUtil.ZOOKEEPER_PARENT_DEFAULT;
        String port = tblProps.getProperty(HiveConfigurationUtil.ZOOKEEPER_PORT) != null ?
                        tblProps.getProperty(HiveConfigurationUtil.ZOOKEEPER_PORT) :
                        HiveConfigurationUtil.ZOOKEEPER_PORT_DEFAULT;
        if (!znode.startsWith("/")) {
            znode = "/" + znode;
        }
        LOG.debug("quorum:" + quorum);
        LOG.debug("port:" + port);
        LOG.debug("parent:" +znode);
        LOG.debug("table:" + tblProps.getProperty(HiveConfigurationUtil.TABLE_NAME));
        LOG.debug("batch:" + tblProps.getProperty(PhoenixConfigurationUtil.UPSERT_BATCH_SIZE));
        
        jobProperties.put(HiveConfigurationUtil.ZOOKEEPER_QUORUM, quorum);
        jobProperties.put(HiveConfigurationUtil.ZOOKEEPER_PORT, port);
        jobProperties.put(HiveConfigurationUtil.ZOOKEEPER_PARENT, znode);
        String tableName = tblProps.getProperty(HiveConfigurationUtil.TABLE_NAME);
        if (tableName == null) {
            tableName = tblProps.get("name").toString();
            tableName = tableName.split(".")[1];
        }
        // TODO this is synch with common Phoenix mechanism revisit to make wiser decisions
        jobProperties.put(HConstants.ZOOKEEPER_QUORUM,quorum+PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR
                + port + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR+znode);
        jobProperties.put(HiveConfigurationUtil.TABLE_NAME, tableName);
        // TODO this is synch with common Phoenix mechanism revisit to make wiser decisions
        jobProperties.put(PhoenixConfigurationUtil.OUTPUT_TABLE_NAME, tableName);
        jobProperties.put(PhoenixConfigurationUtil.INPUT_TABLE_NAME, tableName);
    }

    public static PDataType[] hiveTypesToPDataType(
            PrimitiveObjectInspector.PrimitiveCategory[] hiveTypes) throws SerDeException {
        final PDataType[] result = new PDataType[hiveTypes.length];
        for (int i = 0; i < hiveTypes.length; i++) {
            result[i] = hiveTypeToPDataType(hiveTypes[i]);
        }
        return result;
    }

    public static PDataType[] hiveTypesToSqlTypes(List<TypeInfo> columnTypes) throws SerDeException {
        final PDataType[] result = new PDataType[columnTypes.size()];
        for (int i = 0; i < columnTypes.size(); i++) {
            result[i] = hiveTypeToPDataType(columnTypes.get(i));
        }
        return result;
    }

    /**
     * This method returns the most appropriate PDataType associated with the incoming hive type.
     * @param hiveType
     * @return PDataType
     */

    public static PDataType hiveTypeToPDataType(TypeInfo hiveType) throws SerDeException {
        switch (hiveType.getCategory()) {
        case PRIMITIVE:
            // return
            // hiveTypeToPDataType(((PrimitiveObjectInspector)hiveType).getPrimitiveCategory());
            return hiveTypeToPDataType(hiveType.getTypeName());
        default:

            throw new SerDeException("Unsupported column type: " + hiveType.getCategory().name());
        }
    }

    public static PDataType hiveTypeToPDataType(String hiveType) throws SerDeException {
        final String lctype = hiveType.toLowerCase();
        if ("string".equals(lctype)) {
            return PVarchar.INSTANCE;
        } else if ("float".equals(lctype)) {
            return PFloat.INSTANCE;
        } else if ("double".equals(lctype)) {
            return PDouble.INSTANCE;
        } else if ("boolean".equals(lctype)) {
            return PBoolean.INSTANCE;
        } else if ("tinyint".equals(lctype)) {
            return PTinyint.INSTANCE;
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
        } else if ("date".equals(lctype)) {
            return PDate.INSTANCE;
        }

        throw new SerDeException("Unrecognized column type: " + hiveType);
    }

    /**
     * This method returns the most appropriate PDataType associated with the incoming hive type.
     * @param hiveType
     * @return PDataType
     */

    public static PDataType hiveTypeToPDataType(PrimitiveObjectInspector.PrimitiveCategory hiveType)
            throws SerDeException {
        /* TODO check backward type compatibility prior to hive 0.12 */

        if (hiveType == null) {
            return null;
        }
        switch (hiveType) {
        case BOOLEAN:
            return PBoolean.INSTANCE;
        case BYTE:
            return PBinary.INSTANCE;
        case DATE:
            return PDate.INSTANCE;
        case DECIMAL:
            return PDecimal.INSTANCE;
        case DOUBLE:
            return PDouble.INSTANCE;
        case FLOAT:
            return PFloat.INSTANCE;
        case INT:
            return PInteger.INSTANCE;
        case LONG:
            return PLong.INSTANCE;
        case SHORT:
            return PSmallint.INSTANCE;
        case STRING:
            return PVarchar.INSTANCE;
        case TIMESTAMP:
            return PTimestamp.INSTANCE;
        case VARCHAR:
            return PVarchar.INSTANCE;
        case VOID:
            return PChar.INSTANCE;
        case UNKNOWN:
            throw new RuntimeException("Unknown primitive");
        default:
            new SerDeException("Unrecognized column type: " + hiveType);
        }
        return null;
    }

    /**
     * This method encodes a value with Phoenix data type. It begins with checking whether an object
     * is BINARY and makes a call to {@link #castBytes(Object, PDataType)} to convert bytes to
     * targetPhoenixType
     * @param o
     * @param targetPhoenixType
     * @return Object
     */
    public static Object castHiveTypeToPhoenix(Object o, byte objectType,
            PDataType targetPhoenixType) {
        /*
         * PDataType inferredPType = getType(o, objectType); if (inferredPType == null) { return
         * null; } if (inferredPType == PDataType.VARBINARY && targetPhoenixType !=
         * PDataType.VARBINARY) { try { o = castBytes(o, targetPhoenixType); inferredPType =
         * getType(o, DataType.findType(o)); } catch (IOException e) { throw new
         * RuntimeException("Error while casting bytes for object " + o); } } if (inferredPType ==
         * PDataType.DATE) { int inferredSqlType = targetPhoenixType.getSqlType(); if
         * (inferredSqlType == Types.DATE) { return new Date(((DateTime)o).getMillis()); } if
         * (inferredSqlType == Types.TIME) { return new Time(((DateTime)o).getMillis()); } if
         * (inferredSqlType == Types.TIMESTAMP) { return new Timestamp(((DateTime)o).getMillis()); }
         * } if (targetPhoenixType == inferredPType ||
         * inferredPType.isCoercibleTo(targetPhoenixType)) { return inferredPType.toObject(o,
         * targetPhoenixType); } throw new RuntimeException(o.getClass().getName() +
         * " cannot be coerced to " + targetPhoenixType.toString());
         */
        return null;
    }

    /**
     * This method converts bytes to the target type required for Phoenix. It uses
     * {@link Utf8StorageConverter} for the conversion.
     * @param o
     * @param targetPhoenixType
     * @return Object
     * @throws IOException
     */
    /*
     * public static Object castBytes(Object o, PDataType targetPhoenixType) throws IOException {
     * byte[] bytes = ((DataByteArray)o).get(); switch (targetPhoenixType) { case CHAR: case
     * VARCHAR: return utf8Converter.bytesToCharArray(bytes); case UNSIGNED_SMALLINT: case SMALLINT:
     * return utf8Converter.bytesToInteger(bytes).shortValue(); case UNSIGNED_TINYINT: case TINYINT:
     * return utf8Converter.bytesToInteger(bytes).byteValue(); case UNSIGNED_INT: case INTEGER:
     * return utf8Converter.bytesToInteger(bytes); case BOOLEAN: return
     * utf8Converter.bytesToBoolean(bytes); case DECIMAL: return
     * utf8Converter.bytesToBigDecimal(bytes); case FLOAT: case UNSIGNED_FLOAT: return
     * utf8Converter.bytesToFloat(bytes); case DOUBLE: case UNSIGNED_DOUBLE: return
     * utf8Converter.bytesToDouble(bytes); case UNSIGNED_LONG: case LONG: return
     * utf8Converter.bytesToLong(bytes); case TIME: case TIMESTAMP: case DATE: case UNSIGNED_TIME:
     * case UNSIGNED_TIMESTAMP: case UNSIGNED_DATE: return utf8Converter.bytesToDateTime(bytes);
     * default: return o; } }
     */
}
