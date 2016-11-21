/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive.util;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import javax.naming.NamingException;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat.Options;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.net.DNS;
import org.apache.phoenix.hive.constants.PhoenixStorageHandlerConstants;
import org.apache.phoenix.hive.ql.index.IndexSearchCondition;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;

/**
 * Misc utils for PhoenixStorageHandler
 */

public class PhoenixStorageHandlerUtil {

    public static String getTargetTableName(Table table) {
        Map<String, String> tableParameterMap = table.getParameters();
        String tableName = tableParameterMap.get(PhoenixStorageHandlerConstants
                .PHOENIX_TABLE_NAME);
        if (tableName == null) {
            tableName = table.getTableName();
            tableParameterMap.put(PhoenixStorageHandlerConstants.PHOENIX_TABLE_NAME, tableName);
        }

        return tableName;
    }


    public static Object[] toTypedValues(JobConf jobConf, String typeName, String[] values) throws
            Exception {
        Object[] results = new Object[values.length];
        DateFormat df = null;

        for (int i = 0, limit = values.length; i < limit; i++) {
            if (serdeConstants.STRING_TYPE_NAME.equals(typeName) ||
                    typeName.startsWith(serdeConstants.CHAR_TYPE_NAME) ||
                    typeName.startsWith(serdeConstants.VARCHAR_TYPE_NAME)) {
                results[i] = values[i];
            } else if (serdeConstants.INT_TYPE_NAME.equals(typeName)) {
                results[i] = new Integer(values[i]);
            } else if (serdeConstants.BIGINT_TYPE_NAME.equals(typeName)) {
                results[i] = new Long(values[i]);
            } else if (serdeConstants.DOUBLE_TYPE_NAME.equals(typeName)) {
                results[i] = new Double(values[i]);
            } else if (serdeConstants.FLOAT_TYPE_NAME.equals(typeName)) {
                results[i] = new Float(values[i]);
            } else if (serdeConstants.SMALLINT_TYPE_NAME.equals(typeName)) {
                results[i] = new Short(values[i]);
            } else if (serdeConstants.TINYINT_TYPE_NAME.equals(typeName)) {
                results[i] = new Byte(values[i]);
            } else if (serdeConstants.DATE_TYPE_NAME.equals(typeName)) {
                String dateFormat = jobConf.get(PhoenixStorageHandlerConstants.HBASE_DATE_FORMAT,
                        PhoenixStorageHandlerConstants.DEFAULT_DATE_FORMAT);
                df = new SimpleDateFormat(dateFormat);
                results[i] = new Long(df.parse(values[i]).getTime());
            } else if (serdeConstants.TIMESTAMP_TYPE_NAME.equals(typeName)) {
                String timestampFormat = jobConf.get(PhoenixStorageHandlerConstants
                        .HBASE_TIMESTAMP_FORMAT, PhoenixStorageHandlerConstants
                        .DEFAULT_TIMESTAMP_FORMAT);
                df = new SimpleDateFormat(timestampFormat);
                results[i] = new Long(df.parse(values[i]).getTime());
            } else if (typeName.contains(serdeConstants.DECIMAL_TYPE_NAME)) {
                results[i] = new BigDecimal(values[i]);
            }
        }

        return results;
    }

    public static String[] getConstantValues(IndexSearchCondition condition, String comparisonOp) {
        String[] constantValues = null;

        if (comparisonOp.endsWith("UDFOPEqual") || comparisonOp.endsWith("UDFOPNotEqual")) {
            constantValues = new String[]{String.valueOf(condition.getConstantDesc().getValue())};
        } else if (comparisonOp.endsWith("UDFOPEqualOrGreaterThan")) {    // key >= 1
            constantValues = new String[]{String.valueOf(condition.getConstantDesc().getValue())};
        } else if (comparisonOp.endsWith("UDFOPGreaterThan")) {        // key > 1
            constantValues = new String[]{String.valueOf(condition.getConstantDesc().getValue())};
        } else if (comparisonOp.endsWith("UDFOPEqualOrLessThan")) {    // key <= 1
            constantValues = new String[]{String.valueOf(condition.getConstantDesc().getValue())};
        } else if (comparisonOp.endsWith("UDFOPLessThan")) {    // key < 1
            constantValues = new String[]{String.valueOf(condition.getConstantDesc().getValue())};
        } else if (comparisonOp.endsWith("GenericUDFBetween")) {
            constantValues = new String[]{String.valueOf(condition.getConstantDesc(0).getValue()),
                    String.valueOf(condition.getConstantDesc(1).getValue())};
        } else if (comparisonOp.endsWith("GenericUDFIn")) {
            ExprNodeConstantDesc[] constantDescs = condition.getConstantDescs();
            constantValues = new String[constantDescs.length];
            for (int i = 0, limit = constantDescs.length; i < limit; i++) {
                constantValues[i] = String.valueOf(condition.getConstantDesc(i).getValue());
            }
        }

        return constantValues;
    }

    public static String getRegionLocation(HRegionLocation location, Log log) throws IOException {
        InetSocketAddress isa = new InetSocketAddress(location.getHostname(), location.getPort());
        if (isa.isUnresolved()) {
            log.warn("Failed resolve " + isa);
        }
        InetAddress regionAddress = isa.getAddress();
        String regionLocation = null;
        try {
            regionLocation = reverseDNS(regionAddress);
        } catch (NamingException e) {
            log.warn("Cannot resolve the host name for " + regionAddress + " because of " + e);
            regionLocation = location.getHostname();
        }

        return regionLocation;
    }

    // Copy from org.apache.hadoop.hbase.mapreduce.TableInputFormatBase.reverseDNS
    private static final Map<InetAddress, String> reverseDNSCacheMap = Maps.newConcurrentMap();

    private static String reverseDNS(InetAddress ipAddress) throws NamingException,
            UnknownHostException {
        String hostName = reverseDNSCacheMap.get(ipAddress);

        if (hostName == null) {
            String ipAddressString = null;
            try {
                ipAddressString = DNS.reverseDns(ipAddress, null);
            } catch (Exception e) {
                // We can use InetAddress in case the jndi failed to pull up the reverse DNS entry
                // from the name service. Also, in case of ipv6, we need to use the InetAddress
                // since resolving reverse DNS using jndi doesn't work well with ipv6 addresses.
                ipAddressString = InetAddress.getByName(ipAddress.getHostAddress()).getHostName();
            }

            if (ipAddressString == null) {
                throw new UnknownHostException("No host found for " + ipAddress);
            }

            hostName = Strings.domainNamePointerToHostName(ipAddressString);
            reverseDNSCacheMap.put(ipAddress, hostName);
        }

        return hostName;
    }

    public static String getTableKeyOfSession(JobConf jobConf, String tableName) {

        String sessionId = jobConf.get(PhoenixConfigurationUtil.SESSION_ID);
        return new StringBuilder("[").append(sessionId).append("]-").append(tableName).toString();
    }

    public static Map<String, TypeInfo> createColumnTypeMap(JobConf jobConf) {
        Map<String, TypeInfo> columnTypeMap = Maps.newHashMap();

        String[] columnNames = jobConf.get(serdeConstants.LIST_COLUMNS).split
                (PhoenixStorageHandlerConstants.COMMA);
        List<TypeInfo> typeInfos =
                TypeInfoUtils.getTypeInfosFromTypeString(jobConf.get(serdeConstants.LIST_COLUMN_TYPES));

        for (int i = 0, limit = columnNames.length; i < limit; i++) {
            columnTypeMap.put(columnNames[i], typeInfos.get(i));
        }

        return columnTypeMap;
    }

    public static List<String> getReadColumnNames(Configuration conf) {
        String colNames = conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR);
        if (colNames != null && !colNames.isEmpty()) {
            return Arrays.asList(colNames.split(PhoenixStorageHandlerConstants.COMMA));
        }
        return Collections.EMPTY_LIST;
    }

    public static boolean isTransactionalTable(Properties tableProperties) {
        String tableIsTransactional = tableProperties.getProperty(hive_metastoreConstants
                .TABLE_IS_TRANSACTIONAL);

        return tableIsTransactional != null && tableIsTransactional.equalsIgnoreCase("true");
    }

    public static boolean isTransactionalTable(Configuration config) {
        String tableIsTransactional = config.get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL);

        return tableIsTransactional != null && tableIsTransactional.equalsIgnoreCase("true");
    }

    public static void printConfiguration(Configuration config) {
        if (Boolean.getBoolean("dev")) {
            for (Iterator<Entry<String, String>> iterator = config.iterator(); iterator.hasNext();
                    ) {
                Entry<String, String> entry = iterator.next();

                System.out.println(entry.getKey() + "=" + entry.getValue());
            }
        }
    }

    public static String toString(Object obj) {
        String content = null;

        if (obj instanceof Array) {
            Object[] values = (Object[]) obj;

            content = Joiner.on(PhoenixStorageHandlerConstants.COMMA).join(values);
        } else {
            content = obj.toString();
        }

        return content;
    }

    public static Map<?, ?> toMap(byte[] serialized) {
        Map<?, ?> resultMap = null;
        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);

        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            resultMap = (Map<?, ?>) ois.readObject();
        } catch (ClassNotFoundException | IOException e) {
            throw new RuntimeException(e);
        }

        return resultMap;
    }

    public static String getOptionsValue(Options options) {
        StringBuilder content = new StringBuilder();

        int bucket = options.getBucket();
        String inspectorInfo = options.getInspector().getCategory() + ":" + options.getInspector()
                .getTypeName();
        long maxTxnId = options.getMaximumTransactionId();
        long minTxnId = options.getMinimumTransactionId();
        int recordIdColumn = options.getRecordIdColumn();
        boolean isCompresses = options.isCompressed();
        boolean isWritingBase = options.isWritingBase();

        content.append("bucket : ").append(bucket).append(", inspectorInfo : ").append
                (inspectorInfo).append(", minTxnId : ").append(minTxnId).append(", maxTxnId : ")
                .append(maxTxnId).append(", recordIdColumn : ").append(recordIdColumn);
        content.append(", isCompressed : ").append(isCompresses).append(", isWritingBase : ")
                .append(isWritingBase);

        return content.toString();
    }
}
