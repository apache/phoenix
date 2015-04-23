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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.MetaException;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class PhoenixUtil {
    static Log LOG = LogFactory.getLog(PhoenixUtil.class.getName());

    public static boolean createTable(Connection conn, String TableName,
            Map<String, String> fields, String[] pks, boolean addIfNotExists, int salt_buckets,
            String compression,int versions_num) throws SQLException, MetaException {
        Preconditions.checkNotNull(conn);
        if (pks == null || pks.length == 0) {
            throw new SQLException("Phoenix Table no Rowkeys specified in "
                    + HiveConfigurationUtil.PHOENIX_ROWKEYS);
        }
        for (String pk : pks) {
            String val = fields.get(pk.toLowerCase());
            if (val == null) {
                throw new MetaException("Phoenix Table rowkey " + pk
                        + " does not belong to listed fields ");
            }
            val += " not null";
            fields.put(pk, val);
        }

        StringBuffer query = new StringBuffer("CREATE TABLE ");
        if (addIfNotExists) {
            query.append("IF NOT EXISTS ");
        }
        query.append(TableName + " ( ");
        Joiner.MapJoiner mapJoiner = Joiner.on(',').withKeyValueSeparator(" ");
        query.append(" " + mapJoiner.join(fields));
        if (pks != null && pks.length > 0) {
            query.append(" CONSTRAINT pk PRIMARY KEY (");
            Joiner joiner = Joiner.on(" , ");
            query.append(" " + joiner.join(pks) + " )");
        }
        query.append(" )");
        if (salt_buckets > 0) {
            query.append(" SALT_BUCKETS = " + salt_buckets);
        }
        if (compression != null) {
            query.append(" ,COMPRESSION='GZ'");
        }
        if (versions_num > 0) {
            query.append(" ,VERSIONS="+versions_num);
        }
        System.out.println("CREATED QUERY " +query.toString());
        LOG.info("Create table query statement " + query.toString());
        return createTable(conn, query.toString());
    }

    public static boolean createTable(Connection conn, String query) throws SQLException {
        Preconditions.checkNotNull(conn);
        return conn.createStatement().execute(query);
    }

    public static boolean findTable(Connection conn, String name) throws SQLException {
        Preconditions.checkNotNull(conn);
        Preconditions.checkNotNull(name);
        DatabaseMetaData dbm = conn.getMetaData();
        ResultSet rs = dbm.getTables(null, null, name, null);
        LOG.info("looking for table");
        if (rs.next()) {
            LOG.info("found the table " + rs.getString("TABLE_NAME"));
            while (rs.next()) {
                LOG.info("found the table " + rs.getString("TABLE_NAME"));
            }
            return true;
        }
        return false;
    }

    public static boolean testTable(Connection conn, String name, Map<String, String> fields)
            throws SQLException, MetaException {
        Preconditions.checkNotNull(conn);
        Preconditions.checkNotNull(name);
        DatabaseMetaData dbm = conn.getMetaData();
        ResultSet rs = dbm.getTables(null, null, name, null);
        ResultSet cols = dbm.getColumns(null, null, name, null);
        Map<String, String> columns = new LinkedHashMap<String, String>();
        while (cols.next()) {
            columns.put(cols.getString("COLUMN_NAME"), cols.getString("TYPE_NAME"));
        }
        if (columns.size() != fields.size()) {
            throw new MetaException("Rowcount mismatch between Hive and Phoenix tables");
        }
        if (PhoenixUtil.compareColumns(columns, fields)) {
            throw new MetaException("Row order mismatch between Hive and Phoenix tables phoenix cols "+columns.toString()+" hive fields "+fields.toString());
        }

        if (columns.equals(fields)) {
            throw new MetaException("Row type mismatch between Hive and Phoenix tables");
        }
        return true;
    }

    private static Boolean compareColumns(Map<String, String> col1, Map<String, String> col2) {
        assert col1.size() == col2.size() : " size mismatch";
        Iterator<String> keys1 = col1.keySet().iterator();
        Iterator<String> keys2 = col2.keySet().iterator();
        boolean result = true;
        while (keys1.hasNext()) {
            String k1 = keys1.next();
            String k2 = keys2.next();
            if (!k1.toLowerCase().equals(k2.toLowerCase())) {
                result = false;
            }
        }
        return result;
    }

    public static boolean dropTable(Connection conn, String TableName) throws SQLException {
        Preconditions.checkNotNull(conn);
        return conn.createStatement().execute("DROP TABLE IF EXISTS " + TableName);
    }

    public ResultSet getAll(Connection conn, String TableName, String predicate)
            throws SQLException {
        Preconditions.checkNotNull(conn);
        String query = "SELECT * FROM " + TableName;
        if (!predicate.isEmpty()) {
            query += " where " + predicate;
        }
        ResultSet rs = conn.createStatement().executeQuery(query);
        ;
        return rs;
    }

}
