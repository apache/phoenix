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
package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.ROW1;

import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;

import org.apache.phoenix.query.BaseTest;

public abstract class ArrayIT extends ParallelStatsDisabledIT {

    protected static String createTableWithArray(String url, byte[][] bs, Object object) throws SQLException {
        String tableName = generateUniqueName();
        String ddlStmt = "create table "
                + tableName
                + "   (organization_id char(15) not null, \n"
                + "    entity_id char(15) not null,\n"
                + "    a_string_array varchar(100) array[3],\n"
                + "    b_string varchar(100),\n"
                + "    a_integer integer,\n"
                + "    a_date date,\n"
                + "    a_time time,\n"
                + "    a_timestamp timestamp,\n"
                + "    x_decimal decimal(31,10),\n"
                + "    x_long_array bigint[5],\n"
                + "    x_integer integer,\n"
                + "    a_byte_array tinyint array,\n"
                + "    a_short smallint,\n"
                + "    a_float float,\n"
                + "    a_double_array double array[],\n"
                + "    a_unsigned_float unsigned_float,\n"
                + "    a_unsigned_double unsigned_double \n"
                + "    CONSTRAINT pk PRIMARY KEY (organization_id, entity_id)\n"
                + ")";
        BaseTest.createTestTable(url, ddlStmt, bs, null);
        return tableName;
    }

    protected static void initTablesWithArrays(String tableName, String tenantId, Date date, boolean useNull, String url) throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(url, props);
        try {
            // Insert all rows at ts
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " + tableName + 
                            "(" +
                            "    ORGANIZATION_ID, " +
                            "    ENTITY_ID, " +
                            "    a_string_array, " +
                            "    B_STRING, " +
                            "    A_INTEGER, " +
                            "    A_DATE, " +
                            "    X_DECIMAL, " +
                            "    x_long_array, " +
                            "    X_INTEGER," +
                            "    a_byte_array," +
                            "    A_SHORT," +
                            "    A_FLOAT," +
                            "    a_double_array," +
                            "    A_UNSIGNED_FLOAT," +
                            "    A_UNSIGNED_DOUBLE)" +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW1);
            // Need to support primitive
            String[] strArr =  new String[4];
            strArr[0] = "ABC";
            if (useNull) {
                strArr[1] = null;
            } else {
                strArr[1] = "CEDF";
            }
            strArr[2] = "XYZWER";
            strArr[3] = "AB";
            Array array = conn.createArrayOf("VARCHAR", strArr);
            stmt.setArray(3, array);
            stmt.setString(4, B_VALUE);
            stmt.setInt(5, 1);
            stmt.setDate(6, date);
            stmt.setBigDecimal(7, null);
            // Need to support primitive
            Long[] longArr =  new Long[2];
            longArr[0] = 25l;
            longArr[1] = 36l;
            array = conn.createArrayOf("BIGINT", longArr);
            stmt.setArray(8, array);
            stmt.setNull(9, Types.INTEGER);
            // Need to support primitive
            Byte[] byteArr =  new Byte[2];
            byteArr[0] = 25;
            byteArr[1] = 36;
            array = conn.createArrayOf("TINYINT", byteArr);
            stmt.setArray(10, array);
            stmt.setShort(11, (short) 128);
            stmt.setFloat(12, 0.01f);
            // Need to support primitive
            Double[] doubleArr =  new Double[4];
            doubleArr[0] = 25.343;
            doubleArr[1] = 36.763;
            doubleArr[2] = 37.56;
            doubleArr[3] = 386.63;
            array = conn.createArrayOf("DOUBLE", doubleArr);
            stmt.setArray(13, array);
            stmt.setFloat(14, 0.01f);
            stmt.setDouble(15, 0.0001);
            stmt.execute();

            conn.commit();
        } finally {
            conn.close();
        }
    }
}
