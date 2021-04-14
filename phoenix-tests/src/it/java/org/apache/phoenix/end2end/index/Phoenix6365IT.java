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
package org.apache.phoenix.end2end.index;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.phoenix.util.TestUtil;
import org.junit.Test;


public class Phoenix6365IT extends BaseLocalIndexIT {
    public Phoenix6365IT(boolean isNamespaceMapped) {
        super(isNamespaceMapped);
    }

    @Test
    public void minimal( ) throws SQLException {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE customer "
                    + "(customer_id integer primary key, postal_code varchar, country_code varchar)");

            stmt.execute("CREATE LOCAL INDEX customer_local_index ON CUSTOMER (postal_code)");

            stmt.executeUpdate("UPSERT INTO customer values (1, '1121', 'HU')");
            stmt.executeUpdate("UPSERT INTO customer values (2, '2323', 'HU')");
            stmt.executeUpdate("UPSERT INTO customer values (3, '1121', 'UK')");
            stmt.executeUpdate("UPSERT INTO customer values (4, '1121', 'HU')");

            conn.commit();

            String sql = "SELECT c1.customer_id, c2.customer_id, c1.postal_code, c2.postal_code,"
                    + " c1.country_code, c2.country_code from customer c1, customer c2"
                    + " where c1.customer_id=c2.customer_id and c2.postal_code='1121'";
            
            ResultSet rs = stmt.executeQuery(sql);
            assertTrue(rs.next());
        }
    }
    
    @Test
    public void Phoenix6365( ) throws SQLException {
        
        String primaryTableName = generateUniqueName();
        String secondaryTableName = generateUniqueName();
        
        String primaryGlobalIndexName = generateUniqueName();
        String primaryLocalIndexName = generateUniqueName();

        String secondaryGlobalIndexName = generateUniqueName();
        String secondaryLocalIndexName = generateUniqueName();
        
        String createPrimary = "CREATE TABLE IF NOT EXISTS %s (ID INTEGER PRIMARY KEY, "+
            "unsig_id UNSIGNED_INT, "+
            "big_id BIGINT, "+
            "unsig_long_id UNSIGNED_LONG, "+
            "tiny_id TINYINT, "+
            "unsig_tiny_id UNSIGNED_TINYINT,"+
            "small_id SMALLINT, "+
            "unsig_small_id UNSIGNED_SMALLINT, "+
            "float_id FLOAT, "+
            "unsig_float_id UNSIGNED_FLOAT, "+
            "double_id DOUBLE, "+
            "unsig_double_id UNSIGNED_DOUBLE, "+
            "decimal_id DECIMAL, "+
            "boolean_id BOOLEAN, "+
            "time_id TIME, "+
            "date_id DATE, "+
            "timestamp_id TIMESTAMP, "+
            "unsig_time_id UNSIGNED_TIME, "+
            "unsig_date_id UNSIGNED_DATE, "+
            "unsig_timestamp_id UNSIGNED_TIMESTAMP, "+
            "varchar_id VARCHAR (30), "+
            "char_id CHAR (30), "+
            "binary_id BINARY (100), "+
            "varbinary_id VARBINARY (100), "+
            "array_id VARCHAR[])";
        
        String createSecondary = 
                "CREATE TABLE IF NOT EXISTS %s (SEC_ID INTEGER PRIMARY KEY," +
                "sec_unsig_id UNSIGNED_INT," +
                "sec_big_id BIGINT," +
                "sec_usnig_long_id UNSIGNED_LONG," +
                "sec_tiny_id TINYINT," +
                "sec_unsig_tiny_id UNSIGNED_TINYINT," +
                "sec_small_id SMALLINT," +
                "sec_unsig_small_id UNSIGNED_SMALLINT," +
                "sec_float_id FLOAT," +
                "sec_unsig_float_id UNSIGNED_FLOAT," +
                "sec_double_id DOUBLE," +
                "sec_unsig_double_id UNSIGNED_DOUBLE," +
                "sec_decimal_id DECIMAL," +
                "sec_boolean_id BOOLEAN," +
                "sec_time_id TIME," +
                "sec_date_id DATE," +
                "sec_timestamp_id TIMESTAMP," +
                "sec_unsig_time_id UNSIGNED_TIME," +
                "sec_unsig_date_id UNSIGNED_DATE," +
                "sec_unsig_timestamp_id UNSIGNED_TIMESTAMP," +
                "sec_varchar_id VARCHAR (30)," +
                "sec_char_id CHAR (30)," +
                "sec_binary_id BINARY (100)," +
                "sec_varbinary_id VARBINARY (100)," +
                "sec_array_id VARCHAR[])";
        
        String createGlobalIndexOnPrimary = "CREATE INDEX %s ON %s (unsig_id)";
        
        String createLocalIndexOnPrimary = "CREATE LOCAL INDEX %s ON %s (unsig_small_id)";
        
        String createGlobalIndexOnSecondary = "CREATE INDEX %s ON %s (sec_unsig_id)";
        
        String createLocalIndexOnSecondary = "CREATE LOCAL INDEX %s ON %s (sec_unsig_small_id)";
        
        String upsert = "UPSERT INTO %s values (%s)";
        
        String[] primaryValues = {
                "0, 2, 3, 4, -5, 6, 7, 8, 9.3, 10.4, 11.5, 12.6, 13.7, True, CURRENT_TIME(),"+
                "CURRENT_DATE(), CURRENT_TIME(), CURRENT_TIME(), CURRENT_DATE(), CURRENT_TIME(),"+
                "'This is random textA', 'a', 'VGhpcyBpcyBhIG5vcm1hbCB0ZXN0IA==',"+
                "'VGhpcyBpcyBhIG5vcm1hbCB0ZXN0IA=='",
                
                "1, 2, 2, 40, -8, 6, 7, 8, 9.3, 10.4, 11.5, 12.6, 13.7, False,"+
                "CURRENT_TIME() - 1, CURRENT_DATE() + 1, CURRENT_TIME(), CURRENT_TIME(), CURRENT_DATE(),"+
                "CURRENT_TIME(), 'This is random textB', 'b', 'VGhpcyBpcyBhIG5vcm1hbCB0ZXN0IA==',"+
                "'VGhpcyBpcyBhIG5vcm1hbCB0ZXN0IA=='",
                
                "2, 5, -1, 4000000, -9, 1, 6, 8, 9.3, 10.4, 11.5, 12.6, 13.7, True,"+
                "CURRENT_TIME() - 6, CURRENT_DATE() + 10051, CURRENT_TIME(), CURRENT_TIME(), CURRENT_DATE(),"+
                "CURRENT_TIME(), 'This is random textG', 'g', 'VGhpcyBpcyBhIG5vcm1hbCB0ZXN0IA==',"+
                "'VGhpcyBpcyBhIG5vcm1hbCB0ZXN0IA=='",
                
                "3, null, null, null, null, null, null, null, null, null, null, null, null,"+
                "null, null, null, null, null, null, null, null, null, null, null"
        };
        
        String[] secondaryValues = {
                "0, 2, 3, 4, 5, 6, 7, 8, 9.3, 10.4, 11.5, 12.6, 13.7, True, CURRENT_TIME(),"+
                "CURRENT_DATE(), CURRENT_TIME(), CURRENT_TIME(), CURRENT_DATE(), CURRENT_TIME(),"+
                "'This is random text', 'a', 'VGhpcyBpcyBhIG5vcm1hbCB0ZXN0IA==',"+
                "'VGhpcyBpcyBhIG5vcm1hbCB0ZXN0IA=='",
                
                "2, null, null, null, null, null, null, null, null, null, null, null, null,"+
                "null, null, null, null, null, null, null, null, null, null, null",
                
                "3, 5, 3, 4, -5, 6, 7, 8, 9.3, 10.4, 11.5, 12.6, 13.7, True, CURRENT_TIME(),"+
                "CURRENT_DATE(), CURRENT_TIME(), CURRENT_TIME(), CURRENT_DATE(), CURRENT_TIME(),"+
                "'This is random textA', 'a', 'VGhpcyBpcyBhIG5vcm1hbCB0ZXN0IA==',"+
                "'VGhpcyBpcyBhIG5vcm1hbCB0ZXN0IA=='"
        };
        
        String query = 
                " SELECT A.ID, B.SEC_UNSIG_ID, C.TINY_ID, D.TIME_ID, E.SEC_TIME_ID, F.SEC_BOOLEAN_ID, G.VARBINARY_ID, H.SEC_UNSIG_ID, I.UNSIG_TIMESTAMP_ID, I.UNSIG_SMALL_ID "+
                " FROM " + primaryTableName + " AS A "
              + " INNER JOIN " + secondaryTableName + " AS B ON A.ID = B.SEC_ID "+
                " LEFT OUTER JOIN " + primaryTableName + " AS C ON B.SEC_BIG_ID=C.BIG_ID "+
                " LEFT OUTER JOIN " + primaryTableName + " AS D ON C.TINY_ID=D.TINY_ID "+
                " INNER JOIN " + secondaryTableName + " AS E ON D.TIME_ID=E.SEC_TIME_ID "+
                " INNER JOIN " + secondaryTableName + " AS F ON D.BOOLEAN_ID = F.SEC_BOOLEAN_ID "+
                " RIGHT OUTER JOIN " + primaryTableName + " AS G ON F.SEC_VARBINARY_ID=G.VARBINARY_ID "+
                " RIGHT OUTER JOIN " + secondaryTableName + " AS H ON H.SEC_UNSIG_ID=G.UNSIG_ID "+
                " INNER JOIN " + primaryTableName + " AS I ON I.UNSIG_TIMESTAMP_ID=E.SEC_UNSIG_TIMESTAMP_ID "+
                " INNER JOIN " + primaryTableName + " AS J ON J.UNSIG_SMALL_ID=F.SEC_UNSIG_SMALL_ID "+
                " WHERE A.ID IS NOT NULL " +
                "      AND I.UNSIG_TIMESTAMP_ID IS NOT NULL "+
                "      AND I.UNSIG_SMALL_ID IS NOT NULL "+
                " ORDER BY A.ID DESC";

        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            
            stmt.executeUpdate(String.format(createPrimary, primaryTableName));
            stmt.executeUpdate(String.format(createSecondary, secondaryTableName));
            
            stmt.executeUpdate(String.format(createGlobalIndexOnPrimary, primaryGlobalIndexName, primaryTableName));
            stmt.executeUpdate(String.format(createLocalIndexOnPrimary, primaryLocalIndexName, primaryTableName));

            stmt.executeUpdate(String.format(createGlobalIndexOnSecondary, secondaryGlobalIndexName, secondaryTableName));
            stmt.executeUpdate(String.format(createLocalIndexOnSecondary, secondaryLocalIndexName, secondaryTableName));

            stmt.execute(String.format(upsert, primaryTableName, primaryValues[0]));
            stmt.execute(String.format(upsert, primaryTableName, primaryValues[1]));
            stmt.execute(String.format(upsert, primaryTableName, primaryValues[2]));
            stmt.execute(String.format(upsert, primaryTableName, primaryValues[3]));

            stmt.execute(String.format(upsert, secondaryTableName, secondaryValues[0]));
            stmt.execute(String.format(upsert, secondaryTableName, secondaryValues[1]));
            stmt.execute(String.format(upsert, secondaryTableName, secondaryValues[2]));
            
            conn.commit();
            
            ResultSet rs = stmt.executeQuery(query);
            assertTrue(rs.next());
//            assertEquals("2", rs.getString("A.ID"));
//            assertEquals(null, rs.getString("B.SEC_UNSIG_ID"));
//            assertEquals(null, rs.getString("C.TINY_ID"));
//            assertFalse(rs.getString("D.TIME_ID").equals(null));
//            assertFalse(rs.getString("E.SEC_TIME_ID").equals(null));
//            assertEquals("null", rs.getString("F.SEC_BOOLEAN_ID"));
//            assertFalse(rs.getString("G.VARBINARY_ID").equals(null));
//            assertEquals(null, rs.getString("H.SEC_UNSIG_ID"));
//            assertFalse(rs.getString("G.UNSIG_TIMESTAMP_ID").equals(null));
//            assertEquals("8", rs.getString("I.UNSIG_SMALL_ID"));
        }


    }
}
