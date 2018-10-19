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
package org.apache.phoenix.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.phoenix.end2end.BaseAggregateIT;
import org.apache.phoenix.util.QueryBuilder;

public class AggregateIT extends BaseAggregateIT {

    @Override
    protected ResultSet executeQueryThrowsException(Connection conn, QueryBuilder queryBuilder,
        String expectedPhoenixExceptionMsg, String expectedSparkExceptionMsg) {
        ResultSet rs = null;
        try {
            rs = executeQuery(conn, queryBuilder);
            fail();
        }
        catch(Exception e) {
            assertTrue(e.getMessage().contains(expectedSparkExceptionMsg));
        }
        return rs;
    }

    @Override
    protected ResultSet executeQuery(Connection conn, QueryBuilder queryBuilder) throws SQLException {
        return SparkUtil.executeQuery(conn, queryBuilder, getUrl(), config);
    }

    @Override
    protected void testCountNullInNonEmptyKeyValueCF(int columnEncodedBytes) throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            //Type is INT
            String intTableName=generateUniqueName();
            String sql="create table " + intTableName + " (mykey integer not null primary key, A.COLA integer, B.COLB integer) "
                    + "IMMUTABLE_ROWS=true, IMMUTABLE_STORAGE_SCHEME = ONE_CELL_PER_COLUMN, COLUMN_ENCODED_BYTES = " + columnEncodedBytes + ", DISABLE_WAL=true";

            conn.createStatement().execute(sql);
            conn.createStatement().execute("UPSERT INTO "+intTableName+" VALUES (1,1)");
            conn.createStatement().execute("UPSERT INTO "+intTableName+" VALUES (2,1)");
            conn.createStatement().execute("UPSERT INTO "+intTableName+" VALUES (3,1,2)");
            conn.createStatement().execute("UPSERT INTO "+intTableName+" VALUES (4,1)");
            conn.createStatement().execute("UPSERT INTO "+intTableName+" VALUES (5,1)");
            conn.commit();

            sql="select count(*) from "+intTableName;
            QueryBuilder queryBuilder = new QueryBuilder()
                .setSelectExpression("COUNT(*)")
                .setFullTableName(intTableName);
            ResultSet rs = executeQuery(conn, queryBuilder);
            assertTrue(rs.next());
            assertEquals(5, rs.getLong(1));

            sql="select count(*) from "+intTableName + " where b.colb is not null";
            queryBuilder.setWhereClause("`B.COLB` IS NOT NULL");
            rs = executeQuery(conn, queryBuilder);
            assertTrue(rs.next());
            assertEquals(1, rs.getLong(1));

            sql="select count(*) from "+intTableName + " where b.colb is null";
            queryBuilder.setWhereClause("`B.COLB` IS NULL");
            rs = executeQuery(conn, queryBuilder);
            assertTrue(rs.next());
            assertEquals(4, rs.getLong(1));
        }
    }

}
