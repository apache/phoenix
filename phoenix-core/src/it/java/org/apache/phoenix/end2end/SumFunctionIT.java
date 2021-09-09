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

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@Category(ParallelStatsDisabledTest.class)
public class SumFunctionIT extends ParallelStatsDisabledIT {
    @Test
    public void testSumFunctionWithCaseWhenStatement() throws Exception {
        String tableName = generateUniqueName();

        try (Connection c = DriverManager.getConnection(getUrl());
          Statement s = c.createStatement()) {
            s.execute("create table " + tableName + " (id varchar primary key, col1 varchar, "
              + "col2 integer)");
            s.execute("upsert into " + tableName + " values('id1', 'aaa', 2)");
            s.execute("upsert into " + tableName + " values('id2', null, 1)");
            c.commit();

            try (ResultSet rs = s.executeQuery(
              "select sum(case when col1 is null then col2 else 0 end), "
                + "sum(case when col1 is not null then col2 else 0 end) from " + tableName)) {

                assertThat(rs.next(), is(true));
                assertThat(rs.getInt(1), is(1));
                assertThat(rs.getInt(2), is(2));
                assertThat(rs.next(), is(false));
            }
        }
    }
}
