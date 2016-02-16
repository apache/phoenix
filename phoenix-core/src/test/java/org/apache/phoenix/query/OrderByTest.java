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
package org.apache.phoenix.query;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;

public class OrderByTest extends BaseConnectionlessQueryTest {
    @Test
    public void testSortOrderForSingleDescVarLengthCol() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k VARCHAR PRIMARY KEY DESC)");
        conn.createStatement().execute("UPSERT INTO t VALUES ('a')");
        conn.createStatement().execute("UPSERT INTO t VALUES ('ab')");

        Iterator<Pair<byte[],List<KeyValue>>> dataIterator = PhoenixRuntime.getUncommittedDataIterator(conn);
        List<KeyValue> kvs = dataIterator.next().getSecond();
        Collections.sort(kvs, KeyValue.COMPARATOR);
        KeyValue first = kvs.get(0);
        assertEquals("ab", Bytes.toString(SortOrder.invert(first.getRowArray(), first.getRowOffset(), first.getRowLength()-1)));
        KeyValue second = kvs.get(1);
        assertEquals("a", Bytes.toString(SortOrder.invert(second.getRowArray(), second.getRowOffset(), second.getRowLength()-1)));
    }

    @Test
    public void testSortOrderForLeadingDescVarLengthColWithNullFollowing() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k1 VARCHAR, k2 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1 DESC,k2))");
        conn.createStatement().execute("UPSERT INTO t VALUES ('a')");
        conn.createStatement().execute("UPSERT INTO t VALUES ('ab')");

        Iterator<Pair<byte[],List<KeyValue>>> dataIterator = PhoenixRuntime.getUncommittedDataIterator(conn);
        List<KeyValue> kvs = dataIterator.next().getSecond();
        Collections.sort(kvs, KeyValue.COMPARATOR);
        KeyValue first = kvs.get(0);
        assertEquals("ab", Bytes.toString(SortOrder.invert(first.getRowArray(), first.getRowOffset(), first.getRowLength()-1)));
        KeyValue second = kvs.get(1);
        assertEquals("a", Bytes.toString(SortOrder.invert(second.getRowArray(), second.getRowOffset(), second.getRowLength()-1)));
    }

    @Test
    public void testSortOrderForLeadingDescVarLengthColWithNonNullFollowing() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k1 VARCHAR, k2 VARCHAR NOT NULL, CONSTRAINT pk PRIMARY KEY (k1 DESC,k2))");
        conn.createStatement().execute("UPSERT INTO t VALUES ('a','x')");
        conn.createStatement().execute("UPSERT INTO t VALUES ('ab', 'x')");

        Iterator<Pair<byte[],List<KeyValue>>> dataIterator = PhoenixRuntime.getUncommittedDataIterator(conn);
        List<KeyValue> kvs = dataIterator.next().getSecond();
        Collections.sort(kvs, KeyValue.COMPARATOR);
        KeyValue first = kvs.get(0);
        assertEquals("ab", Bytes.toString(SortOrder.invert(first.getRowArray(), first.getRowOffset(), 2)));
        KeyValue second = kvs.get(1);
        assertEquals("a", Bytes.toString(SortOrder.invert(second.getRowArray(), second.getRowOffset(), 1)));
    }

    @Test
    public void testSortOrderForSingleDescTimestampCol() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k TIMESTAMP PRIMARY KEY DESC)");
        conn.createStatement().execute("UPSERT INTO t VALUES ('2016-01-04 13:11:51.631')");

        Iterator<Pair<byte[], List<KeyValue>>> dataIterator = PhoenixRuntime
            .getUncommittedDataIterator(conn);
        List<KeyValue> kvs = dataIterator.next().getSecond();
        Collections.sort(kvs, KeyValue.COMPARATOR);
        KeyValue first = kvs.get(0);
        long millisDeserialized = PDate.INSTANCE.getCodec().decodeLong(first.getRowArray(),
            first.getRowOffset(), SortOrder.DESC);
        assertEquals(1451913111631L, millisDeserialized);
  }
}
