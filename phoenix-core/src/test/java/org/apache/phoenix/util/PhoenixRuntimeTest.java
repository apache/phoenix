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

package org.apache.phoenix.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.util.Arrays;

import com.google.common.collect.ImmutableList;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.junit.Test;

public class PhoenixRuntimeTest extends BaseConnectionlessQueryTest {
    @Test
    public void testEncodeDecode() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(
                "CREATE TABLE t(org_id CHAR(3) not null, p_id CHAR(3) not null, date DATE not null, e_id CHAR(3) not null, old_value VARCHAR, new_value VARCHAR " +
                "CONSTRAINT pk PRIMARY KEY (org_id, p_id, date, e_id))");
        Date date = new Date(System.currentTimeMillis());
        Object[] expectedValues = new Object[] {"abc", "def", date, "123"};
        byte[] value = PhoenixRuntime.encodePK(conn, "T", expectedValues);
        Object[] actualValues = PhoenixRuntime.decodePK(conn, "T", value);
        assertEquals(Arrays.asList(expectedValues), Arrays.asList(actualValues));
    }

    @Test
    public void testParseArguments_MinimalCase() {
        PhoenixRuntime.ExecutionCommand execCmd = PhoenixRuntime.ExecutionCommand.parseArgs(
                new String[] { "localhost", "test.csv" });


        assertEquals(
                "localhost",
                execCmd.getConnectionString());

        assertEquals(
                ImmutableList.of("test.csv"),
                execCmd.getInputFiles());

        assertEquals(',', execCmd.getFieldDelimiter());
        assertEquals('"', execCmd.getQuoteCharacter());
        assertNull(execCmd.getEscapeCharacter());

        assertNull(execCmd.getTableName());

        assertNull(execCmd.getColumns());

        assertFalse(execCmd.isStrict());

        assertEquals(
                CSVCommonsLoader.DEFAULT_ARRAY_ELEMENT_SEPARATOR,
                execCmd.getArrayElementSeparator());
    }

    @Test
    public void testParseArguments_FullOption() {
        PhoenixRuntime.ExecutionCommand execCmd = PhoenixRuntime.ExecutionCommand.parseArgs(
                new String[] { "-t", "mytable", "myzkhost:2181",  "--strict", "file1.sql",
                        "test.csv", "file2.sql", "--header", "one, two,three", "-a", "!", "-d",
                        ":", "-q", "3", "-e", "4" });

        assertEquals("myzkhost:2181", execCmd.getConnectionString());

        assertEquals(ImmutableList.of("file1.sql", "test.csv", "file2.sql"),
                execCmd.getInputFiles());

        assertEquals(':', execCmd.getFieldDelimiter());
        assertEquals('3', execCmd.getQuoteCharacter());
        assertEquals(Character.valueOf('4'), execCmd.getEscapeCharacter());

        assertEquals("mytable", execCmd.getTableName());

        assertEquals(ImmutableList.of("one", "two", "three"), execCmd.getColumns());
        assertTrue(execCmd.isStrict());
        assertEquals("!", execCmd.getArrayElementSeparator());
    }
}
