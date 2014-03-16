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
package org.apache.phoenix.mapreduce;

import org.apache.commons.cli.CommandLine;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CsvBulkLoadToolTest {

    private CsvBulkLoadTool bulkLoadTool;

    @Before
    public void setUp() {
        bulkLoadTool = new CsvBulkLoadTool();
    }

    @Test
    public void testParseOptions() {
        CommandLine cmdLine = bulkLoadTool.parseOptions(new String[] { "--input", "/input",
                "--table", "mytable" });

        assertEquals("mytable", cmdLine.getOptionValue(CsvBulkLoadTool.TABLE_NAME_OPT.getOpt()));
        assertEquals("/input", cmdLine.getOptionValue(CsvBulkLoadTool.INPUT_PATH_OPT.getOpt()));
    }

    @Test(expected=IllegalStateException.class)
    public void testParseOptions_ExtraArguments() {
        bulkLoadTool.parseOptions(new String[] { "--input", "/input",
                "--table", "mytable", "these", "shouldnt", "be", "here" });
    }

    @Test(expected=IllegalStateException.class)
    public void testParseOptions_NoInput() {
        bulkLoadTool.parseOptions(new String[] { "--table", "mytable" });
    }

    @Test(expected=IllegalStateException.class)
    public void testParseOptions_NoTable() {
        bulkLoadTool.parseOptions(new String[] { "--input", "/input" });
    }

    @Test
    public void testGetQualifiedTableName() {
        assertEquals("MYSCHEMA.MYTABLE", CsvBulkLoadTool.getQualifiedTableName("mySchema", "myTable"));
    }

    @Test
    public void testGetQualifiedTableName_NullSchema() {
        assertEquals("MYTABLE", CsvBulkLoadTool.getQualifiedTableName(null, "myTable"));
    }

    @Test
    public void testGetJdbcUrl_WithQuorumSupplied() {
        assertEquals("jdbc:phoenix:myzkhost:2181", bulkLoadTool.getJdbcUrl("myzkhost:2181"));
    }

    @Test
    public void testGetJdbcUrl_NoQuorumSupplied() {
        assertEquals("jdbc:phoenix:localhost:2181", bulkLoadTool.getJdbcUrl(null));
    }

}
