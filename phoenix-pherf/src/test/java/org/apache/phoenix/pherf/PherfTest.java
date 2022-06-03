/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.phoenix.pherf;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class PherfTest {
    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @Test
    public void testListArgument() {
        String[] args = {"-listFiles"};
        Pherf.main(args);
    }

    @Test
    public void testUnknownOption() {
        String[] args = {"-drop", "all", "-q", "-m","-unknownOption"};

        // Makes sure that System.exit(1) is called.
        exit.expectSystemExitWithStatus(1);
        Pherf.main(args);
    }

    @Test
    public void testLongOptions() throws Exception{
        String extension = ".sql";
        String args = "testArgs";
        Long numericArg = 15l;

        String[] longOptionArgs = {"--schemaFile",PherfConstants.SCHEMA_ROOT_PATTERN + extension,"--disableSchemaApply","--disableRuntimeResult","--listFiles","--scenarioFile",args,"--scenarioName",args,"--useAverageCompareType"};
        //Asset that No Exception is thrown, ParseException is thrown in case of invalid option
        assertNotNull(new Pherf(longOptionArgs));

        String[] otherLongOptionArgs = {"--drop",args,"--monitorFrequency",args,"--rowCountOverride",numericArg.toString(),"--hint",args,"--log_per_nrows",numericArg.toString(),"--diff","--export","--writerThreadSize",args,"--stats","--label",args,"--compare",args};
        //Asset that No Exception is thrown, ParseException is thrown in case of invalid option
        assertNotNull(new Pherf(otherLongOptionArgs));
    }

    @Test
    public void testDefaultLogPerNRowsArgument() throws Exception {
        String[] args = {"-listFiles"};
        assertEquals(Long.valueOf(PherfConstants.LOG_PER_NROWS),
                getLogPerNRowsValue(new Pherf(args).getProperties()));
    }

    @Test
    public void testCustomizedLogPerNRowsArgument() throws Exception {
        Long customizedPerNRows = 15l;
        String[] args = {"-listFiles", "-log_per_nrows", customizedPerNRows.toString()};
        assertEquals(customizedPerNRows,
                getLogPerNRowsValue(new Pherf(args).getProperties()));
    }

    @Test
    public void testInvalidLogPerNRowsArgument() throws Exception {
        Long zero = 0l;
        Long negativeOne = -1l;
        String invaildNum = "abc";

        String[] args = {"-listFiles", "-log_per_nrows", zero.toString()};
        assertEquals(Long.valueOf(PherfConstants.LOG_PER_NROWS),
                getLogPerNRowsValue(new Pherf(args).getProperties()));

        String[] args2 = {"-listFiles", "-log_per_nrows", negativeOne.toString()};
        assertEquals(Long.valueOf(PherfConstants.LOG_PER_NROWS),
                getLogPerNRowsValue(new Pherf(args2).getProperties()));

        String[] args3 = {"-listFiles", "-log_per_nrows", invaildNum};
        assertEquals(Long.valueOf(PherfConstants.LOG_PER_NROWS),
                getLogPerNRowsValue(new Pherf(args3).getProperties()));
    }

    private Long getLogPerNRowsValue(Properties prop) {
        return Long.valueOf(prop.getProperty(PherfConstants.LOG_PER_NROWS_NAME));
    }
}
