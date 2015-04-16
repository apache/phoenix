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

import java.sql.Date;

public class PherfTest extends BaseTestWithCluster {
    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @Test
    public void testPherfMain() {
        String[] args = {"-drop", "all", "-l", "-q", "-m",
                "--monitorFrequency", "100",
                "-z", "localhost",
                "--scenarioFile", ".*user_defined_scenario.xml",
                "--schemaFile", ".*user_defined_schema_194.sql"};
        Pherf.main(args);
    }

    @Test
    public void testListArgument() {
        String[] args = {"-listFiles"};
        Pherf.main(args);
    }

    @Test
    public void testReleaseExists() {
        String[] args = {"-drop", "all", "-l", "-q", "-m",
                "--monitorFrequency", "100",
                "--scenarioFile", ".*test_scenario.xml",
                "--schemaFile", ".*user_defined_schema_194.sql"};

        // Makes sure that System.exit(1) is called. Release is a required param.
        exit.expectSystemExitWithStatus(1);
        Pherf.main(args);
    }

    @Test
    public void testUnknownOption() {
        String[] args = {"-drop", "all", "-l", "-q", "-m","-bsOption"};

        // Makes sure that System.exit(1) is called. Release is a required param.
        exit.expectSystemExitWithStatus(1);
        Pherf.main(args);
    }
}
