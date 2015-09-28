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

import org.apache.phoenix.pherf.result.ResultUtil;
import org.junit.BeforeClass;

import java.util.Properties;

public class ResultBaseTest {
    protected static PherfConstants constants;
    private static boolean isSetUpDone = false;

    @BeforeClass
    public static void setUp() throws Exception {
        if (isSetUpDone) {
            return;
        }

        ResultUtil util = new ResultUtil();
        constants = PherfConstants.create();
        Properties properties = constants.getProperties(PherfConstants.PHERF_PROPERTIES, false);
        String dir = properties.getProperty("pherf.default.results.dir");
        String targetDir = "target/" + dir;
        properties.setProperty("pherf.default.results.dir", targetDir);
        util.ensureBaseDirExists(targetDir);
        isSetUpDone = true;
    }
}
