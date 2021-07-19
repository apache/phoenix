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
package org.apache.phoenix.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import static org.apache.phoenix.tools.util.PhckSupportedFeatureDocs.HELP_USAGE_STRING;
import static org.apache.phoenix.tools.util.PhckSupportedFeatureDocs.MISS_FIX_OR_MONITOR_OPTION_MSG;
import static org.apache.phoenix.tools.util.PhckSupportedFeatureDocs.SYSTEM_LEVEL_TABLE;
import static org.junit.Assert.assertTrue;

public class PhckTest {
    @Test
    public void testHelp() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        String output = retrieveOptionOutput(null, conf);
        assertTrue(output, output.startsWith(HELP_USAGE_STRING));

        String[] option = new String[] {"-h"};
        output = retrieveOptionOutput(option, conf);
        assertTrue(output, output.startsWith(HELP_USAGE_STRING));
    }

    @Test
    public void testCommandWithSysLevelOptions() throws IOException {
        Configuration conf = HBaseConfiguration.create();

        String output = retrieveOptionOutput(new String[] {SYSTEM_LEVEL_TABLE}, conf);
        assertTrue(output, output.startsWith(HELP_USAGE_STRING));
        assertTrue(output, output.contains(MISS_FIX_OR_MONITOR_OPTION_MSG));
    }

    private String retrieveOptionOutput(String[] option, Configuration conf) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(os);
        PrintStream oldOut = System.out;
        System.setOut(stream);
        Phck phck = new Phck(conf);

        if (option != null) {
            phck.run(option);
        } else {
            phck.run(null);
        }
        stream.close();
        os.close();
        System.setOut(oldOut);
        return os.toString();
    }
}
