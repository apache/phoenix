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
package org.apache.phoenix.tools.util;

import java.io.PrintWriter;
import java.io.StringWriter;

public class PhckSupportedFeatureDocs {
    public static final String SYSTEM_LEVEL_TABLE = "systemLevelTable";
    public static final String USER_LEVEL_TABLE = "userLevelTable";
    public static final String FIX_ALL_CORRUPTED_METADATA = "allTables";
    public static final String HELP_USAGE_STRING = "FOR USAGE, use the -h or --help option";
    public static final String MISS_FIX_OR_MONITOR_OPTION_MSG = " takes either fix or monitor mode option.";
    public static String getCommandUsage() {
        StringWriter sw = new StringWriter();
        PrintWriter writer = new PrintWriter(sw);
        writer.println("Command:");
        usageCorruptedMetadata(writer);
        writer.println();
        usageSystemLevelMetadata(writer);
        writer.println();
        usageSystemLevel(writer);
        writer.println();

        writer.close();
        return sw.toString();
    }

    private static void usageSystemLevel(PrintWriter writer) {
        writer.println(" " + SYSTEM_LEVEL_TABLE + " [OPTIONS] [<TABLENAME>...]");
        writer.println("   Options:");
        writer.println("   '-m --monitor' option. Looks for system level table data corruption");
        writer.println("   and generates a detail report. Without table name will run all system level tables");
        writer.println("    -f, --fixMissingTable  Fix a missing system level table by creating a new table.");
        writer.println("    -e, --enableTable  Fix a disabled system level table.");
        writer.println("    -c, --fixRowCount  Fix head row column count is not matching number of column rows");
    }

    private static void usageSystemLevelMetadata(PrintWriter writer) {
        writer.println(" " + USER_LEVEL_TABLE + " [OPTIONS] [<TABLENAME>...]");
        writer.println("   Options:");
        writer.println("    -f, --fix    fix any user level table issues found.");
        writer.println("   '-m --monitor' option. Looks for user level table data corruption");
        writer.println("   and generates a detail report.");
        writer.println("    -t, --table  Pass a table name to check/fix any corruption.");
        writer.println("    -c, --rowCount  Fix the head row column count is not matching number");
        writer.println("    of column rows from the syscat table.");
        writer.println("    -e, --enable  Fix a disabled user level table.");
        writer.println("    -o, --orphanView  Removing orphan view.");
        writer.println("    -r, --orphanRow  Removing orphan row.");
    }

    private static void usageCorruptedMetadata(PrintWriter writer) {
        writer.println(" " + FIX_ALL_CORRUPTED_METADATA + " [OPTIONS] [<TABLENAME>...]");
        writer.println("    -f, --fix    fix any issues found.");
        writer.println("   '-m --monitor' option. Looks for all tables data corruption");
    }
}
