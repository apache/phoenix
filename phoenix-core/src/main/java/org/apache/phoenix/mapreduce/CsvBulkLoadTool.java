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

import java.sql.SQLException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.util.ColumnInfo;

public class CsvBulkLoadTool extends AbstractBulkLoadTool {

    static final Option DELIMITER_OPT = new Option("d", "delimiter", true, "Input delimiter, defaults to comma");
    static final Option QUOTE_OPT = new Option("q", "quote", true, "Supply a custom phrase delimiter, defaults to double quote character");
    static final Option ESCAPE_OPT = new Option("e", "escape", true, "Supply a custom escape character, default is a backslash");
    static final Option ARRAY_DELIMITER_OPT = new Option("a", "array-delimiter", true, "Array element delimiter (optional)");
    static final Option binaryEncodingOption = new Option("b", "binaryEncoding", true, "Specifies binary encoding");

    @Override
    protected Options getOptions() {
        Options options = super.getOptions();
        options.addOption(DELIMITER_OPT);
        options.addOption(QUOTE_OPT);
        options.addOption(ESCAPE_OPT);
        options.addOption(ARRAY_DELIMITER_OPT);
        options.addOption(binaryEncodingOption);
        return options;
    }

    @Override
    protected void configureOptions(CommandLine cmdLine, List<ColumnInfo> importColumns,
                                         Configuration conf) throws SQLException {

        // we don't parse ZK_QUORUM_OPT here because we need it in order to
        // create the connection we need to build importColumns.

        char delimiterChar = ',';
        if (cmdLine.hasOption(DELIMITER_OPT.getOpt())) {
            String delimString = StringEscapeUtils.unescapeJava(cmdLine.getOptionValue(DELIMITER_OPT.getOpt()));
            if (delimString.length() != 1) {
                throw new IllegalArgumentException("Illegal delimiter character: " + delimString);
            }
            delimiterChar = delimString.charAt(0);
        }

        char quoteChar = '"';
        if (cmdLine.hasOption(QUOTE_OPT.getOpt())) {
            String quoteString = cmdLine.getOptionValue(QUOTE_OPT.getOpt());
            if (quoteString.length() != 1) {
                throw new IllegalArgumentException("Illegal quote character: " + quoteString);
            }
            quoteChar = quoteString.charAt(0);
        }

        char escapeChar = '\\';
        if (cmdLine.hasOption(ESCAPE_OPT.getOpt())) {
            String escapeString = cmdLine.getOptionValue(ESCAPE_OPT.getOpt());
            if (escapeString.length() != 1) {
                throw new IllegalArgumentException("Illegal escape character: " + escapeString);
            }
            escapeChar = escapeString.charAt(0);
        }
        
        String binaryEncoding = null;
        if (cmdLine.hasOption(binaryEncodingOption.getOpt())) {
            binaryEncoding = cmdLine.getOptionValue(binaryEncodingOption.getOpt());
        }
        
        CsvBulkImportUtil.initCsvImportJob(
                conf,
                delimiterChar,
                quoteChar,
                escapeChar,
                cmdLine.getOptionValue(ARRAY_DELIMITER_OPT.getOpt()),
                binaryEncoding);
    }

    @Override
    protected void setupJob(Job job) {
        // Allow overriding the job jar setting by using a -D system property at startup
        if (job.getJar() == null) {
            job.setJarByClass(CsvToKeyValueMapper.class);
        }
        job.setMapperClass(CsvToKeyValueMapper.class);
    }

    public static void main(String[] args) throws Exception {
        int exitStatus = ToolRunner.run(new CsvBulkLoadTool(), args);
        System.exit(exitStatus);
    }
}
