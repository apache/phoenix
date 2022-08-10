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

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Option;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Options;
import org.apache.phoenix.util.ColumnInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parse csv bulk load commands from shell and run bulk load tool.
 */
public class CsvBulkLoadTool extends AbstractBulkLoadTool {
    private static final Logger LOGGER = LoggerFactory.getLogger(CsvBulkLoadTool.class);
    static final Option DELIMITER_OPT = new Option("d", "delimiter", true,
            "Input delimiter, defaults to comma");
    static final Option MULTIPLE_DELIMITER_OPT = new Option("md", "multiple-delimiter", true,
            "Input delimiter, support multiple delimiter, it will cover \"delimiter\" if it exist");
    static final Option QUOTE_OPT = new Option("q", "quote", true,
            "Supply a custom phrase delimiter, defaults to double quote character");
    static final Option ESCAPE_OPT = new Option("e", "escape", true,
            "Supply a custom escape character, default is a backslash");
    static final Option ARRAY_DELIMITER_OPT = new Option("a", "array-delimiter", true,
            "Array element delimiter (optional)");
    static final Option BINARY_ENCODING_OPT = new Option("b", "binaryEncoding", true,
            "Specifies binary encoding");
    private static final int DEFAULT_DELIMITER_MAX_LEN = 32;

    @Override
    protected Options getOptions() {
        Options options = super.getOptions();
        options.addOption(DELIMITER_OPT);
        options.addOption(MULTIPLE_DELIMITER_OPT);
        options.addOption(QUOTE_OPT);
        options.addOption(ESCAPE_OPT);
        options.addOption(ARRAY_DELIMITER_OPT);
        options.addOption(BINARY_ENCODING_OPT);
        return options;
    }

    @Override
    protected void configureOptions(CommandLine cmdLine, List<ColumnInfo> importColumns,
                                    Configuration conf) throws SQLException {

        // we don't parse ZK_QUORUM_OPT here because we need it in order to
        // create the connection we need to build importColumns.
        String delimiterStr = null;
        char delimiterChar = CsvBulkImportUtil.DEFAULT_DELIMITER_CHAR;
        if (cmdLine.hasOption(MULTIPLE_DELIMITER_OPT)) {
            delimiterStr = StringEscapeUtils.unescapeJava(
                    cmdLine.getOptionValue(MULTIPLE_DELIMITER_OPT.getOpt()));
            if (delimiterStr != null && delimiterStr.length() > DEFAULT_DELIMITER_MAX_LEN) {
                LOGGER.warn("Delimiter String length is longer than {},"
                        + "suggest to use a shorter delimiter to improve performance",
                        DEFAULT_DELIMITER_MAX_LEN);
            }
        } else if (cmdLine.hasOption(DELIMITER_OPT.getOpt())) {
            String delimiterCharOptStr =
                    StringEscapeUtils.unescapeJava(
                    cmdLine.getOptionValue(DELIMITER_OPT.getOpt()));
            if (delimiterCharOptStr.length() != 1) {
                throw new IllegalArgumentException("Illegal delimiter character: "
                        + delimiterCharOptStr);
            }
            delimiterChar = delimiterCharOptStr.charAt(0);
        }

        Character quoteChar = CsvBulkImportUtil.DEFAULT_QUOTE_CHAR;
        if (cmdLine.hasOption(QUOTE_OPT.getOpt())) {
            String quoteString = StringEscapeUtils.unescapeJava(cmdLine.getOptionValue(QUOTE_OPT
                    .getOpt()));
            if (quoteString.length() == 0) {
                quoteChar = null;
            } else if (quoteString.length() != 1) {
                throw new IllegalArgumentException("Illegal quote character: " + quoteString);
            } else {
                quoteChar = quoteString.charAt(0);
            }
        }

        Character escapeChar = CsvBulkImportUtil.DEFAULT_ESCAPE_CHAR;
        if (cmdLine.hasOption(ESCAPE_OPT.getOpt())) {
            String escapeString = cmdLine.getOptionValue(ESCAPE_OPT.getOpt());
            if (escapeString.length() == 0) {
                escapeChar = null;
            } else if (escapeString.length() != 1) {
                throw new IllegalArgumentException("Illegal escape character: " + escapeString);
            } else {
                escapeChar = escapeString.charAt(0);
            }
        }

        String binaryEncoding = null;
        if (cmdLine.hasOption(BINARY_ENCODING_OPT.getOpt())) {
            binaryEncoding = cmdLine.getOptionValue(BINARY_ENCODING_OPT.getOpt());
        }
        if (delimiterStr != null) {
            CsvBulkImportUtil.initCsvImportJob(conf, delimiterStr, quoteChar, escapeChar,
                    cmdLine.getOptionValue(ARRAY_DELIMITER_OPT.getOpt()), binaryEncoding);
        } else {
            CsvBulkImportUtil.initCsvImportJob(conf, delimiterChar, quoteChar, escapeChar,
                    cmdLine.getOptionValue(ARRAY_DELIMITER_OPT.getOpt()), binaryEncoding);
        }
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
