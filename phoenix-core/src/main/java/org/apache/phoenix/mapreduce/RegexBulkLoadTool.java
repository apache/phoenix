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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.util.ColumnInfo;

/**
 * A tool for running MapReduce-based ingests of input data based on regex.
 * Lists are converted into typed ARRAYS.
 */
public class RegexBulkLoadTool extends AbstractBulkLoadTool {

    static final Option REGEX_OPT = new Option("r", "regex", true, "Input regex String, defaults is (.*)");
    static final Option ARRAY_DELIMITER_OPT = new Option("a", "array-delimiter", true, "Array element delimiter (optional), defaults is ','");

    @Override
    protected Options getOptions() {
        Options options = super.getOptions();
        options.addOption(REGEX_OPT);
        options.addOption(ARRAY_DELIMITER_OPT);
        return options;
    }

    @Override
    protected void configureOptions(CommandLine cmdLine, List<ColumnInfo> importColumns,
                                         Configuration conf) throws SQLException {
    	if (cmdLine.hasOption(REGEX_OPT.getOpt())) {
            String regexString = cmdLine.getOptionValue(REGEX_OPT.getOpt());
            conf.set(RegexToKeyValueMapper.REGEX_CONFKEY, regexString);
        }
    	
    	if (cmdLine.hasOption(ARRAY_DELIMITER_OPT.getOpt())) {
            String arraySeparator = cmdLine.getOptionValue(ARRAY_DELIMITER_OPT.getOpt());
            conf.set(RegexToKeyValueMapper.ARRAY_DELIMITER_CONFKEY, arraySeparator);
        }
    }

    @Override
    protected void setupJob(Job job) {
        // Allow overriding the job jar setting by using a -D system property at startup
        if (job.getJar() == null) {
            job.setJarByClass(RegexToKeyValueMapper.class);
        }
        job.setMapperClass(RegexToKeyValueMapper.class);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new RegexBulkLoadTool(), args);
    }
}
