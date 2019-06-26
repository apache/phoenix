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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.SchemaUtil;

public class CsvBulkLoadTool extends AbstractBulkLoadTool {

    static final Option DELIMITER_OPT = new Option("d", "delimiter", true, "Input delimiter, defaults to comma");
    static final Option QUOTE_OPT = new Option("q", "quote", true, "Supply a custom phrase delimiter, defaults to double quote character");
    static final Option ESCAPE_OPT = new Option("e", "escape", true, "Supply a custom escape character, default is a backslash");
    static final Option ARRAY_DELIMITER_OPT = new Option("a", "array-delimiter", true, "Array element delimiter (optional)");
    static final Option binaryEncodingOption = new Option("b", "binaryEncoding", true, "Specifies binary encoding");
    static final Option SKIP_HEADER_OPT = new Option("k", "skip-header", false, "Skip the first line of CSV files (the header)");
    static final Option HEADER_OPT = new Option("p", "parse-header", false, "Parses the first line of CSV as the header");

    @Override
    protected Options getOptions() {
        Options options = super.getOptions();
        options.addOption(DELIMITER_OPT);
        options.addOption(QUOTE_OPT);
        options.addOption(ESCAPE_OPT);
        options.addOption(ARRAY_DELIMITER_OPT);
        options.addOption(binaryEncodingOption);
        options.addOption(SKIP_HEADER_OPT);
        options.addOption(HEADER_OPT);
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

        // Skip the first line of the CSV file(s)?
        if (cmdLine.hasOption(SKIP_HEADER_OPT.getOpt()) || cmdLine.hasOption(HEADER_OPT.getOpt())) {
            PhoenixTextInputFormat.setSkipHeader(conf);
        }
        
        CsvBulkImportUtil.initCsvImportJob(
                conf,
                delimiterChar,
                quoteChar,
                escapeChar,
                cmdLine.getOptionValue(ARRAY_DELIMITER_OPT.getOpt()),
                binaryEncoding);
    }

    /**
     * Build up the list of columns to be imported. The list is taken from the command line if
     * present, otherwise it is taken from the table description.
     *
     * @param conn connection to Phoenix
     * @param cmdLine supplied command line options
     * @param qualifiedTableName table name (possibly with schema) of the table to be imported
     * @param conf Configured options
     * @return the list of columns to be imported
     */
    @Override
    List<ColumnInfo> buildImportColumns(
            Connection conn, CommandLine cmdLine, String qualifiedTableName, Configuration conf
    ) throws SQLException, IOException {
        List<ColumnInfo> columnInfos;
        if (cmdLine.hasOption(HEADER_OPT.getOpt())) {
            List<String> parsedColumns = parseCsvHeaders(cmdLine, conf);
            columnInfos = SchemaUtil.generateColumnInfo(
                    conn, qualifiedTableName, parsedColumns, true);
        } else {
            columnInfos = super.buildImportColumns(conn, cmdLine, qualifiedTableName, conf);
        }
        return columnInfos;
    }

    /**
     * Parse the header (first line) from the input CSV and return the ArrayList of input columns
     * @param cmdLine Supplied commandline options
     * @param conf Configured options
     * @return the list of columns to be imported parsed from input CSV header
     * @throws IOException Exception thrown by FileSystem IO.
     */
    private List<String> parseCsvHeaders(CommandLine cmdLine, Configuration conf) throws IOException {
        List<String> headerColumns;
        String inputPaths = cmdLine.getOptionValue(INPUT_PATH_OPT.getOpt());
        Iterable<String> paths = Splitter.on(",").trimResults().split(inputPaths);
        List<String> headers = fetchAllHeaders(paths, conf);
        List<String> uniqueHeaders = headers.stream().distinct().collect(Collectors.toList());
        if (uniqueHeaders.size() > 1) {
            throw new IllegalArgumentException(
                "Headers in provided input files are different. Headers must be unique for all input files"
            );
        }
        String header = uniqueHeaders.get(0);
        headerColumns = Lists.newArrayList(Splitter.on(",").trimResults().split(header));
        return headerColumns;
    }

    /**
     * Fetch the headers from all comma separated input files provided by user.
     * @param paths Iterable instance of the provided input paths
     * @param conf Configured options
     * @return The list of headers from all input files.
     * @throws IOException Exception thrown by FileSystem IO
     */
    private List<String> fetchAllHeaders(Iterable<String> paths, Configuration conf) throws IOException {
        List<String> headers = new ArrayList<>();
        for (String path : paths) {
            headers.add(fetchCsvHeader(conf, path));
        }
        return headers;
    }

    /**
     * Fetch CSV header (first line) from given HDFS path
     * @param conf Configured Options
     * @param path HDFS Path to single input file
     * @return The header line (first line) from the input file
     * @throws IOException Exception thrown by FileSystem IO
     */
    private String fetchCsvHeader(Configuration conf, String path) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(path), conf);
        try(FSDataInputStream inputStream = fs.open(new Path(path))) {
            try(BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                return reader.readLine();
            }
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

    /**
     * Parses the commandline arguments, throws IllegalStateException if mandatory arguments are
     * missing and throws IllegalArgumentException if --parse-header and --skip-header are used
     * together.
     *
     * @param args supplied command line arguments
     * @return the parsed command line
     */
    @Override
    protected CommandLine parseOptions(String[] args) {
        CommandLine cmdLine = super.parseOptions(args);

        if (cmdLine.hasOption(HEADER_OPT.getOpt()) && cmdLine.hasOption(SKIP_HEADER_OPT.getOpt())) {
            throw new IllegalArgumentException(HEADER_OPT.getLongOpt() + " and " +
                    SKIP_HEADER_OPT.getLongOpt() + " cannot be used together.");
        }

        return cmdLine;
    }

    public static void main(String[] args) throws Exception {
        int exitStatus = ToolRunner.run(new CsvBulkLoadTool(), args);
        System.exit(exitStatus);
    }
}
