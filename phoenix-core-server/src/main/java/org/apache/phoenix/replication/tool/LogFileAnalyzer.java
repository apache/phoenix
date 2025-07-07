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
package org.apache.phoenix.replication.tool;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.replication.log.LogFile;
import org.apache.phoenix.replication.log.LogFile.Record;
import org.apache.phoenix.replication.log.LogFileReader;
import org.apache.phoenix.replication.log.LogFileReaderContext;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command-line tool for analyzing Phoenix Replication Log files.
 * This tool can:
 * - Read a single log file or directory of log files
 * - Print file headers, trailers, and block headers
 * - Decode and display log record contents
 * - Verify checksums and report corruption
 */
public class LogFileAnalyzer extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(LogFileAnalyzer.class);

    private static final String USAGE = "Usage: LogFileAnalyzer [options] <path>\n"
        + "Options:\n"
        + "  -h, --help        Show this help message\n"
        + "  -v, --verbose     Show detailed information\n"
        + "  -c, --check       Verify checksums and report corruption\n"
        + "  -d, --decode      Decode and display record contents\n";

    private boolean verbose = false;
    private boolean decode = false;
    private boolean check = false;
    FileSystem fs;

    private void init() throws IOException {
        Configuration conf = getConf();
        if (conf == null) {
            conf = HBaseConfiguration.create();
            setConf(conf);
        }
        if (fs == null) {
            fs = FileSystem.get(getConf());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (!parseArgs(args)) {
            System.err.println(USAGE);
            return 1;
        }
        try {
            init();
            Path path = new Path(args[args.length - 1]);
            List<Path> filesToAnalyze = getFilesToAnalyze(path);
            if (filesToAnalyze.isEmpty()) {
                System.err.println("No log files found in: " + path);
                return 1;
            }
            // Analyze each file
            for (Path file : filesToAnalyze) {
                analyzeFile(file);
            }
            return 0;
        } catch (Exception e) {
            LOG.error("Error analyzing log files", e);
            return 1;
        }
    }

    /**
     * Returns all the mutations grouped by the table name under a source path
     * @param source Path which can be a file or directory
     * @return Mutations grouped by the table name
     * @throws IOException
     */
    public Map<String, List<Mutation>> groupLogsByTable(String source) throws IOException {
        Map<String, List<Mutation>> allFiles = Maps.newHashMap();
        init();
        Path path = new Path(source);
        List<Path> filesToAnalyze = getFilesToAnalyze(path);
        if (filesToAnalyze.isEmpty()) {
            return allFiles;
        }
        // Analyze each file
        for (Path file : filesToAnalyze) {
            Map<String, List<Mutation>> perFile = groupLogsByTable(file);
            for (Map.Entry<String, List<Mutation>> entry : perFile.entrySet()) {
                List<Mutation> mutations = allFiles.get(entry.getKey());
                if (mutations == null) {
                    allFiles.put(entry.getKey(), entry.getValue());
                } else {
                    mutations.addAll(entry.getValue());
                }
            }
        }
        return allFiles;
    }

    private List<Path> getFilesToAnalyze(Path path) throws IOException {
        if (!fs.exists(path)) {
            throw new PathNotFoundException(path.toString());
        }
        List<Path> filesToAnalyze = Lists.newArrayList();
        if (fs.getFileStatus(path).isDirectory()) {
            // Recursively find all .plog files
            findLogFiles(path, filesToAnalyze);
        } else {
            filesToAnalyze.add(path);
        }
        return filesToAnalyze;
    }

    private void findLogFiles(Path dir, List<Path> files) throws IOException {
        FileStatus[] statuses = fs.listStatus(dir);
        for (FileStatus status : statuses) {
            Path path = status.getPath();
            if (status.isDirectory()) {
                findLogFiles(path, files);
            } else if (path.getName().endsWith(".plog")) {
                files.add(path);
            }
        }
    }

    private void analyzeFile(Path file) throws IOException {
        System.out.println("\nAnalyzing file: " + file);

        LogFileReaderContext context = new LogFileReaderContext(getConf())
            .setFileSystem(fs)
            .setFilePath(file)
            .setSkipCorruptBlocks(check); // Skip corrupt blocks if checking

        LogFileReader reader = new LogFileReader();
        try {
            reader.init(context);

            // Print header information
            System.out.println("Header:");
            System.out.println("  Version: " + reader.getHeader().getMajorVersion() + "."
                + reader.getHeader().getMinorVersion());

            // Process records
            int recordCount = 0;
            Record record;
            while ((record = reader.next()) != null) {
                recordCount++;
                if (decode) {
                    System.out.println("\nRecord #" + recordCount + ":");
                    System.out.println("  Table: " + record.getHBaseTableName());
                    System.out.println("  Commit ID: " + record.getCommitId());
                    System.out.println("  Mutation: " + record.getMutation());
                    if (verbose) {
                        System.out.println("  Serialized Length: " + record.getSerializedLength());
                    }
                }
            }

            // Print trailer information
            LogFile.Trailer trailer = reader.getTrailer();
            if (trailer != null) {
                System.out.println("\nTrailer:");
                System.out.println("  Record Count: " + reader.getTrailer().getRecordCount());
                System.out.println("  Block Count: " + reader.getTrailer().getBlockCount());
                System.out.println("  Blocks Start Offset: "
                        + reader.getTrailer().getBlocksStartOffset());
                System.out.println("  Trailer Start Offset: "
                        + reader.getTrailer().getTrailerStartOffset());
            } else {
                System.out.println("\nTrailer is null");
            }

            // Print verification results if checking
            if (check) {
                System.out.println("\nVerification Results:");
                System.out.println("  Records Read: " + context.getRecordsRead());
                System.out.println("  Blocks Read: " + context.getBlocksRead());
                System.out.println("  Corrupt Blocks Skipped: "
                    + context.getCorruptBlocksSkipped());

                if (context.getCorruptBlocksSkipped() > 0) {
                    System.out.println("  WARNING: File contains corrupt blocks!");
                } else if (context.getRecordsRead() == reader.getTrailer().getRecordCount()) {
                    System.out.println("  File integrity verified successfully");
                } else {
                    System.out.println("  WARNING: Record count mismatch!");
                }
            }
        } finally {
            reader.close();
        }
    }

    private Map<String, List<Mutation>> groupLogsByTable(Path file) throws IOException {
        Map<String, List<Mutation>> mutationsByTable = Maps.newHashMap();
        System.out.println("\nAnalyzing file: " + file);
        LogFileReaderContext context = new LogFileReaderContext(getConf())
                .setFileSystem(fs)
                .setFilePath(file)
                .setSkipCorruptBlocks(check); // Skip corrupt blocks if checking
        LogFileReader reader = new LogFileReader();
        try {
            reader.init(context);
            // Process records
            Record record;
            while ((record = reader.next()) != null) {
                String tableName = record.getHBaseTableName();
                List<Mutation> mutations = mutationsByTable.getOrDefault(tableName,
                        Lists.newArrayList());
                mutations.add(record.getMutation());
                mutationsByTable.put(tableName, mutations);
            }
        } finally {
            reader.close();
        }
        return mutationsByTable;
    }

    private boolean parseArgs(String[] args) {
        if (args.length == 0) {
            return false;
        }
        for (int i = 0; i < args.length - 1; i++) {
            String arg = args[i];
            switch (arg) {
            case "-h":
            case "--help":
                return false;
            case "-v":
            case "--verbose":
                verbose = true;
                break;
            case "-c":
            case "--check":
                check = true;
                break;
            case "-d":
            case "--decode":
                decode = true;
                break;
            default:
                if (arg.startsWith("-")) {
                    System.err.println("Unknown option: " + arg);
                    return false;
                }
            }
        }
        return true;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new LogFileAnalyzer(), args);
        System.exit(res);
    }

}
