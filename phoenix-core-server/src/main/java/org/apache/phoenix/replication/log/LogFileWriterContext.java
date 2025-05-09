/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.replication.log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Context for LogFileWriter. Uses Builder pattern.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = { "EI_EXPOSE_REP", "EI_EXPOSE_REP2" },
    justification = "Intentional")
public class LogFileWriterContext {

    private static final Logger LOG = LoggerFactory.getLogger(LogFileWriterContext.class);

    /** Configuration key for the block size */
    public static final String LOGFILE_BLOCK_SIZE = "phoenix.replication.logfile.block.size";
    /** Default block size for replication logs (e.g., 1MB) */
    public static final long DEFAULT_LOGFILE_BLOCK_SIZE = 1L * 1024 * 1024; // 1 MB

    /** Configuration key for compression type */
    public static final String LOGFILE_COMPRESSION = "phoenix.replication.logfile.compression";
    /** Default block size for replication logs (e.g., 1MB) */
    public static final String DEFAULT_LOGFILE_COMPRESSION = Compression.Algorithm.NONE.name();

    private final Configuration conf;
    private FileSystem fs;
    private Path path;
    private Compression.Algorithm compression;
    private LogFileCodec codec;
    private long maxBlockSize;

    public LogFileWriterContext(Configuration conf) {
        this.conf = conf;
        try {
            this.compression =
                Compression.getCompressionAlgorithmByName(conf.get(LOGFILE_COMPRESSION,
                    DEFAULT_LOGFILE_COMPRESSION));
        } catch (IllegalArgumentException e) {
            // "NONE" is not actually a valid compression algorithm name
            this.compression = Compression.Algorithm.NONE;
        }
        this.maxBlockSize = conf.getLong(LOGFILE_BLOCK_SIZE, DEFAULT_LOGFILE_BLOCK_SIZE);
        if (this.maxBlockSize <= 0) {
            LOG.warn("Invalid {} configured: {}. Using default: {}", LOGFILE_BLOCK_SIZE,
                this.maxBlockSize, DEFAULT_LOGFILE_BLOCK_SIZE);
            this.maxBlockSize = DEFAULT_LOGFILE_BLOCK_SIZE;
        }
        // Note: When we have multiple codec types, instantiate the appropriate type based on
        // configuration;
        this.codec = new LogFileCodec();
    }

    public Configuration getConfiguration() {
        return conf;
    }

    public FileSystem getFileSystem() {
        return fs;
    }

    public Path getFilePath() {
        return path;
    }

    public LogFileWriterContext setFilePath(Path path) {
        this.path = path;
        return this;
    }

    public LogFileWriterContext setFileSystem(FileSystem fs) {
        this.fs = fs;
        return this;
    }

    public Compression.Algorithm getCompression() {
        return compression;
    }

    public LogFileWriterContext setCompression(Compression.Algorithm compression) {
        this.compression = compression;
        return this;
    }

    public LogFileCodec getCodec() {
        return codec;
    }

    public LogFileWriterContext setCodec(LogFileCodec codec) {
        this.codec = codec;
        return this;
    }

    public long getMaxBlockSize() {
        return maxBlockSize;
    }

    public LogFileWriterContext setMaxBlockSize(long maxBlockSize) {
        this.maxBlockSize = maxBlockSize;
        return this;
    }

    @Override
    public String toString() {
        return "LogFileWriterContext [path=" + path + ", compression=" + compression + ", codec="
            + codec + ", maxBlockSize=" + maxBlockSize + "]";
    }

}
