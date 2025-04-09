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
 * Context for {@link ReplicationLog.Writer}. Uses Builder pattern.
 */
public class LogWriterContext {

    private static final Logger LOG = LoggerFactory.getLogger(LogWriterContext.class);

    /** Configuration key for the block size */
    public static final String LOG_BLOCK_SIZE = "phoenix.replication.log.block.size";
    /** Default block size for replication logs (e.g., 1MB) */
    public static final long DEFAULT_LOG_BLOCK_SIZE = 1L * 1024 * 1024; // 1 MB

    /** Configuration key for compression type */
    public static final String LOG_COMPRESSION = "phoenix.replication.log.compression";
    /** Default block size for replication logs (e.g., 1MB) */
    public static final String DEFAULT_LOG_COMPRESSION = Compression.Algorithm.NONE.name();

    private final Configuration conf;
    private FileSystem fs;
    private Path path;
    private Compression.Algorithm compression;
    private LogCodec codec;
    private long maxBlockSize;

    public LogWriterContext(Configuration conf) {
        this.conf = conf;
        try {
            this.compression = Compression.getCompressionAlgorithmByName(conf.get(LOG_COMPRESSION,
                DEFAULT_LOG_COMPRESSION));
        } catch (IllegalArgumentException e) {
            // "NONE" is not actually a valid compression algorithm name
            this.compression = Compression.Algorithm.NONE;
        }
        this.maxBlockSize = conf.getLong(LOG_BLOCK_SIZE, DEFAULT_LOG_BLOCK_SIZE);
        if (this.maxBlockSize <= 0) {
            LOG.warn("Invalid {} configured: {}. Using default: {}", LOG_BLOCK_SIZE,
                this.maxBlockSize, DEFAULT_LOG_BLOCK_SIZE);
            this.maxBlockSize = DEFAULT_LOG_BLOCK_SIZE;
        }
        // Note: When we have multiple codec types, instantiate the appropriate type based on
        // configuration;
        this.codec = new LogCodec();
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

    public LogWriterContext setFilePath(Path path) {
        this.path = path;
        return this;
    }

    public LogWriterContext setFileSystem(FileSystem fs) {
        this.fs = fs;
        return this;
    }

    public Compression.Algorithm getCompression() {
        return compression;
    }

    public LogWriterContext setCompression(Compression.Algorithm compression) {
        this.compression = compression;
        return this;
    }

    public LogCodec getCodec() {
        return codec;
    }

    public LogWriterContext setCodec(LogCodec codec) {
        this.codec = codec;
        return this;
    }

    public long getMaxBlockSize() {
        return maxBlockSize;
    }

    public LogWriterContext setMaxBlockSize(long maxBlockSize) {
        this.maxBlockSize = maxBlockSize;
        return this;
    }

    @Override
    public String toString() {
        return "LogWriterContext [path=" + path + ", compression=" + compression + ", codec="
            + codec + ", maxBlockSize=" + maxBlockSize + "]";
    }

}
