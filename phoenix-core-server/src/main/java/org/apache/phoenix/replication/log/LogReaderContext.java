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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Context for {@link ReplicationLog.Reader}. Uses Builder pattern.
 */
public class LogReaderContext {

    /** Configuration key for compression type */
    public static final String LOG_SKIP_CORRUPT_BLOCKS = "phoenix.replication.log.skip.corrupt.blocks";
    /** Default for skipping corrupt blocks */
    public static final boolean DEFAULT_LOG_SKIP_CORRUPT_BLOCKS = true;

    private final Configuration conf;
    private FileSystem fs;
    private Path path;
    private LogCodec codec;
    private long fileSize = -1;
    private boolean isSkipCorruptBlocks;
    private AtomicLong blocksRead = new AtomicLong();
    private AtomicLong recordsRead = new AtomicLong();
    private AtomicLong corruptBlocksSkipped = new AtomicLong();

    public LogReaderContext(Configuration conf) {
        this.conf = conf;
        this.isSkipCorruptBlocks = conf.getBoolean(LOG_SKIP_CORRUPT_BLOCKS,
            DEFAULT_LOG_SKIP_CORRUPT_BLOCKS);
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

    public LogReaderContext setFileSystem(FileSystem fileSystem) {
        this.fs = fileSystem;
        return this;
    }

    public Path getFilePath() {
        return path;
    }

    public LogReaderContext setFilePath(Path filePath) {
        this.path = filePath;
        return this;
    }

    public long getFileSize() throws IOException {
        if (fileSize < 0) {
            fileSize = fs.getFileStatus(path).getLen();
        }
        return fileSize;
    }

    public LogReaderContext setFileSize(long fileSize) {
        this.fileSize = fileSize;
        return this;
    }

    public boolean isSkipCorruptBlocks() {
        return isSkipCorruptBlocks;
    }

    public LogReaderContext setSkipCorruptBlocks(boolean isSkipCorruptBlocks) {
        this.isSkipCorruptBlocks = isSkipCorruptBlocks;
        return this;
    }

    public LogCodec getCodec() {
        return codec;
    }

    public LogReaderContext setCodec(LogCodec codec) {
        this.codec = codec;
        return this;
    }

    public void incrementBlocksRead() {
        blocksRead.incrementAndGet();
    }

    public long getBlocksRead() {
        return blocksRead.get();
    }

    public LogReaderContext setBlocksRead(long value) {
        blocksRead.set(value);
        return this;
    }

    public void incrementRecordsRead() {
        recordsRead.incrementAndGet();
    }

    public long getRecordsRead() {
        return recordsRead.get();
    }

    public LogReaderContext setRecordsRead(long value) {
        recordsRead.set(value);
        return this;
    }

    public void incrementCorruptBlocksSkipped() {
        corruptBlocksSkipped.incrementAndGet();
    }

    public long getCorruptBlocksSkipped() {
        return corruptBlocksSkipped.get();
    }

    public LogReaderContext setCorruptBlocksSkipped(long value) {
        corruptBlocksSkipped.set(value);
        return this;
    }

    @Override
    public String toString() {
        return "LogReaderContext [filePath=" + path + ", fileSize=" + fileSize
            + ", isSkipCorruptBlocks=" + isSkipCorruptBlocks + ", codec=" + codec + ", blocksRead="
            + blocksRead + ", recordsRead=" + recordsRead + ", corruptBlocksSkipped="
            + corruptBlocksSkipped + "]";
    }

}
