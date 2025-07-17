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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Context for LogFileReader. Uses Builder pattern.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = { "EI_EXPOSE_REP", "EI_EXPOSE_REP2" },
    justification = "Intentional")
public class LogFileReaderContext {

    /** Configuration key for skipping corrupt blocks */
    public static final String LOGFILE_SKIP_CORRUPT_BLOCKS =
        "phoenix.replication.logfile.skip.corrupt.blocks";
    /** Default for skipping corrupt blocks */
    public static final boolean DEFAULT_LOGFILE_SKIP_CORRUPT_BLOCKS = true;

    private final Configuration conf;
    private FileSystem fs;
    private Path path;
    private LogFileCodec codec;
    private long fileSize = -1;
    private boolean isSkipCorruptBlocks;
    private long blocksRead;
    private long recordsRead;
    private long corruptBlocksSkipped;
    private boolean validateTrailer = true;

    public LogFileReaderContext(Configuration conf) {
        this.conf = conf;
        this.isSkipCorruptBlocks = conf.getBoolean(LOGFILE_SKIP_CORRUPT_BLOCKS,
            DEFAULT_LOGFILE_SKIP_CORRUPT_BLOCKS);
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

    public LogFileReaderContext setFileSystem(FileSystem fileSystem) {
        this.fs = fileSystem;
        return this;
    }

    public Path getFilePath() {
        return path;
    }

    public LogFileReaderContext setFilePath(Path filePath) {
        this.path = filePath;
        return this;
    }

    public long getFileSize() throws IOException {
        if (fileSize < 0) {
            fileSize = fs.getFileStatus(path).getLen();
        }
        return fileSize;
    }

    public LogFileReaderContext setFileSize(long fileSize) {
        this.fileSize = fileSize;
        return this;
    }

    public boolean isSkipCorruptBlocks() {
        return isSkipCorruptBlocks;
    }

    public LogFileReaderContext setSkipCorruptBlocks(boolean isSkipCorruptBlocks) {
        this.isSkipCorruptBlocks = isSkipCorruptBlocks;
        return this;
    }

    public LogFileCodec getCodec() {
        return codec;
    }

    public LogFileReaderContext setCodec(LogFileCodec codec) {
        this.codec = codec;
        return this;
    }

    public void incrementBlocksRead() {
        blocksRead++;
    }

    public long getBlocksRead() {
        return blocksRead;
    }

    public LogFileReaderContext setBlocksRead(long value) {
        blocksRead = value;
        return this;
    }

    public void incrementRecordsRead() {
        recordsRead++;
    }

    public long getRecordsRead() {
        return recordsRead;
    }

    public LogFileReaderContext setRecordsRead(long value) {
        recordsRead = value;
        return this;
    }

    public void incrementCorruptBlocksSkipped() {
        corruptBlocksSkipped++;
    }

    public long getCorruptBlocksSkipped() {
        return corruptBlocksSkipped;
    }

    public LogFileReaderContext setCorruptBlocksSkipped(long value) {
        corruptBlocksSkipped = value;
        return this;
    }

    public boolean isValidateTrailer() {
        return validateTrailer;
    }

    public LogFileReaderContext setValidateTrailer(boolean validateTrailer) {
        this.validateTrailer = validateTrailer;
        return this;
    }

    @Override
    public String toString() {
        return "LogFileReaderContext [filePath=" + path + ", fileSize=" + fileSize
            + ", isSkipCorruptBlocks=" + isSkipCorruptBlocks + ", codec=" + codec + ", blocksRead="
            + blocksRead + ", recordsRead=" + recordsRead + ", corruptBlocksSkipped="
            + corruptBlocksSkipped + ", validateTrailer=" + validateTrailer + "]";
    }

}
