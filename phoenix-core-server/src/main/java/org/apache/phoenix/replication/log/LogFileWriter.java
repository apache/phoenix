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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writer for Phoenix Replication Log files.
 * Manages writing the header, blocks (via LogFormatWriter), and trailer.
 */
public class LogFileWriter implements LogFile.Writer {

    private static final Logger LOG = LoggerFactory.getLogger(LogFileWriter.class);

    private LogFileWriterContext context;
    private LogFileFormatWriter writer;
    private boolean closed = false;

    public LogFileWriter() {

    }

    public LogFileWriterContext getContext() {
        return context;
    }

    @Override
    public void init(LogFileWriterContext context) throws IOException {
        this.context = context;
        this.writer = new LogFileFormatWriter();
        // TODO: Handle stream creation with proper permissions and overwrite options based on
        // config. For now we overwrite.
        this.writer.init(context,
            new HDFSDataOutput(context.getFileSystem().create(context.getFilePath(),
                true)));
        LOG.debug("Initialized LogFileWriter for path {}", context.getFilePath());
    }

    @Override
    public void append(String tableName, long commitId, Mutation mutation) throws IOException {
        if (closed) {
            throw new IOException("Writer has been closed");
        }
        writer.append(new LogFileRecord().setHBaseTableName(tableName).setCommitId(commitId)
            .setMutation(mutation));
    }

    @Override
    public void sync() throws IOException {
        if (closed) {
            throw new IOException("Writer has been closed");
        }
        writer.sync();
    }

    @Override
    public long getLength() throws IOException {
        if (closed) {
            // Attempt to get length from filesystem if stream is closed
            if (context.getFileSystem().exists(context.getFilePath())) {
                return context.getFileSystem().getFileStatus(context.getFilePath()).getLen();
            } else {
                throw new FileNotFoundException("LogFile not found at path "
                    + context.getFilePath());
            }
        }
        if (writer == null) {
            return 0; // Not initialized or already closed cleanly
        }
        // Return the current position for an open file being written
        return writer.getPosition();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            // Close the final block and write the trailer
            if (writer != null) {
                writer.close();
            }
        } finally {
            closed = true;
            LOG.debug("Closed LogFileWriter for path {}", context.getFilePath());
        }
    }

    @Override
    public String toString() {
        return "LogFileWriter [writerContext=" + context + ", formatWriter=" + writer
            + ", closed=" + closed + "]";
    }

}
