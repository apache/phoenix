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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writer for Phoenix Replication Log files.
 * Manages writing the header, blocks (via LogFormatWriter), and trailer.
 */
public class LogWriter implements Log.Writer {

    private static final Logger LOG = LoggerFactory.getLogger(LogWriter.class);

    private LogWriterContext writerContext;
    private LogFormatWriter formatWriter;
    private FSDataOutputStream outputStream;
    private boolean closed = false;

    public LogWriter() {

    }

    public LogWriterContext getContext() {
        return writerContext;
    }

    @Override
    public void init(LogWriterContext context) throws IOException {
        this.writerContext = context;
        // TODO: Handle stream creation with proper permissions and overwrite options based on
        // config. For now we overwrite.
        this.outputStream = context.getFileSystem().create(writerContext.getFilePath(), true);
        this.formatWriter = new LogFormatWriter();  // Instantiate from conf when more than one
        this.formatWriter.init(context, outputStream); // Pass context for codec, allocator etc.
        LOG.debug("Initialized ReplicationLogWriter for path {}", writerContext.getFilePath());
    }

    @Override
    public void append(Log.Record record) throws IOException {
        if (closed) {
            throw new IOException("Writer has been closed");
        }
        formatWriter.append(record);
    }

    @Override
    public void sync() throws IOException {
        if (closed) {
            throw new IOException("Writer has been closed");
        }
        formatWriter.sync();
    }

    @Override
    public long getLength() throws IOException {
         if (closed) {
             // Attempt to get length from filesystem if stream is closed
             if (writerContext.getFileSystem().exists(writerContext.getFilePath())) {
                 return writerContext.getFileSystem()
                     .getFileStatus(writerContext.getFilePath()).getLen();
             } else {
                 throw new FileNotFoundException("Log file not found at path "
                     + writerContext.getFilePath());
             }
        }
        if (outputStream == null || formatWriter == null) {
            return 0; // Not initialized or already closed cleanly
        }
        // Return the current position for an open file being written
        return formatWriter.getPosition();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            // Close the final block and write the trailer
            if (formatWriter != null) {
                formatWriter.close();
            }
        } catch (IOException e) {
            LOG.error("Error closing LogFormatWriter for " + writerContext.getFilePath(), e);
            // Still attempt to close the underlying stream
            if (outputStream != null) {
                 try {
                     outputStream.close();
                 } catch (IOException nested) {
                      LOG.error("Error closing output stream for " + writerContext.getFilePath()
                          + " after format writer close failed", nested);
                 }
            }
            throw e;
        } finally {
            closed = true;
            LOG.debug("Closed LogWriter for path {}", writerContext.getFilePath());
        }
    }

    @Override
    public String toString() {
        return "LogWriter [writerContext=" + writerContext + ", formatWriter=" + formatWriter
            + ", closed=" + closed + "]";
    }

}
