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
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reader for Phoenix Replication Log files.
 * Manages reading the header, trailer, and iterating through blocks and records via
 * the LogFormatReader.
 */
public class LogFileReader implements LogFile.Reader  {

    private static final Logger LOG = LoggerFactory.getLogger(LogFileReader.class);
    private LogFileReaderContext context;
    private FSDataInputStream input;
    private LogFileFormatReader reader;
    private LogFile.Record current;
    private boolean closed = false;

    public LogFileReader() {

    }

    public LogFileReaderContext getContext() {
        return context;
    }

    @Override
    public void init(LogFileReaderContext context) throws IOException {
        this.context = context;
        this.input = context.getFileSystem().open(context.getFilePath());
        this.reader = new LogFileFormatReader(); // Instantiate from conf when more than one
        this.reader.init(context, input);
        LOG.debug("Initialized LogFileReader for path {}", context.getFilePath());
    }

    @Override
    public LogFile.Record next() throws IOException {
        return next(null);
    }

    @Override
    public LogFile.Record next(LogFile.Record reuse) throws IOException {
        if (closed) {
            throw new IOException("LogFileReader has been closed");
        }
        current = reader.next(reuse);
        return current;
    }

    @Override
    public Iterator<LogFile.Record> iterator() {
        return new Iterator<LogFile.Record>() {
            private LogFile.Record next = null;
            private boolean fetched = false;

            @Override
            public boolean hasNext() {
                if (closed) {
                    return false;
                }
                if (!fetched) {
                    try {
                        next = LogFileReader.this.next();
                        fetched = true;
                    } catch (IOException e) {
                        throw new LogFileIterationException(e);
                    }
                }
                return next != null;
            }

            @Override
            public LogFile.Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more records in the Log");
                }
                LogFile.Record record = next;
                // Reset state for the next hasNext() call
                next = null;
                fetched = false;
                return record;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException(
                    "Remove operation is not supported by this iterator");
            }
        };
    }

    @Override
    public LogFile.Header getHeader() {
        if (reader == null) {
             throw new IllegalStateException("LogFileReader not initialized");
        }
        return reader.getHeader();
    }

     @Override
    public LogFile.Trailer getTrailer() {
         if (reader == null) {
             throw new IllegalStateException("LogFileReader not initialized");
         }
        return reader.getTrailer();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            LOG.error("Error closing LogFileReader for path " + context.getFilePath(), e);
            throw e;
        } finally {
             closed = true;
             LOG.debug("Closed LogFileReader for path {}", context.getFilePath());
        }
    }

    @Override
    public String toString() {
        return "LogFileReader [readerContext=" + context + ", formatReader=" + reader
            + ", currentRecord=" + current + ", closed=" + closed + "]";
    }

}
