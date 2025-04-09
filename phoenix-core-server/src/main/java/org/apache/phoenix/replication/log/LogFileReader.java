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

import org.apache.hadoop.hbase.client.Mutation;
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
        this.reader = new LogFileFormatReader(); // Instantiate from conf when more than one
        this.reader.init(context,
            new FSDataInput(context.getFileSystem().open(context.getFilePath())));
        LOG.debug("Initialized LogFileReader for path {}", context.getFilePath());
    }

    @Override
    public Mutation next() throws IOException {
        if (closed) {
            throw new IOException("LogFileReader has been closed");
        }
        current = reader.next(current);
        if (current == null) {
            return null;
        }
        // NOTE: We don't currently do anything with the Record's table name or commit id, but may
        // some day.
        return current.getMutation();
    }

    @Override
    public Iterator<Mutation> iterator() {
        return new Iterator<Mutation>() {
            private Mutation next = null;
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
            public Mutation next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more records in the Log");
                }
                Mutation mutation = next;
                // Reset state for the next hasNext() call
                next = null;
                fetched = false;
                return mutation;
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
