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
public class LogReader implements Log.Reader  {

    private static final Logger LOG = LoggerFactory.getLogger(LogReader.class);
    private LogReaderContext readerContext;
    private FSDataInputStream in;
    private LogFormatReader formatReader;
    private Log.Record currentRecord;
    private boolean closed = false;

    public LogReader() {

    }

    public LogReaderContext getContext() {
        return readerContext;
    }

    @Override
    public void init(LogReaderContext context) throws IOException {
        this.readerContext = context;
        this.in = context.getFileSystem().open(readerContext.getFilePath());
        this.formatReader = new LogFormatReader(); // Instantiate from conf when more than one
        this.formatReader.init(context, in);
        LOG.debug("Initialized LogReader for path {}", readerContext.getFilePath());
    }

    @Override
    public Log.Record next() throws IOException {
        return next(null);
    }

    @Override
    public Log.Record next(Log.Record reuse) throws IOException {
         if (closed) {
            throw new IOException("Reader has been closed");
        }
        currentRecord = formatReader.next(reuse);
        return currentRecord;
    }

    @Override
    public Iterator<Log.Record> iterator() {
       return new Iterator<Log.Record>() {
           private Log.Record nextRecord = null;
           private boolean fetched = false;

           @Override
           public boolean hasNext() {
               if (closed) {
                   return false;
               }
               if (!fetched) {
                   try {
                       nextRecord = LogReader.this.next();
                       fetched = true;
                   } catch (IOException e) {
                       throw new LogIterationException(e);
                   }
               }
               return nextRecord != null;
           }

           @Override
           public Log.Record next() {
               if (!hasNext()) {
                   throw new NoSuchElementException("No more records in the Replication Log");
               }
               Log.Record record = nextRecord;
               // Reset state for the next hasNext() call
               nextRecord = null;
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
    public Log.Header getHeader() {
        if (formatReader == null) {
             throw new IllegalStateException("LogReader not initialized");
        }
        return formatReader.getHeader();
    }

     @Override
    public Log.Trailer getTrailer() {
         if (formatReader == null) {
             throw new IllegalStateException("LogReader not initialized");
         }
        return formatReader.getTrailer();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            if (formatReader != null) {
                formatReader.close();
            }
            // inputStream is closed by formatReader
        } catch (IOException e) {
            LOG.error("Error closing LogReader for path " + readerContext.getFilePath(), e);
            throw e;
        } finally {
             closed = true;
             LOG.debug("Closed LogReader for path {}", readerContext.getFilePath());
        }
    }

    @Override
    public String toString() {
        return "LogReader [readerContext=" + readerContext + ", formatReader=" + formatReader
            + ", currentRecord=" + currentRecord + ", closed=" + closed + "]";
    }

}
