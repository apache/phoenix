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

package org.apache.phoenix.hbase.index.scanner;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.phoenix.hbase.index.covered.KeyValueStore;

/**
 * Combine a simplified version of the logic in the ScanQueryMatcher and the KeyValueScanner. We can get away with this
 * here because we are only concerned with a single MemStore for the index; we don't need to worry about multiple column
 * families or minimizing seeking through file - we just want to iterate the kvs quickly, in-memory.
 */
public class FilteredKeyValueScanner implements ReseekableScanner {

    private ReseekableScanner delegate;
    private Filter filter;

    public FilteredKeyValueScanner(Filter filter, KeyValueStore store) {
        this(filter, store.getScanner());
    }

    private FilteredKeyValueScanner(Filter filter, ReseekableScanner delegate) {
        this.delegate = delegate;
        this.filter = filter;
    }

    @Override
    public Cell peek() throws IOException {
        return delegate.peek();
    }

    /**
     * Same a {@link KeyValueScanner#next()} except that we filter out the next {@link KeyValue} until we find one that
     * passes the filter.
     *
     * @return the next {@link KeyValue} or <tt>null</tt> if no next {@link KeyValue} is present and passes all the
     *         filters.
     */
    @Override
    public Cell next() throws IOException {
        seekToNextUnfilteredKeyValue();
        return delegate.next();
    }

    @Override
    public boolean seek(Cell key) throws IOException {
        if (filter.filterAllRemaining()) { return false; }
        // see if we can seek to the next key
        if (!delegate.seek(key)) { return false; }

        return seekToNextUnfilteredKeyValue();
    }

    private boolean seekToNextUnfilteredKeyValue() throws IOException {
        while (true) {
            Cell peeked = delegate.peek();
            // no more key values, so we are done
            if (peeked == null) { return false; }

            // filter the peeked value to see if it should be served
            ReturnCode code = filter.filterKeyValue(peeked);
            switch (code) {
            // included, so we are done
            case INCLUDE:
            case INCLUDE_AND_NEXT_COL:
                return true;
                // not included, so we need to go to the next row
            case SKIP:
            case NEXT_COL:
            case NEXT_ROW:
                delegate.next();
                break;
            // use a seek hint to find out where we should go
            case SEEK_NEXT_USING_HINT:
                delegate.seek(KeyValueUtil.ensureKeyValue(filter.getNextCellHint(peeked)));
            }
        }
    }

    public boolean reseek(Cell key) throws IOException {
        this.delegate.reseek(key);
        return this.seekToNextUnfilteredKeyValue();
    }


    @Override
    public void close() throws IOException {
        this.delegate.close();
    }

}
