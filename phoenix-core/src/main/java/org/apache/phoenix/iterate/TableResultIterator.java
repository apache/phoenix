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
package org.apache.phoenix.iterate;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.ServerUtil;


/**
 *
 * Wrapper for ResultScanner creation that closes HTableInterface
 * when ResultScanner is closed.
 *
 * 
 * @since 0.1
 */
public class TableResultIterator extends ExplainTable implements ResultIterator {
	public enum ScannerCreation {IMMEDIATE, DELAYED};
	
    private final Scan scan;
    private final HTableInterface htable;
    private volatile ResultIterator delegate;

    public TableResultIterator(StatementContext context, TableRef tableRef) throws SQLException {
        this(context, tableRef, context.getScan());
    }

    /*
     * Delay the creation of the underlying HBase ResultScanner if creationMode is DELAYED.
     * Though no rows are returned when the scanner is created, it still makes several RPCs
     * to open the scanner. In queries run serially (i.e. SELECT ... LIMIT 1), we do not
     * want to be hit with this cost when it's likely we'll never execute those scanners.
     */
    private ResultIterator getDelegate(boolean isClosing) throws SQLException {
        ResultIterator delegate = this.delegate;
        if (delegate == null) {
            synchronized (this) {
                delegate = this.delegate;
                if (delegate == null) {
                    try {
                        this.delegate = delegate = isClosing ? ResultIterator.EMPTY_ITERATOR : new ScanningResultIterator(htable.getScanner(scan));
                    } catch (IOException e) {
                        Closeables.closeQuietly(htable);
                        throw ServerUtil.parseServerException(e);
                    }
                }
            }
        }
        return delegate;
    }
    
    public TableResultIterator(StatementContext context, TableRef tableRef, Scan scan) throws SQLException {
        this(context, tableRef, scan, ScannerCreation.IMMEDIATE);
    }

    public TableResultIterator(StatementContext context, TableRef tableRef, Scan scan, ScannerCreation creationMode) throws SQLException {
        super(context, tableRef);
        this.scan = scan;
        htable = context.getConnection().getQueryServices().getTable(tableRef.getTable().getPhysicalName().getBytes());
        if (creationMode == ScannerCreation.IMMEDIATE) {
        	getDelegate(false);
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            getDelegate(true).close();
        } finally {
            try {
                htable.close();
            } catch (IOException e) {
                throw ServerUtil.parseServerException(e);
            }
        }
    }

    @Override
    public Tuple next() throws SQLException {
        return getDelegate(false).next();
    }

    @Override
    public void explain(List<String> planSteps) {
        StringBuilder buf = new StringBuilder();
        explain(buf.toString(),planSteps);
    }

	@Override
	public String toString() {
		return "TableResultIterator [htable=" + htable + ", scan=" + scan  + "]";
	}
}
