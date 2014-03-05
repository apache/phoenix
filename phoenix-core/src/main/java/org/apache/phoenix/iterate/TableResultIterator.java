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

import com.google.common.io.Closeables;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.Tuple;
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
    private final HTableInterface htable;
    private final ResultIterator delegate;

    public TableResultIterator(StatementContext context, TableRef tableRef) throws SQLException {
        this(context, tableRef, context.getScan());
    }

    public TableResultIterator(StatementContext context, TableRef tableRef, Scan scan) throws SQLException {
        super(context, tableRef);
        htable = context.getConnection().getQueryServices().getTable(tableRef.getTable().getPhysicalName().getBytes());
        try {
            delegate = new ScanningResultIterator(htable.getScanner(scan));
        } catch (IOException e) {
            Closeables.closeQuietly(htable);
            throw ServerUtil.parseServerException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            delegate.close();
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
        return delegate.next();
    }

    @Override
    public void explain(List<String> planSteps) {
        StringBuilder buf = new StringBuilder();
        explain(buf.toString(),planSteps);
    }
}
