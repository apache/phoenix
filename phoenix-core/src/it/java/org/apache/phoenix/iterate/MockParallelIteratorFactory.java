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

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.schema.PTable;

/**
 * Iterator factory that creates {@code MockTableResultIterator}
 */
public class MockParallelIteratorFactory implements ParallelIteratorFactory {
    private static final AtomicInteger counter = new AtomicInteger(1);
    private PTable table;
    
    @Override
    public PeekingResultIterator newIterator(StatementContext context, ResultIterator scanner, Scan scan,
            String physicalTableName) throws SQLException {
        return new MockResultIterator(String.valueOf(counter.incrementAndGet()), table);
    }
    
    public void setTable(PTable table) {
        this.table = table;
    }
    
}

    

