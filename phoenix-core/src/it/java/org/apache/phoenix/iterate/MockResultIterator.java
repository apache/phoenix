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

import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * Mock result iterator that returns its id as a string in a {@code Tuple} when {@link #next()} and {@link #peek()} are called. 
 */
public class MockResultIterator implements PeekingResultIterator {

    private final Tuple tuple;

    public MockResultIterator(String id, PTable table) throws SQLException {
        TupleProjector projector = new TupleProjector(table);
        List<Cell> result = new ArrayList<>();
        result.add(new KeyValue(Bytes.toBytes(id), SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(id)));
        this.tuple = projector.projectResults(new ResultTuple(Result.create(result)));
    }

    @Override
    public Tuple next() throws SQLException {
        return tuple;
    }

    @Override
    public void explain(List<String> planSteps) {}

    @Override
    public void close() throws SQLException {}

    @Override
    public Tuple peek() throws SQLException {
        return tuple;
    }

}
