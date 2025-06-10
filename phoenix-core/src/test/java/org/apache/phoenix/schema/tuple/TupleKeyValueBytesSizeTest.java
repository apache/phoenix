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

package org.apache.phoenix.schema.tuple;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Test class for verifying the {@link Tuple#getSerializedSize()} implementation across
 * different tuple implementations.
 */
public class TupleKeyValueBytesSizeTest {

    private static final byte[] ROW = Bytes.toBytes(UUID.randomUUID().toString());
    private static final byte[] FAMILY = Bytes.toBytes(UUID.randomUUID().toString());

    @Test
    public void testEmptyTuple() {
        // Test empty tuple returns 0 bytes
        Tuple tuple = new SingleKeyValueTuple();
        assertEquals(0, tuple.getSerializedSize());
    }

    @Test
    public void testSingleKeyValueTuple() {
        // Test tuple with single KeyValue
        Cell kv = ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(ROW)
                .setFamily(FAMILY).setQualifier(Bytes.toBytes(UUID.randomUUID().toString()))
                .setTimestamp(1294754).setType(Cell.Type.Put)
                .setValue(Bytes.toBytes(UUID.randomUUID().toString())).build();
        SingleKeyValueTuple tuple = new SingleKeyValueTuple(kv);
        long expectedSize = ((KeyValue) kv).getLength();
        assertEquals(expectedSize, tuple.getSerializedSize());
    }

    @Test
    public void testMultiKeyValueTuple() {
        Cell kv1 = ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
                .setRow(ROW)
                .setFamily(FAMILY)
                .setQualifier(Bytes.toBytes(UUID.randomUUID().toString()))
                .setTimestamp(999999999)
                .setType(Cell.Type.Put)
                .setValue(Bytes.toBytes(UUID.randomUUID().toString()))
                .build();
        Cell kv2 = ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
                .setRow(ROW)
                .setFamily(FAMILY)
                .setQualifier(Bytes.toBytes(UUID.randomUUID().toString()))
                .setTimestamp(0)
                .setType(Cell.Type.Put)
                .setValue(Bytes.toBytes(UUID.randomUUID().toString()))
                .build();
        List<Cell> kvs = Arrays.asList(kv1, kv2);

        MultiKeyValueTuple tuple = new MultiKeyValueTuple(kvs);
        long expectedSize = ((KeyValue) kv1).getLength() + ((KeyValue) kv2).getLength();
        assertEquals(expectedSize, tuple.getSerializedSize());
    }

    @Test
    public void testDelegateTuple() {
        Cell kv = ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
                .setRow(ROW)
                .setFamily(FAMILY)
                .setQualifier(Bytes.toBytes(UUID.randomUUID().toString()))
                .setTimestamp(75304756904L)
                .setType(Cell.Type.Put)
                .setValue(Bytes.toBytes(UUID.randomUUID().toString()))
                .build();
        SingleKeyValueTuple delegate = new SingleKeyValueTuple(kv);
        DelegateTuple tuple = new DelegateTuple(delegate);
        assertEquals(delegate.getSerializedSize(), tuple.getSerializedSize());
    }

    @Test
    public void testMultiCellResultTuple() {
        byte[] row = Bytes.toBytes(UUID.randomUUID().toString());
        byte[] family = Bytes.toBytes(UUID.randomUUID().toString());
        byte[] qual1 = Bytes.toBytes(UUID.randomUUID().toString());
        byte[] qual2 = Bytes.toBytes(UUID.randomUUID().toString());
        byte[] qual3 = Bytes.toBytes(UUID.randomUUID().toString());
        byte[] val1 = Bytes.toBytes(UUID.randomUUID().toString());
        byte[] val2 = Bytes.toBytes(UUID.randomUUID().toString());
        byte[] val3 = Bytes.toBytes(UUID.randomUUID().toString());

        Cell cell1 = ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
                .setRow(row)
                .setFamily(family)
                .setQualifier(qual1)
                .setTimestamp(100L)
                .setType(Cell.Type.Put)
                .setValue(val1)
                .build();

        Cell cell2 = ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
                .setRow(row)
                .setFamily(family)
                .setQualifier(qual2)
                .setTimestamp(101L)
                .setType(Cell.Type.Put)
                .setValue(val2)
                .build();

        Cell cell3 = ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
                .setRow(row)
                .setFamily(family)
                .setQualifier(qual3)
                .setTimestamp(102L)
                .setType(Cell.Type.Put)
                .setValue(val3)
                .build();

        List<Cell> cells = Arrays.asList(cell1, cell2, cell3);
        ResultTuple tuple = new ResultTuple(Result.create(cells));

        long expectedSize = ((KeyValue) cell1).getLength() +
                ((KeyValue) cell2).getLength() +
                ((KeyValue) cell3).getLength();

        assertEquals(expectedSize, tuple.getSerializedSize());
    }

}