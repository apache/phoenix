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
package org.apache.phoenix.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.mapreduce.FormatToBytesWritableMapper.BadRecordWriter;
import org.apache.phoenix.mapreduce.FormatToBytesWritableMapper.MapperUpsertListener;
import org.apache.phoenix.mapreduce.bulkload.TableRowkeyPair;
import org.junit.Test;

/**
 * Unit tests for bad records writing functionality in {@link FormatToBytesWritableMapper}.
 * Tests the {@link BadRecordWriter} interface, {@link MapperUpsertListener} error handling,
 * and configuration of the bad records path option.
 */
public class BadRecordsWriterTest {

  @SuppressWarnings("unchecked")
  private Mapper<LongWritable, Text, TableRowkeyPair, ImmutableBytesWritable>.Context
    createMockContext() {
    Mapper<LongWritable, Text, TableRowkeyPair, ImmutableBytesWritable>.Context context =
      mock(Mapper.Context.class);
    Counter counter = mock(Counter.class);
    when(context.getCounter(anyString(), anyString())).thenReturn(counter);
    return context;
  }

  @Test
  public void testMapperUpsertListenerWritesBadRecordOnError() {
    Mapper<LongWritable, Text, TableRowkeyPair, ImmutableBytesWritable>.Context context =
      createMockContext();

    List<String> captured = new ArrayList<>();
    BadRecordWriter writer = (record, errorMessage) -> {
      captured.add(errorMessage + "\t" + record);
    };

    MapperUpsertListener<String> listener = new MapperUpsertListener<>(context, true, writer);
    listener.errorOnRecord("bad,record,data", new RuntimeException("type mismatch"));

    assertEquals(1, captured.size());
    assertEquals("type mismatch\tbad,record,data", captured.get(0));
  }

  @Test
  public void testMapperUpsertListenerNullBadRecordWriter() {
    Mapper<LongWritable, Text, TableRowkeyPair, ImmutableBytesWritable>.Context context =
      createMockContext();

    // Should not throw when badRecordWriter is null
    MapperUpsertListener<String> listener = new MapperUpsertListener<>(context, true, null);
    listener.errorOnRecord("bad,record,data", new RuntimeException("type mismatch"));
    // No assertion needed — just verifying no NPE
  }

  @Test(expected = RuntimeException.class)
  public void testMapperUpsertListenerPropagatesErrorWhenNotIgnoring() {
    Mapper<LongWritable, Text, TableRowkeyPair, ImmutableBytesWritable>.Context context =
      createMockContext();

    List<String> captured = new ArrayList<>();
    BadRecordWriter writer = (record, errorMessage) -> {
      captured.add(errorMessage + "\t" + record);
    };

    MapperUpsertListener<String> listener = new MapperUpsertListener<>(context, false, writer);
    listener.errorOnRecord("bad,record,data", new RuntimeException("type mismatch"));
  }

  @Test
  public void testMapperUpsertListenerWritesBadRecordBeforePropagating() {
    Mapper<LongWritable, Text, TableRowkeyPair, ImmutableBytesWritable>.Context context =
      createMockContext();

    List<String> captured = new ArrayList<>();
    BadRecordWriter writer = (record, errorMessage) -> {
      captured.add(errorMessage + "\t" + record);
    };

    MapperUpsertListener<String> listener = new MapperUpsertListener<>(context, false, writer);
    try {
      listener.errorOnRecord("bad,record,data", new RuntimeException("type mismatch"));
    } catch (RuntimeException e) {
      // expected
    }

    // Bad record should be written even when error is propagated
    assertEquals(1, captured.size());
    assertEquals("type mismatch\tbad,record,data", captured.get(0));
  }

  @Test
  public void testMapperUpsertListenerIncrementsErrorCounter() {
    Mapper<LongWritable, Text, TableRowkeyPair, ImmutableBytesWritable>.Context context =
      createMockContext();
    Counter counter = mock(Counter.class);
    when(context.getCounter("Phoenix MapReduce Import", "Errors on records")).thenReturn(counter);

    MapperUpsertListener<String> listener = new MapperUpsertListener<>(context, true, null);
    listener.errorOnRecord("record", new RuntimeException("error"));

    verify(counter).increment(1L);
  }

  @Test
  public void testMapperUpsertListenerUpsertDoneIncrementsCounter() {
    Mapper<LongWritable, Text, TableRowkeyPair, ImmutableBytesWritable>.Context context =
      createMockContext();
    Counter counter = mock(Counter.class);
    when(context.getCounter("Phoenix MapReduce Import", "Upserts Done")).thenReturn(counter);

    MapperUpsertListener<String> listener = new MapperUpsertListener<>(context, true, null);
    listener.upsertDone(1L);

    verify(counter).increment(1L);
  }

  @Test
  public void testBadRecordWriterFunctionalInterface() {
    // Verify that BadRecordWriter works as a functional interface with lambda
    List<String> captured = new ArrayList<>();
    BadRecordWriter writer = (record, errorMessage) -> {
      captured.add(record + "|" + errorMessage);
    };

    writer.write("1,Alice,Sales", "invalid column count");
    writer.write("2,\"Bob", "EOF reached before encapsulated token finished");

    assertEquals(2, captured.size());
    assertEquals("1,Alice,Sales|invalid column count", captured.get(0));
    assertEquals("2,\"Bob|EOF reached before encapsulated token finished", captured.get(1));
  }

  @Test
  public void testBadRecordWriterHandlesNullErrorMessage() {
    Mapper<LongWritable, Text, TableRowkeyPair, ImmutableBytesWritable>.Context context =
      createMockContext();

    List<String> captured = new ArrayList<>();
    BadRecordWriter writer = (record, errorMessage) -> {
      captured.add(errorMessage + "\t" + record);
    };

    MapperUpsertListener<String> listener = new MapperUpsertListener<>(context, true, writer);
    listener.errorOnRecord("record", new RuntimeException());

    assertEquals(1, captured.size());
    // When throwable.getMessage() is null, the writer receives null
    assertTrue(captured.get(0).startsWith("null\t"));
  }

  @Test
  public void testBadRecordsPathConfKey() {
    assertEquals("phoenix.mapreduce.import.badrecordspath",
      FormatToBytesWritableMapper.BAD_RECORDS_PATH_CONFKEY);
  }

  @Test
  public void testMapperUpsertListenerMultipleErrors() {
    Mapper<LongWritable, Text, TableRowkeyPair, ImmutableBytesWritable>.Context context =
      createMockContext();

    List<String> captured = new ArrayList<>();
    BadRecordWriter writer = (record, errorMessage) -> {
      captured.add(record);
    };

    MapperUpsertListener<String> listener = new MapperUpsertListener<>(context, true, writer);
    listener.errorOnRecord("record1", new RuntimeException("error1"));
    listener.errorOnRecord("record2", new RuntimeException("error2"));
    listener.errorOnRecord("record3", new RuntimeException("error3"));

    assertEquals(3, captured.size());
    assertEquals("record1", captured.get(0));
    assertEquals("record2", captured.get(1));
    assertEquals("record3", captured.get(2));
  }
}
