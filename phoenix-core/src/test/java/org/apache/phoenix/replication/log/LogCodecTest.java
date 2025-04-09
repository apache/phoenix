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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class LogCodecTest {

    @Test
    public void testLogCodecSingleRecord() throws IOException {
        LogCodec codec = new LogCodec();
        Log.Record originalRecord = newRecord("TBL1", 100L, "row1", 12345L, 1);

        // Encode
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        Log.Codec.Encoder encoder = codec.getEncoder(dos);
        encoder.write(originalRecord);
        dos.close();
        byte[] encodedBytes = baos.toByteArray();

        // Decode
        ByteArrayInputStream bais = new ByteArrayInputStream(encodedBytes);
        DataInputStream dis = new DataInputStream(bais);
        Log.Codec.Decoder decoder = codec.getDecoder(dis);

        assertTrue("Should be able to advance decoder", decoder.advance(null));
        LogRecord decodedRecord = (LogRecord) decoder.current();

        // Verify length was set
        assertTrue("Serialized length should be greater than 0",
            decodedRecord.getSerializedLength() > 0);
        // Verify content (assuming LogRecord has a proper equals or using field comparison)
        assertEquals("Decoded record should match original", originalRecord, decodedRecord);

        assertFalse("Should be no more records", decoder.advance(null));
    }

    @Test
    public void testLogCodecReuseRecord() throws IOException {
        LogCodec codec = new LogCodec();
        Log.Record originalRecord1 = newRecord("TBL1", 100L, "row1", 12345L, 1);
        Log.Record originalRecord2 = newRecord("TBL2", 101L, "row2", 12346L, 2);

        // Encode
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        Log.Codec.Encoder encoder = codec.getEncoder(dos);
        encoder.write(originalRecord1);
        encoder.write(originalRecord2);
        dos.close();
        byte[] encodedBytes = baos.toByteArray();

        // Decode
        ByteArrayInputStream bais = new ByteArrayInputStream(encodedBytes);
        DataInputStream dis = new DataInputStream(bais);
        Log.Codec.Decoder decoder = codec.getDecoder(dis);

        LogRecord reusableRecord = new LogRecord();

        assertTrue("Should advance to first record", decoder.advance(reusableRecord));
        LogRecord decoded1 = (LogRecord) decoder.current();
        assertSame("Should reuse the provided object for first record", reusableRecord, decoded1);
        assertEquals("First decoded record should match", originalRecord1, decoded1);

        assertTrue("Should advance to second record", decoder.advance(reusableRecord));
        LogRecord decoded2 = (LogRecord) decoder.current();
        assertSame("Should reuse the provided object for second record", reusableRecord, decoded2);
        assertEquals("Second decoded record should match", originalRecord2, decoded2);

        assertFalse("Should be no more records", decoder.advance(reusableRecord));
    }

    @Test
    public void testLogCodecMultipleRecords() throws IOException {
        LogCodec codec = new LogCodec();
        List<Log.Record> originalRecords = Arrays.asList(
            newRecord("TBL1", 100L, "row1", 12345L, 1),
            newRecord("TBL2", 101L, "row2", 12346L, 2),
            newRecord("TBL1", 102L, "row3", 12347L, 0) // No columns
        );

        // Encode
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        Log.Codec.Encoder encoder = codec.getEncoder(dos);
        for (Log.Record record : originalRecords) {
            encoder.write(record);
        }
        dos.close();
        byte[] encodedBytes = baos.toByteArray();

        // Decode
        ByteArrayInputStream bais = new ByteArrayInputStream(encodedBytes);
        DataInputStream dis = new DataInputStream(bais);
        Log.Codec.Decoder decoder = codec.getDecoder(dis);

        List<LogRecord> decodedRecords = new ArrayList<>();
        while (decoder.advance(null)) {
            decodedRecords.add((LogRecord) decoder.current());
        }

        assertEquals("Number of decoded records should match", originalRecords.size(),
            decodedRecords.size());
        for (int i = 0; i < originalRecords.size(); i++) {
            assertEquals("Record " + i + " should match", originalRecords.get(i),
                decodedRecords.get(i));
            assertTrue("Serialized length should be set",
                decodedRecords.get(i).getSerializedLength() > 0);
        }
    }

    @Test
    public void testLogCodecDecodeFromByteBuffer() throws IOException {
        LogCodec codec = new LogCodec();
        Log.Record originalRecord = newRecord("TBLBB", 200L, "row_bb", 54321L, 1);

        // Encode
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        Log.Codec.Encoder encoder = codec.getEncoder(dos);
        encoder.write(originalRecord);
        dos.close();
        byte[] encodedBytes = baos.toByteArray();

        // Decode using ByteBuffer
        ByteBuffer buffer = ByteBuffer.wrap(encodedBytes);
        Log.Codec.Decoder decoder = codec.getDecoder(buffer);

        assertTrue("Should be able to advance decoder from ByteBuffer", decoder.advance(null));
        LogRecord decodedRecord = (LogRecord) decoder.current();
        assertEquals("Decoded record from ByteBuffer should match original", originalRecord,
            decodedRecord);
        assertFalse("Should be no more records in ByteBuffer", decoder.advance(null));
    }

    private Log.Record newRecord(String table, long commitId, String rowKey, long ts, int numCols) {
        Log.Record record = new LogRecord()
            .setMutationType(Log.MutationType.PUT)
            .setSchemaObjectName(table)
            .setCommitId(commitId)
            .setRowKey(Bytes.toBytes(rowKey))
            .setTimestamp(ts);
        for (int i = 0; i < numCols; i++) {
            record.addColumnValue(Bytes.toBytes("col" + i), Bytes.toBytes("v" + i + "_" + rowKey));
        }
        return record;
    }

}
