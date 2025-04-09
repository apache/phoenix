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
import java.util.Random;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogFileCodecTest {

    private static final Logger LOG = LoggerFactory.getLogger(LogFileCodecTest.class);

    private static Random RNG = new Random();

    @Test
    public void testLogFileCodecSinglePut() throws IOException {
        singleRecordTest(LogFileTestUtil.newPutRecord("TBLPUT", 10L, "row1", 12345L, 1));
    }

    @Test
    public void testLogFileCodecSingleDelete() throws IOException {
        singleRecordTest(LogFileTestUtil.newDeleteRecord("TBLDEL", 10L, "row1", 12345L, 1));
    }

    @Test
    public void testLogFileCodecSingleDeleteColumn() throws IOException {
        singleRecordTest(LogFileTestUtil.newDeleteColumnRecord("TBLDELCOL", 10L, "row1",
            12345L, 1));
    }

    @Test
    public void testLogFileCodecSingleDeleteFamily() throws IOException {
        singleRecordTest(LogFileTestUtil.newDeleteFamilyRecord("TBLDELFAM", 10L, "row1",
            12345L, 1));
    }

    @Test
    public void testLogFileCodecSingleDeleteFamilyVersion() throws IOException {
        singleRecordTest(LogFileTestUtil.newDeleteFamilyVersionRecord("TBLDELFAMVER", 10L, "row1",
            12345L, 1));
    }

    private void singleRecordTest(LogFile.Record original) throws IOException {
        LogFileCodec codec = new LogFileCodec();
        // Encode
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        LogFile.Codec.Encoder encoder = codec.getEncoder(dos);
        encoder.write(original);
        LOG.debug("Encoded " + original);
        dos.close();
        byte[] encodedBytes = baos.toByteArray();

        // Decode
        ByteArrayInputStream bais = new ByteArrayInputStream(encodedBytes);
        DataInputStream dis = new DataInputStream(bais);
        LogFile.Codec.Decoder decoder = codec.getDecoder(dis);

        assertTrue("Should be able to advance decoder", decoder.advance(null));
        LogFileRecord decoded = (LogFileRecord) decoder.current();
        LOG.debug("Decoded " + decoded);

        // Verify length was set
        assertTrue("Serialized length should be greater than 0",
            decoded.getSerializedLength() > 0);
        LogFileTestUtil.assertRecordEquals("Decoded record should match original", original,
            decoded);

        assertFalse("Should be no more records", decoder.advance(null));
    }

    @Test
    public void testLogFileCodecReuseRecord() throws IOException {
        LogFileCodec codec = new LogFileCodec();
        LogFile.Record original1 = LogFileTestUtil.newPutRecord("TBL1", 100L, "row1", 12345L, 1);
        LogFile.Record original2 = LogFileTestUtil.newPutRecord("TBL2", 101L, "row2", 12346L, 2);

        // Encode
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        LogFile.Codec.Encoder encoder = codec.getEncoder(dos);
        encoder.write(original1);
        LOG.debug("Encoded " + original1);
        encoder.write(original2);
        LOG.debug("Encoded " + original2);
        dos.close();
        byte[] encodedBytes = baos.toByteArray();

        // Decode
        ByteArrayInputStream bais = new ByteArrayInputStream(encodedBytes);
        DataInputStream dis = new DataInputStream(bais);
        LogFile.Codec.Decoder decoder = codec.getDecoder(dis);

        LogFileRecord reusableRecord = new LogFileRecord();

        assertTrue("Should advance to first record", decoder.advance(reusableRecord));
        LogFileRecord decoded1 = (LogFileRecord) decoder.current();
        LOG.debug("Decoded: " + decoded1);
        assertSame("Should reuse the provided object for first record", reusableRecord, decoded1);
        LogFileTestUtil.assertRecordEquals("First decoded record should match", original1,
            decoded1);

        assertTrue("Should advance to second record", decoder.advance(reusableRecord));
        LogFileRecord decoded2 = (LogFileRecord) decoder.current();
        LOG.debug("Decoded: " + decoded2);
        assertSame("Should reuse the provided object for second record", reusableRecord, decoded2);
        LogFileTestUtil.assertRecordEquals("Second decoded record should match", original2,
            decoded2);

        assertFalse("Should be no more records", decoder.advance(reusableRecord));
    }

    @Test
    public void testLogFileCodecMultipleRecords() throws IOException {
        LogFileCodec codec = new LogFileCodec();
        List<LogFile.Record> originals = Arrays.asList(
            LogFileTestUtil.newPutRecord("TBL1", 100L, "row1", 12345L, 1),
            LogFileTestUtil.newPutRecord("TBL2", 101L, "row2", 12346L, 2),
            LogFileTestUtil.newPutRecord("TBL1", 102L, "row3", 12347L, 0) // No columns
        );

        // Encode
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        LogFile.Codec.Encoder encoder = codec.getEncoder(dos);
        for (LogFile.Record record : originals) {
            encoder.write(record);
            LOG.info("Encoded: size={} record={}", record.getSerializedLength(), record);
        }
        dos.close();
        byte[] encodedBytes = baos.toByteArray();

        // Decode
        ByteArrayInputStream bais = new ByteArrayInputStream(encodedBytes);
        DataInputStream dis = new DataInputStream(bais);
        LogFile.Codec.Decoder decoder = codec.getDecoder(dis);

        List<LogFile.Record> decodedRecords = new ArrayList<>();
        while (decoder.advance(null)) {
            LogFile.Record record = decoder.current();
            LOG.info("Decoded: record={}", record);
            decodedRecords.add(record);
        }

        assertEquals("Number of decoded records should match", originals.size(),
            decodedRecords.size());
        for (int i = 0; i < originals.size(); i++) {
            LogFileTestUtil.assertRecordEquals("Record " + i + " should match", originals.get(i),
                decodedRecords.get(i));
            assertTrue("Serialized length should be set",
                decodedRecords.get(i).getSerializedLength() > 0);
        }
    }

    @Test
    public void testLogFileCodecManyRecords() throws IOException {
        LogFileCodec codec = new LogFileCodec();
        List<LogFile.Record> originals = new ArrayList<>();
        for (int i = 0; i < 100_000; i++) {
            switch (RNG.nextInt(4)) {
                case 0:
                    originals.add(LogFileTestUtil.newPutRecord("TBLMR", 10L + i, "row" + i,
                        12345L + i, 1));
                    break;
                case 1:
                    originals.add(LogFileTestUtil.newDeleteRecord("TBLMR", 10L + i, "row" + i,
                        12345L + i, 1));
                    break;
                case 2:
                    originals.add(LogFileTestUtil.newDeleteFamilyRecord("TBLMR", 10L + i,
                        "row" + i, 12345L + i, 1));
                    break;
                case 3:
                    originals.add(LogFileTestUtil.newDeleteFamilyVersionRecord("TBLMR", 10L + i,
                        "row" + i, 12345L + i, 1));
                    break;
            }
        }

        // Encode
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        LogFile.Codec.Encoder encoder = codec.getEncoder(dos);
        long start = System.currentTimeMillis();
        for (LogFile.Record record : originals) {
            encoder.write(record);
        }
        dos.close();
        long end = System.currentTimeMillis();
        byte[] encodedBytes = baos.toByteArray();

        LOG.info("Encoded {} records into {} bytes (avg {} bytes/record) in {} ms",
            originals.size(), encodedBytes.length, encodedBytes.length / originals.size(),
            end - start);

        // Decode
        ByteArrayInputStream bais = new ByteArrayInputStream(encodedBytes);
        DataInputStream dis = new DataInputStream(bais);
        LogFile.Codec.Decoder decoder = codec.getDecoder(dis);
        List<LogFile.Record> decodedRecords = new ArrayList<>();
        start = System.currentTimeMillis();
        while (decoder.advance(null)) {
            LogFile.Record record = decoder.current();
            decodedRecords.add(record);
        }
        end = System.currentTimeMillis();

        LOG.info("Decoded {} records in {} ms", decodedRecords.size(), end - start);

        assertEquals("Number of decoded records should match", originals.size(),
            decodedRecords.size());
        for (int i = 0; i < originals.size(); i++) {
            LogFileTestUtil.assertRecordEquals("Record " + i + " should match", originals.get(i),
                decodedRecords.get(i));
            assertTrue("Serialized length should be set",
                decodedRecords.get(i).getSerializedLength() > 0);
        }
    }

    @Test
    public void testLogFileCodecDecodeFromByteBuffer() throws IOException {
        LogFileCodec codec = new LogFileCodec();
        LogFile.Record original = LogFileTestUtil.newPutRecord("TBLBB", 200L, "row1", 54321L, 1);

        // Encode
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        LogFile.Codec.Encoder encoder = codec.getEncoder(dos);
        encoder.write(original);
        dos.close();
        byte[] encodedBytes = baos.toByteArray();

        // Decode using ByteBuffer
        ByteBuffer buffer = ByteBuffer.wrap(encodedBytes);
        LogFile.Codec.Decoder decoder = codec.getDecoder(buffer);

        assertTrue("Should be able to advance decoder from ByteBuffer", decoder.advance(null));
        LogFileRecord decoded = (LogFileRecord) decoder.current();
        LogFileTestUtil.assertRecordEquals("Decoded record from ByteBuffer should match original",
            original, decoded);
        assertFalse("Should be no more records in ByteBuffer", decoder.advance(null));
    }

}
