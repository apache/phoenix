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
package org.apache.phoenix.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.junit.Test;

public class CsvBulkImportUtilTest {

    @Test
    public void testInitCsvImportJob() throws IOException {
        Configuration conf = new Configuration();

        char delimiter = '\001';
        char quote = '\002';
        char escape = '!';

        CsvBulkImportUtil.initCsvImportJob(conf, delimiter, quote, escape, null, null);

        // Serialize and deserialize the config to ensure that there aren't any issues
        // with non-printable characters as delimiters
        File tempFile = File.createTempFile("test-config", ".xml");
        FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
        conf.writeXml(fileOutputStream);
        fileOutputStream.close();
        Configuration deserialized = new Configuration();
        deserialized.addResource(new FileInputStream(tempFile));

        assertEquals(Character.valueOf('\001'),
                CsvBulkImportUtil.getCharacter(deserialized, CsvToKeyValueMapper.FIELD_DELIMITER_CONFKEY));
        assertEquals(Character.valueOf('\002'),
                CsvBulkImportUtil.getCharacter(deserialized, CsvToKeyValueMapper.QUOTE_CHAR_CONFKEY));
        assertEquals(Character.valueOf('!'),
                CsvBulkImportUtil.getCharacter(deserialized, CsvToKeyValueMapper.ESCAPE_CHAR_CONFKEY));
        assertNull(deserialized.get(CsvToKeyValueMapper.ARRAY_DELIMITER_CONFKEY));

        tempFile.delete();
    }

    @Test
    public void testConfigurePreUpsertProcessor() {
        Configuration conf = new Configuration();
        CsvBulkImportUtil.configurePreUpsertProcessor(conf, MockProcessor.class);
        ImportPreUpsertKeyValueProcessor processor = PhoenixConfigurationUtil.loadPreUpsertProcessor(conf);
        assertEquals(MockProcessor.class, processor.getClass());
    }

    @Test
    public void testGetAndSetChar_BasicChar() {
        Configuration conf = new Configuration();
        CsvBulkImportUtil.setChar(conf, "conf.key", '|');
        assertEquals(Character.valueOf('|'), CsvBulkImportUtil.getCharacter(conf, "conf.key"));
    }

    @Test
    public void testGetAndSetChar_NonPrintableChar() {
        Configuration conf = new Configuration();
        CsvBulkImportUtil.setChar(conf, "conf.key", '\001');
        assertEquals(Character.valueOf('\001'), CsvBulkImportUtil.getCharacter(conf, "conf.key"));
    }

    @Test
    public void testGetChar_NotPresent() {
        Configuration conf = new Configuration();
        assertNull(CsvBulkImportUtil.getCharacter(conf, "conf.key"));
    }

    public static class MockProcessor implements ImportPreUpsertKeyValueProcessor {

        @Override
        public List<KeyValue> preUpsert(byte[] rowKey, List<KeyValue> keyValues) {
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }
}
