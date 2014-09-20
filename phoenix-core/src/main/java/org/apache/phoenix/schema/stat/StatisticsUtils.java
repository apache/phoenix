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
package org.apache.phoenix.schema.stat;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;
/**
 * Simple utility class for managing multiple key parts of the statistic
 */
public class StatisticsUtils {

    private StatisticsUtils() {
        // private ctor for utility classes
    }

    /** Number of parts in our complex key */
    protected static final int NUM_KEY_PARTS = 3;

    public static byte[] getRowKey(byte[] table, byte[] fam, byte[] region) throws IOException {
        // always starts with the source table
        TrustedByteArrayOutputStream os = new TrustedByteArrayOutputStream(table.length + region.length
                + fam.length + (NUM_KEY_PARTS - 1));
        os.write(table);
        os.write(QueryConstants.SEPARATOR_BYTE_ARRAY);
        os.write(fam);
        os.write(QueryConstants.SEPARATOR_BYTE_ARRAY);
        os.write(region);
        os.close();
        return os.getBuffer();
    }
    
    public static byte[] getRowKeyForTSUpdate(byte[] table) throws IOException {
        // always starts with the source table
        TrustedByteArrayOutputStream os = new TrustedByteArrayOutputStream(table.length);
        os.write(table);
        os.close();
        return os.getBuffer();
    }

    public static byte[] getCFFromRowKey(byte[] table, byte[] row, int rowOffset, int rowLength) {
        // Move over the the sepeartor byte that would be written after the table name
        int startOff = Bytes.indexOf(row, table) + (table.length) + 1;
        int endOff = startOff;
        while (endOff < rowLength) {
            // Check for next seperator byte
            if (row[endOff] != QueryConstants.SEPARATOR_BYTE) {
                endOff++;
            } else {
                break;
            }
        }
        int cfLength = endOff - startOff;
        byte[] cf = new byte[cfLength];
        System.arraycopy(row, startOff, cf, 0, cfLength);
        return cf;
    }

    public static byte[] copyRow(KeyValue kv) {
        return Arrays.copyOfRange(kv.getRowArray(), kv.getRowOffset(), kv.getRowOffset() + kv.getRowLength());
    }
}