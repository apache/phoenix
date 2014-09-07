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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.SchemaUtil;
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
        TrustedByteArrayOutputStream os = new TrustedByteArrayOutputStream(table.length + region.length + fam.length
                + (NUM_KEY_PARTS - 1));
        os.write(table);
        os.write(QueryConstants.SEPARATOR_BYTE_ARRAY);
        os.write(fam);
        os.write(QueryConstants.SEPARATOR_BYTE_ARRAY);
        os.write(region);
        os.close();
        return os.getBuffer();
    }

    /**
     * Extracts the region name from the cell's row using the table name
     * 
     * @param table
     * @param cell
     * @return
     */
    public static byte[] getRegionFromRowKey(byte[] table, Cell cell) {
        int lengthOfTableKey = Bytes.toInt(cell.getRowArray(), (cell.getRowOffset() + cell.getRowLength())
                - ((Bytes.SIZEOF_INT * 3)), Bytes.SIZEOF_INT);
        int lengthOfRegionKey = Bytes.toInt(cell.getRowArray(), (cell.getRowOffset() + cell.getRowLength())
                - ((Bytes.SIZEOF_INT * 2)), Bytes.SIZEOF_INT);
        byte[] region = new byte[lengthOfRegionKey];
        System.arraycopy(cell.getRowArray(), cell.getRowOffset() + lengthOfTableKey, region, 0, lengthOfRegionKey);
        return region;
    }

    public static byte[] getCFFromRowKey(byte[] table, Cell cell) {
        int tableKeyLength = Bytes.toInt(cell.getRowArray(), (cell.getRowOffset() + cell.getRowLength())
                - ((Bytes.SIZEOF_INT * 3)), Bytes.SIZEOF_INT);
        int cfLength = Bytes.toInt(cell.getRowArray(), (cell.getRowOffset() + cell.getRowLength())
                - ((Bytes.SIZEOF_INT * 2)), Bytes.SIZEOF_INT);
        byte[] cf = new byte[cfLength];
        System.arraycopy(cell.getRowArray(), cell.getRowOffset() + tableKeyLength, cf, 0,
                cfLength);
        return cf;
    }
    
   public static byte[] getCFFromRowKey(byte[] table, byte[] row, int rowOffset, int rowLength) {
       // Move over the the sepeartor byte that would be written after the table name
        int startOff = Bytes.indexOf(row, table) + (table.length);
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
        System.arraycopy(row, startOff, cf, 0,
                cfLength);
        return cf;
    }

    /**
     * Extracts the table name from the cell's row.
     * 
     * @param cell
     * @return
     */
    public static byte[] getTableNameFromRowKey(Cell cell) {
        int lengthOfTableKey = Bytes.toInt(cell.getRowArray(), (cell.getRowOffset() + cell.getRowLength())
                - ((Bytes.SIZEOF_INT * 3)), Bytes.SIZEOF_INT);
        byte[] table = new byte[lengthOfTableKey];
        System.arraycopy(cell.getRowArray(), cell.getRowOffset(), table, 0, lengthOfTableKey);
        String tableNameFromFullName = SchemaUtil.getTableNameFromFullName(table);
        return Bytes.toBytes(tableNameFromFullName);
    }

    public static byte[] copyRow(KeyValue kv) {
        return Arrays.copyOfRange(kv.getRowArray(), kv.getRowOffset(), kv.getRowOffset() + kv.getRowLength());
    }
}