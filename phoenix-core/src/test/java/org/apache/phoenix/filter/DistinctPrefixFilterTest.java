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
package org.apache.phoenix.filter;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;

import junit.framework.TestCase;

public class DistinctPrefixFilterTest extends TestCase {
    private DistinctPrefixFilter createFilter(int[] widths, int prefixLength) {
        RowKeySchemaBuilder builder = new RowKeySchemaBuilder(widths.length);
        for (final int width : widths) {
            builder.addField(
                new PDatum() {

                @Override
                public boolean isNullable() {
                    return width <= 0;
                }

                @Override
                public PDataType<?> getDataType() {
                    return width <= 0 ? PVarchar.INSTANCE : PChar.INSTANCE;
                }

                @Override
                public Integer getMaxLength() {
                    return width <= 0 ? null : width;
                }

                @Override
                public Integer getScale() {
                    return null;
                }

                @Override
                public SortOrder getSortOrder() {
                    return SortOrder.getDefault();
                }

            }, width <= 0, SortOrder.getDefault());
        }
        return new DistinctPrefixFilter(builder.build(), prefixLength);
    }

    private void assertInclude(String next, Filter f) throws IOException {
        assertInclude(Bytes.toBytes(next), f);
    }

    private void assertInclude(byte[] next, Filter f) throws IOException {
        Cell c = new KeyValue(next, ByteUtil.EMPTY_BYTE_ARRAY, ByteUtil.EMPTY_BYTE_ARRAY, 0, ByteUtil.EMPTY_BYTE_ARRAY);
        assertTrue(f.filterKeyValue(c) == ReturnCode.INCLUDE);
        assertFalse(f.filterAllRemaining());
    }

    private void assertSeekAndHint(String next, Filter f, String rowHint) throws IOException {
        assertSeekAndHint(next, f, rowHint, false);
    }

    private void assertSeekAndHint(String next, Filter f, String rowHint, boolean filterAll) throws IOException {
        assertSeekAndHint(Bytes.toBytes(next), f, Bytes.toBytes(rowHint), filterAll);
    }

    private void assertSeekAndHint(byte[] next, Filter f, byte[] rowHint, boolean filterAll) throws IOException {
        Cell c = new KeyValue(next, ByteUtil.EMPTY_BYTE_ARRAY, ByteUtil.EMPTY_BYTE_ARRAY, 0, ByteUtil.EMPTY_BYTE_ARRAY);
        assertTrue(f.filterKeyValue(c) == ReturnCode.SEEK_NEXT_USING_HINT);
        Cell h = f.getNextCellHint(c);
        byte[] hintBytes = rowHint;
        assertTrue(Bytes.equals(hintBytes, 0, hintBytes.length, h.getRowArray(), h.getRowOffset(), h.getRowLength()));
        assertEquals(filterAll, f.filterAllRemaining());
    }

    public void testSingleFixedWidth() throws Exception {
        Filter f = createFilter(new int[]{3}, 1);
        assertInclude("000", f);
        assertInclude("001", f);
        assertSeekAndHint("001", f, "002");
        assertInclude("003", f);
        assertInclude("004", f);
        assertInclude("005", f);
        assertSeekAndHint("005", f, "006");

        f = createFilter(new int[]{3}, 1);
        f.setReversed(true);
        assertInclude("005", f);
        assertInclude("004", f);
        assertSeekAndHint(new byte[]{'0','0','4'}, f, new byte[]{'0','0','4'}, false);
        assertInclude("003", f);
        assertInclude("002", f);
        assertInclude("001", f);
        assertSeekAndHint(new byte[]{'0','0','1'}, f, new byte[]{'0','0','1'}, false);
    }

    public void testMultiFixedWidth() throws Exception {
        Filter f = createFilter(new int[]{5,4}, 1);
        assertInclude("00000aaaa", f);
        assertInclude("00001aaaa", f);
        assertSeekAndHint("00001aaaa", f, "00002");
        assertInclude("00003aaaa", f);
        assertInclude("00004aaaa", f);
        assertInclude("00005aaaa", f);
        assertSeekAndHint("00005aaaa", f, "00006");

        f = createFilter(new int[]{5,4}, 2);
        assertInclude("00000aaaa", f);
        assertInclude("00001aaaa", f);
        assertSeekAndHint("00001aaaa", f, "00001aaab");
        assertInclude("00003aaaa", f);
        assertInclude("00004aaaa", f);
        assertInclude("00005aaaa", f);
        assertSeekAndHint("00005aaaa", f, "00005aaab");

        f = createFilter(new int[]{3,2}, 1);
        f.setReversed(true);
        assertInclude("005aa", f);
        assertInclude("004aa", f);
        assertSeekAndHint(new byte[]{'0','0','4','a','a'}, f, new byte[]{'0','0','4'}, false);
        assertInclude("003aa", f);
        assertInclude("002aa", f);
        assertInclude("001aa", f);
        assertSeekAndHint(new byte[]{'0','0','1','a','a'}, f, new byte[]{'0','0','1'}, false);

        f = createFilter(new int[]{3,2}, 2);
        f.setReversed(true);
        assertInclude("005bb", f);
        assertInclude("004bb", f);
        assertInclude("003bb", f);
        assertSeekAndHint(new byte[]{'0','0','3','b','b'}, f, new byte[]{'0','0','3','b','b'}, false);
        assertInclude("003ba", f);
        assertInclude("002bb", f);
        assertInclude("001bb", f);
        assertSeekAndHint(new byte[]{'0','0','1','b','b'}, f, new byte[]{'0','0','1','b','b'}, false);
    }

    public void testSingleVariableWidth() throws Exception {
        Filter f = createFilter(new int[]{-5}, 1);
        assertInclude("00000", f);
        assertInclude("00001", f);
        assertSeekAndHint("00001", f, "00001\01");
        assertInclude("00003", f);
        assertInclude("00004", f);
        assertInclude("00005", f);
        assertSeekAndHint("00005", f, "00005\01");
    }

    public void testVariableWithNull() throws Exception {
        Filter f = createFilter(new int[]{-2,-2}, 1);
        assertInclude("\00aa", f);
        assertSeekAndHint("\00aa", f, "\01");
        assertSeekAndHint("\00aa", f, "\01");

        f = createFilter(new int[]{-2,-2}, 2);
        assertInclude("\00\00", f);
        assertSeekAndHint("\00\00", f, "\00\00\01");
        assertSeekAndHint("\00\00", f, "\00\00\01");
    }

    public void testMultiVariableWidth() throws Exception {
        Filter f = createFilter(new int[]{-5,-4}, 1);
        assertInclude("00000\00aaaa", f);
        assertInclude("00001\00aaaa", f);
        assertSeekAndHint("00001\00aaaa", f, "00001\01");
        assertInclude("00003\00aaaa", f);
        assertInclude("00004\00aaaa", f);
        assertInclude("00005\00aaaa", f);
        assertSeekAndHint("00005\00aaaa", f, "00005\01");

        f = createFilter(new int[]{-5,-4}, 2);
        assertInclude("00000\00aaaa", f);
        assertInclude("00001\00aaaa", f);
        assertSeekAndHint("00001\00aaaa", f, "00001\00aaaa\01");
        assertInclude("00003\00aaaa", f);
        assertInclude("00004\00aaaa", f);
        assertInclude("00005\00aaaa", f);
        assertSeekAndHint("00005\00aaaa", f, "00005\00aaaa\01");

        f = createFilter(new int[]{-3,-2}, 1);
        f.setReversed(true);
        assertInclude("005\00aa", f);
        assertInclude("004\00aa", f);
        assertSeekAndHint(new byte[]{'0','0','4', 0, 'a', 'a'}, f,
                new byte[] {'0','0','4'}, false);

        f = createFilter(new int[]{-3,-2}, 2);
        f.setReversed(true);
        assertInclude("005\00bb", f);
        assertInclude("004\00bb", f);
        assertSeekAndHint(new byte[]{'0','0','4', 0, 'b', 'b'}, f,
                new byte[]{'0','0','4', 0, 'b', 'b'}, false); 
    }

    public void testFixedAfterVariable() throws Exception {
        Filter f = createFilter(new int[]{-5,4}, 1);
        assertInclude("00000\00aaaa", f);
        assertInclude("00001\00aaaa", f);
        assertSeekAndHint("00001\00aaaa", f, "00001\01");
        assertInclude("00003\00aaaa", f);
        assertInclude("00004\00aaaa", f);
        assertInclude("00005\00aaaa", f);
        assertSeekAndHint("00005\00aaaa", f, "00005\01");

        f = createFilter(new int[]{-5,4}, 2);
        assertInclude("00000\00aaaa", f);
        assertInclude("00001\00aaaa", f);
        assertSeekAndHint("00001\00aaaa", f, "00001\00aaab");
        assertInclude("00003\00aaaa", f);
        assertInclude("00004\00aaaa", f);
        assertInclude("00005\00aaaa", f);
        assertSeekAndHint("00005\00aaaa", f, "00005\00aaab");
    }

    public void testVariableAfterFixed() throws Exception {
        Filter f = createFilter(new int[]{5,-4}, 1);
        assertInclude("00000aaaa", f);
        assertInclude("00001aaaa", f);
        assertSeekAndHint("00001aaaa", f, "00002");
        assertInclude("00003aaaa", f);
        assertInclude("00004aaaa", f);
        assertInclude("00005aaaa", f);
        assertSeekAndHint("00005aaaa", f, "00006");

        f = createFilter(new int[]{5,-4}, 2);
        assertInclude("00000aaaa", f);
        assertInclude("00001aaaa", f);
        assertSeekAndHint("00001aaaa", f, "00001aaaa\01");
        assertInclude("00003aaaa", f);
        assertInclude("00004aaaa", f);
        assertInclude("00005aaaa", f);
        assertSeekAndHint("00005aaaa", f, "00005aaaa\01");
    }

    public void testNoNextKey() throws Exception {
        Filter f = createFilter(new int[]{2,2}, 1);
        assertInclude("00cc", f);
        assertInclude(new byte[]{-1,-1,20,20}, f);
        // make sure we end the scan when we cannot increase a fixed length prefix
        assertSeekAndHint(new byte[]{-1,-1,20,20}, f, new byte[]{-1,-1}, true);
        assertSeekAndHint(new byte[]{-1,-1,20,20}, f, new byte[]{-1,-1}, true);

        f = createFilter(new int[]{2,2}, 1);
        f.setReversed(true);
        assertInclude(new byte[]{0,0,1,1}, f);
        assertSeekAndHint(new byte[]{0,0,1,1}, f, new byte[]{0,0}, false);
        assertSeekAndHint(new byte[]{0,0,1,1}, f, new byte[]{0,0}, false);
    }
}
