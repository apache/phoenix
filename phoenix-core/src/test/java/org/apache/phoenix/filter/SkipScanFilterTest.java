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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.ByteUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

//reset()
//filterAllRemaining() -> true indicates scan is over, false, keep going on.
//filterRowKey(byte[],int,int) -> true to drop this row, if false, we will also call
//filterKeyValue(KeyValue) -> true to drop this key/value
//filterRow(List) -> allows direct modification of the final list to be submitted
//filterRow() -> last chance to drop entire row based on the sequence of filterValue() calls. Eg: filter a row if it doesn't contain a specified column.
@RunWith(Parameterized.class)
public class SkipScanFilterTest extends TestCase {
    private final SkipScanFilter skipper;
    private final List<List<KeyRange>> cnf;
    private final List<Expectation> expectations;

    public SkipScanFilterTest(List<List<KeyRange>> cnf, int[] widths, List<Expectation> expectations) {
        this.expectations = expectations;
        this.cnf = cnf;
        RowKeySchemaBuilder builder = new RowKeySchemaBuilder(widths.length);
        for (final int width : widths) {
            builder.addField(
                new PDatum() {

                @Override
                public boolean isNullable() {
                    return width <= 0;
                }

                @Override
                public PDataType getDataType() {
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
        skipper = new SkipScanFilter(cnf, builder.build());
    }

    @Test
    public void test() throws IOException {
        System.out.println("CNF: " + cnf + "\n" + "Expectations: " + expectations);
        for (Expectation expectation : expectations) {
            expectation.examine(skipper);
        }
    }

    @Parameters(name="{0} {1} {2}")
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        testCases.addAll(
                foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("abc"), true, Bytes.toBytes("def"), true),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("dzy"), false, Bytes.toBytes("xyz"), false),
                },
                {
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("AA"), true, Bytes.toBytes("AB"), false),
                },
                {
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("AA"), true, Bytes.toBytes("AB"), false),
                },
                {
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("AA"), true, Bytes.toBytes("AB"), false),
                },
                {
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("AA"), true, Bytes.toBytes("AB"), false),
                }},
                new int[]{3,2,2,2,2},
                //new SeekNext("abcABABABAB", "abdAAAAAAAA"),
                new SeekNext("defAAABABAB", "dzzAAAAAAAA"),
                new Finished("xyyABABABAB"))
        );
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("j"), false, Bytes.toBytes("k"), true),
                    }},
                    new int[]{0},
                    new SeekNext(Bytes.toBytes("a"), ByteUtil.nextKey(new byte[] {'j',QueryConstants.SEPARATOR_BYTE})),
                    new Include("ja"),
                    new Include("jz"),
                    new Include("k"),
                    new Finished("ka")));
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("aaa"), true, Bytes.toBytes("aaa"), true),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("aac"), true, Bytes.toBytes("aad"), true),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("abc"), true, Bytes.toBytes("def"), true)
                }},
                new int[]{3},
                new SeekNext("aab", "aac"),
                new SeekNext("abb", "abc"),
                new Include("abc"),
                new Include("abe"),
                new Include("def"),
                new Finished("deg")));
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("aaa"), true, Bytes.toBytes("aaa"), true),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("abc"), false, Bytes.toBytes("def"), true)
                }},
                new int[]{3},
                new SeekNext("aba", "abd"),
                new Include("abe"),
                new Include("def"),
                new Finished("deg")));
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("aaa"), true, Bytes.toBytes("aaa"), true),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("abc"), false, Bytes.toBytes("def"), false)
                }},
                new int[]{3},
                new SeekNext("aba", "abd"),
                new Finished("def"))
        );
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("abc"), true, Bytes.toBytes("def"), true),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("dzy"), false, Bytes.toBytes("xyz"), false),
                }},
                new int[]{3},
                new Include("def"),
                new SeekNext("deg", "dzz"),
                new Include("eee"),
                new Finished("xyz"))
        );
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("aaa"), true, Bytes.toBytes("aaa"), true),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("abc"), true, Bytes.toBytes("abc"), true),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("def"), true, Bytes.toBytes("def"), true),
                },
                {
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("AB"), true, Bytes.toBytes("AX"), true),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("EA"), false, Bytes.toBytes("EZ"), false),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("PO"), true, Bytes.toBytes("PP"), false),
                }},
                new int[]{3,2},
                new Include("abcAB"),
                new SeekNext("abcAY","abcEB"),
                new Include("abcEF"),
                new SeekNext("abcPP","defAB"),
                new SeekNext("defEZ","defPO"),
                new Include("defPO"),
                new Finished("defPP")
                )
        );
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("AB"), true, Bytes.toBytes("AX"), true),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("EA"), false, Bytes.toBytes("EZ"), false),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("PO"), true, Bytes.toBytes("PP"), false),
                },
                {
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("abc"), true, Bytes.toBytes("abc"), true),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("def"), true, Bytes.toBytes("def"), true),
                }},
                new int[]{2,3},
                new Include("ABabc"),
                new SeekNext("ABdeg","ACabc"),
                new Include("AMabc"),
                new SeekNext("AYabc","EBabc"),
                new Include("EFabc"),
                new SeekNext("EZdef","POabc"),
                new SeekNext("POabd","POdef"),
                new Include("POdef"),
                new Finished("PPabc"))
        );
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("PO"), true, Bytes.toBytes("PP"), false),
                },
                {
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("def"), true, Bytes.toBytes("def"), true),
                }},
                new int[]{2,3},
                new Include("POdef"),
                new Finished("POdeg"))
        );
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("PO"), true, Bytes.toBytes("PO"), true),
                },
                {
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("def"), true, Bytes.toBytes("def"), true),
                }},
                new int[]{2,3},
                new Include("POdef"))
        );
        testCases.addAll(
                foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("AAA"), true, Bytes.toBytes("AAA"), true),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("abc"), true, Bytes.toBytes("def"), true),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("dzy"), false, Bytes.toBytes("xyz"), false),
                },
                {
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("AB"), true, Bytes.toBytes("AX"), true),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("EA"), false, Bytes.toBytes("EZ"), false),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("PO"), true, Bytes.toBytes("PP"), false),
                }},
                new int[]{3,2},
                new SeekNext("aaaAA", "abcAB"),
                new SeekNext("abcZZ", "abdAB"),
                new SeekNext("abdZZ", "abeAB"),
                new SeekNext(new byte[]{'d','e','a',(byte)0xFF,(byte)0xFF}, new byte[]{'d','e','b','A','B'}),
                new Include("defAB"),
                new Include("defAC"),
                new Include("defAW"),
                new Include("defAX"),
                new Include("defEB"),
                new Include("defPO"),
                new SeekNext("degAB", "dzzAB"),
                new Include("dzzAX"),
                new Include("dzzEY"),
                new SeekNext("dzzEZ", "dzzPO"),
                new Include("eeeAB"),
                new Include("eeeAC"),
                new SeekNext("eeeEA", "eeeEB"),
                new Include("eeeEF"),
                new SeekNext("eeeEZ","eeePO"),
                new Finished("xyzAA"))
        );
        testCases.addAll(
                foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("aaa"), true, Bytes.toBytes("aaa"), true),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("abc"), true, Bytes.toBytes("def"), true),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("dzz"), true, Bytes.toBytes("xyz"), false),
                }},
                new int[]{3},
                new SeekNext("abb", "abc"),
                new Include("abc"),
                new Include("abe"),
                new Finished("xyz"))
        );
        testCases.addAll(
                foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("abc"), true, Bytes.toBytes("def"), true),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("dzy"), false, Bytes.toBytes("xyz"), false),
                },
                {
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("AB"), true, Bytes.toBytes("AX"), true),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("EA"), false, Bytes.toBytes("EZ"), false),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("PO"), true, Bytes.toBytes("PP"), false),
                },
                {
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("100"), true, Bytes.toBytes("250"), false),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("700"), false, Bytes.toBytes("901"), false),
                }},
                new int[]{3,2,3},
                new SeekNext("abcEB700", "abcEB701"),
                new Include("abcEB701"),
                new SeekNext("dzzAB250", "dzzAB701"),
                new Finished("zzzAA000"))
        );
// TODO variable length columns
//        testCases.addAll(
//                foreach(new KeyRange[][]{{
//                    Char.INSTANCE.getKeyRange(Bytes.toBytes("apple"), true, Bytes.toBytes("lemon"), true),
//                    Char.INSTANCE.getKeyRange(Bytes.toBytes("pear"), false, Bytes.toBytes("yam"), false),
//                },
//                {
//                    Char.INSTANCE.getKeyRange(Bytes.toBytes("AB"), true, Bytes.toBytes("AX"), true),
//                    Char.INSTANCE.getKeyRange(Bytes.toBytes("EA"), false, Bytes.toBytes("EZ"), false),
//                    Char.INSTANCE.getKeyRange(Bytes.toBytes("PO"), true, Bytes.toBytes("PP"), false),
//                },
//                {
//                    Char.INSTANCE.getKeyRange(Bytes.toBytes("100"), true, Bytes.toBytes("250"), false),
//                    Char.INSTANCE.getKeyRange(Bytes.toBytes("700"), false, Bytes.toBytes("901"), false),
//                }},
//                new int[]{3,3})
//        );
        return testCases;
    }

    private static Collection<?> foreach(KeyRange[][] ranges, int[] widths, Expectation... expectations) {
        List<List<KeyRange>> cnf = Lists.transform(Lists.newArrayList(ranges), ARRAY_TO_LIST);
        List<Object> ret = Lists.newArrayList();
        ret.add(new Object[] {cnf, widths, Arrays.asList(expectations)} );
        return ret;
    }

    private static final Function<KeyRange[], List<KeyRange>> ARRAY_TO_LIST = new Function<KeyRange[], List<KeyRange>>() {
        @Override public List<KeyRange> apply(KeyRange[] input) {
            return Lists.newArrayList(input);
        }
    };

    static interface Expectation {
        void examine(SkipScanFilter skipper) throws IOException;
    }
    private static final class SeekNext implements Expectation {
        private final byte[] rowkey, hint;
        public SeekNext(String rowkey, String hint) {
            this.rowkey = Bytes.toBytes(rowkey);
            this.hint = Bytes.toBytes(hint);
        }
        public SeekNext(byte[] rowkey, byte[] hint) {
            this.rowkey = rowkey;
            this.hint = hint;
        }

        @SuppressWarnings("deprecation")
        @Override public void examine(SkipScanFilter skipper) throws IOException {
            KeyValue kv = KeyValue.createFirstOnRow(rowkey);
            skipper.reset();
            assertFalse(skipper.filterAllRemaining());
            assertFalse(skipper.filterRowKey(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength()));

            assertEquals(ReturnCode.SEEK_NEXT_USING_HINT, skipper.filterKeyValue(kv));
            assertEquals(KeyValue.createFirstOnRow(hint), skipper.getNextCellHint(kv));
        }

        @Override public String toString() {
            return "rowkey=" + Bytes.toStringBinary(rowkey)+", expected seek next using hint: " + Bytes.toStringBinary(hint);
        }
    }
    private static final class Include implements Expectation {
        private final byte[] rowkey;
        
        public Include(String rowkey) {
            this.rowkey = Bytes.toBytes(rowkey);
        }
        
        @SuppressWarnings("deprecation")
        @Override public void examine(SkipScanFilter skipper) throws IOException {
            KeyValue kv = KeyValue.createFirstOnRow(rowkey);
            skipper.reset();
            assertFalse(skipper.filterAllRemaining());
            assertFalse(skipper.filterRowKey(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength()));
            assertEquals(kv.toString(), ReturnCode.INCLUDE, skipper.filterKeyValue(kv));
        }

        @Override public String toString() {
            return "rowkey=" + Bytes.toStringBinary(rowkey)+", expected include";
        }
    }

    private static final class Finished implements Expectation {
        private final byte[] rowkey;
        public Finished(String rowkey) {
            this.rowkey = Bytes.toBytes(rowkey);
        }

        @Override public void examine(SkipScanFilter skipper) throws IOException {
            KeyValue kv = KeyValue.createFirstOnRow(rowkey);
            skipper.reset();
            assertEquals(ReturnCode.NEXT_ROW,skipper.filterKeyValue(kv));
            skipper.reset();
            assertTrue(skipper.filterAllRemaining());
        }

        @Override public String toString() {
            return "rowkey=" + Bytes.toStringBinary(rowkey)+", expected finished";
        }
    }
}
