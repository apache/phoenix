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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import junit.framework.TestCase;

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

    public SkipScanFilterTest(List<List<KeyRange>> cnf, int[] widths, int[] slotSpans,List<Expectation> expectations) {
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
        if(slotSpans==null) {
            skipper = new SkipScanFilter(cnf, builder.build());
        } else {
            skipper = new SkipScanFilter(cnf, slotSpans,builder.build());
        }
    }

    @Test
    public void test() throws IOException {
        for (Expectation expectation : expectations) {
            expectation.examine(skipper);
        }
    }

    @Parameters(name="{0} {1} {3}")
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        // Variable length tests
        testCases.addAll(
                foreach(new KeyRange[][]{{
                    PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),
                    PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("e"), true, Bytes.toBytes("e"), true),
                    PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("f"), true, Bytes.toBytes("f"), true),
                },
                {
                    PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true),
                },
                {
                    KeyRange.EVERYTHING_RANGE,
                },
                {
                    PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                }},
                new int[4],
                null,
                new Include(ByteUtil.concat(Bytes.toBytes("a"),QueryConstants.SEPARATOR_BYTE_ARRAY, 
                                            Bytes.toBytes("b"), QueryConstants.SEPARATOR_BYTE_ARRAY,
                                            QueryConstants.SEPARATOR_BYTE_ARRAY,
                                            Bytes.toBytes("1") ) ),
                new SeekNext(ByteUtil.concat(Bytes.toBytes("e.f"),QueryConstants.SEPARATOR_BYTE_ARRAY, 
                                             Bytes.toBytes("b"), QueryConstants.SEPARATOR_BYTE_ARRAY,
                                             QueryConstants.SEPARATOR_BYTE_ARRAY,
                                             Bytes.toBytes("1") ), 
                            ByteUtil.concat(Bytes.toBytes("f"),QueryConstants.SEPARATOR_BYTE_ARRAY, 
                                            Bytes.toBytes("b") )),
                new Include(ByteUtil.concat(Bytes.toBytes("f"),QueryConstants.SEPARATOR_BYTE_ARRAY, 
                                            Bytes.toBytes("b"), QueryConstants.SEPARATOR_BYTE_ARRAY,
                                            QueryConstants.SEPARATOR_BYTE_ARRAY,
                                            Bytes.toBytes("1") ) ) )
        );
        testCases.addAll(
                foreach(new KeyRange[][]{{
                    PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("20160116121006"), true, Bytes.toBytes("20160116181006"), true),
                },
                {
                    PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("2404787"), true, Bytes.toBytes("2404787"), true),
                }/*,
                {
                    KeyRange.EVERYTHING_RANGE,
                },
                {
                    KeyRange.EVERYTHING_RANGE,
                }*/},
                new int[4],
                null,
                new SeekNext(ByteUtil.concat(Bytes.toBytes("20160116141006"),QueryConstants.SEPARATOR_BYTE_ARRAY, 
                                            QueryConstants.SEPARATOR_BYTE_ARRAY,
                                            Bytes.toBytes("servlet") ),
                             ByteUtil.concat(Bytes.toBytes("20160116141006"),QueryConstants.SEPARATOR_BYTE_ARRAY, 
                                             Bytes.toBytes("2404787") )),
                new Include(ByteUtil.concat(Bytes.toBytes("20160116151006"),QueryConstants.SEPARATOR_BYTE_ARRAY, 
                                            Bytes.toBytes("2404787"), QueryConstants.SEPARATOR_BYTE_ARRAY,
                                            Bytes.toBytes("jdbc"), QueryConstants.SEPARATOR_BYTE_ARRAY ) ) )
        );
        // Fixed length tests
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
                null,
                new SeekNext("defAAABABAB", "dzzAAAAAAAA"),
                new Finished("xyyABABABAB"))
        );
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("j"), false, Bytes.toBytes("k"), true),
                    }},
                    new int[]{0},
                    null,
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
                null,
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
                null,
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
                null,
                new SeekNext("aba", "abd"),
                new Finished("def"))
        );
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("abc"), true, Bytes.toBytes("def"), true),
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("dzy"), false, Bytes.toBytes("xyz"), false),
                }},
                new int[]{3},
                null,
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
                null,
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
                null,
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
                null,
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
                null,
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
                null,
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
                null,
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
                null,
                new SeekNext("abcEB700", "abcEB701"),
                new Include("abcEB701"),
                new SeekNext("dzzAB250", "dzzAB701"),
                new Finished("zzzAA000"))
        );
        //for PHOENIX-3705
        testCases.addAll(
                foreach(
                    new KeyRange[][]{{
                        PInteger.INSTANCE.getKeyRange(PInteger.INSTANCE.toBytes(1), true, PInteger.INSTANCE.toBytes(4), true)
                    },
                    {
                        KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(5)),
                        KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(7))
                    },
                    {
                        PInteger.INSTANCE.getKeyRange(PInteger.INSTANCE.toBytes(9), true, PInteger.INSTANCE.toBytes(10), true)
                    }},
                    new int[]{4,4,4},
                    null,
                    new SeekNext(
                            ByteUtil.concat(
                                    PInteger.INSTANCE.toBytes(2),
                                    PInteger.INSTANCE.toBytes(7),
                                    PInteger.INSTANCE.toBytes(11)),
                            ByteUtil.concat(
                                    PInteger.INSTANCE.toBytes(3),
                                    PInteger.INSTANCE.toBytes(5),
                                    PInteger.INSTANCE.toBytes(9))),
                    new Finished(ByteUtil.concat(
                            PInteger.INSTANCE.toBytes(4),
                            PInteger.INSTANCE.toBytes(7),
                            PInteger.INSTANCE.toBytes(11))))
        );
        testCases.addAll(
            foreach(
                new KeyRange[][]{{
                    KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(1)),
                    KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(3)),
                    KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(4))
                },
                {
                    KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(5)),
                    KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(7))
                },
                {
                    PInteger.INSTANCE.getKeyRange(PInteger.INSTANCE.toBytes(9), true, PInteger.INSTANCE.toBytes(10), true)
                }},
                new int[]{4,4,4},
                null,
                new SeekNext(
                        ByteUtil.concat(
                                PInteger.INSTANCE.toBytes(3),
                                PInteger.INSTANCE.toBytes(7),
                                PInteger.INSTANCE.toBytes(11)),
                        ByteUtil.concat(
                                PInteger.INSTANCE.toBytes(4),
                                PInteger.INSTANCE.toBytes(5),
                                PInteger.INSTANCE.toBytes(9))),
                new Finished(ByteUtil.concat(
                        PInteger.INSTANCE.toBytes(4),
                        PInteger.INSTANCE.toBytes(7),
                        PInteger.INSTANCE.toBytes(11))))
        );
        //for RVC
        testCases.addAll(
            foreach(
                new KeyRange[][]{
                {
                    KeyRange.getKeyRange(
                            ByteUtil.concat(PInteger.INSTANCE.toBytes(1),PInteger.INSTANCE.toBytes(2)),
                            true,
                            ByteUtil.concat(PInteger.INSTANCE.toBytes(3),PInteger.INSTANCE.toBytes(4)),
                            true)
                },
                {
                    KeyRange.getKeyRange(
                            ByteUtil.concat(PInteger.INSTANCE.toBytes(5),PInteger.INSTANCE.toBytes(6)),
                            true,
                            ByteUtil.concat(PInteger.INSTANCE.toBytes(7),PInteger.INSTANCE.toBytes(8)),
                            true)
                }},
                new int[]{4,4,4,4},
                new int[]{1,1},
                new Include(
                        ByteUtil.concat(
                                PInteger.INSTANCE.toBytes(2),
                                PInteger.INSTANCE.toBytes(3),
                                PInteger.INSTANCE.toBytes(6),
                                PInteger.INSTANCE.toBytes(7))),
                new SeekNext(
                        ByteUtil.concat(
                                PInteger.INSTANCE.toBytes(2),
                                PInteger.INSTANCE.toBytes(3),
                                PInteger.INSTANCE.toBytes(7),
                                PInteger.INSTANCE.toBytes(9)),
                        ByteUtil.concat(
                                PInteger.INSTANCE.toBytes(2),
                                PInteger.INSTANCE.toBytes(4),
                                PInteger.INSTANCE.toBytes(5),
                                PInteger.INSTANCE.toBytes(6))),
                new Finished(
                        ByteUtil.concat(
                                PInteger.INSTANCE.toBytes(3),
                                PInteger.INSTANCE.toBytes(4),
                                PInteger.INSTANCE.toBytes(7),
                                PInteger.INSTANCE.toBytes(9))))
        );
        testCases.addAll(
            foreach(
                new KeyRange[][]{
                {
                    KeyRange.getKeyRange(
                            ByteUtil.concat(PInteger.INSTANCE.toBytes(1),PInteger.INSTANCE.toBytes(2)),
                            true,
                            ByteUtil.concat(PInteger.INSTANCE.toBytes(3),PInteger.INSTANCE.toBytes(4)),
                            true)
                },
                {
                    KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(5)),
                    KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(7))
                },
                {
                    PInteger.INSTANCE.getKeyRange(PInteger.INSTANCE.toBytes(9), true, PInteger.INSTANCE.toBytes(10), true)
                }},
                new int[]{4,4,4,4},
                new int[]{1,0,0},
                new Include(
                        ByteUtil.concat(
                                PInteger.INSTANCE.toBytes(1),
                                PInteger.INSTANCE.toBytes(3),
                                PInteger.INSTANCE.toBytes(5),
                                PInteger.INSTANCE.toBytes(9))),
                new SeekNext(
                        ByteUtil.concat(
                                PInteger.INSTANCE.toBytes(2),
                                PInteger.INSTANCE.toBytes(3),
                                PInteger.INSTANCE.toBytes(7),
                                PInteger.INSTANCE.toBytes(11)),
                        ByteUtil.concat(
                                PInteger.INSTANCE.toBytes(2),
                                PInteger.INSTANCE.toBytes(4),
                                PInteger.INSTANCE.toBytes(5),
                                PInteger.INSTANCE.toBytes(9))),
                new Finished(
                        ByteUtil.concat(
                                PInteger.INSTANCE.toBytes(3),
                                PInteger.INSTANCE.toBytes(4),
                                PInteger.INSTANCE.toBytes(7),
                                PInteger.INSTANCE.toBytes(11))))
        );
        testCases.addAll(
            foreach(
                new KeyRange[][]{
                {
                    KeyRange.getKeyRange(
                            ByteUtil.concat(PInteger.INSTANCE.toBytes(1),PInteger.INSTANCE.toBytes(2)),
                            true,
                            ByteUtil.concat(PInteger.INSTANCE.toBytes(3),PInteger.INSTANCE.toBytes(4)),
                            true)
                },
                {
                    KeyRange.getKeyRange(ByteUtil.concat(PInteger.INSTANCE.toBytes(5),PInteger.INSTANCE.toBytes(6))),
                    KeyRange.getKeyRange(ByteUtil.concat(PInteger.INSTANCE.toBytes(7),PInteger.INSTANCE.toBytes(8)))
                },
                {
                    PInteger.INSTANCE.getKeyRange(PInteger.INSTANCE.toBytes(9), true, PInteger.INSTANCE.toBytes(10), true)
                }},
                new int[]{4,4,4,4,4},
                new int[]{1,1,0},
                new Include(
                        ByteUtil.concat(
                                PInteger.INSTANCE.toBytes(1),
                                PInteger.INSTANCE.toBytes(3),
                                PInteger.INSTANCE.toBytes(5),
                                PInteger.INSTANCE.toBytes(6),
                                PInteger.INSTANCE.toBytes(9))),
                new SeekNext(
                        ByteUtil.concat(
                                PInteger.INSTANCE.toBytes(2),
                                PInteger.INSTANCE.toBytes(3),
                                PInteger.INSTANCE.toBytes(7),
                                PInteger.INSTANCE.toBytes(8),
                                PInteger.INSTANCE.toBytes(11)),
                        ByteUtil.concat(
                                PInteger.INSTANCE.toBytes(2),
                                PInteger.INSTANCE.toBytes(4),
                                PInteger.INSTANCE.toBytes(5),
                                PInteger.INSTANCE.toBytes(6),
                                PInteger.INSTANCE.toBytes(9))),
                new Finished(
                        ByteUtil.concat(
                                PInteger.INSTANCE.toBytes(3),
                                PInteger.INSTANCE.toBytes(4),
                                PInteger.INSTANCE.toBytes(7),
                                PInteger.INSTANCE.toBytes(8),
                                PInteger.INSTANCE.toBytes(11))))
        );
        return testCases;
    }

    private static Collection<?> foreach(KeyRange[][] ranges, int[] widths, int[] slotSpans, Expectation... expectations) {
        List<List<KeyRange>> cnf = Lists.transform(Lists.newArrayList(ranges), ARRAY_TO_LIST);
        List<Object> ret = Lists.newArrayList();
        ret.add(new Object[] {cnf, widths, slotSpans, Arrays.asList(expectations)} );
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
        
        public Include(byte[] rowkey) {
            this.rowkey = rowkey;
        }
        
        @SuppressWarnings("deprecation")
        @Override public void examine(SkipScanFilter skipper) throws IOException {
            KeyValue kv = KeyValue.createFirstOnRow(rowkey);
            skipper.reset();
            assertFalse(skipper.filterAllRemaining());
            assertFalse(skipper.filterRowKey(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength()));
            assertEquals(kv.toString(), ReturnCode.INCLUDE_AND_NEXT_COL, skipper.filterKeyValue(kv));
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

        public Finished(byte[] rowkey) {
            this.rowkey = rowkey;
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
