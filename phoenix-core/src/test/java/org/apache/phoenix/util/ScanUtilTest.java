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
package org.apache.phoenix.util;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.ATABLE_NAME;
import static org.junit.Assert.assertArrayEquals;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.KeyRange.Bound;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.base.Function;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * Test the SetKey method in ScanUtil.
 */
@RunWith(Enclosed.class)
public class ScanUtilTest {

    @RunWith(Parameterized.class)
    public static class ParameterizedScanUtilTest {
        private final List<List<KeyRange>> slots;
        private final byte[] expectedKey;
        private final RowKeySchema schema;
        private final Bound bound;

        public ParameterizedScanUtilTest(List<List<KeyRange>> slots, int[] widths, byte[] expectedKey, Bound bound)
                throws Exception {
            RowKeySchemaBuilder builder = new RowKeySchemaBuilder(widths.length);
            for (final int width : widths) {
                if (width > 0) {
                    builder.addField(new PDatum() {
                        @Override public boolean isNullable() {
                            return false;
                        }

                        @Override public PDataType getDataType() {
                            return PChar.INSTANCE;
                        }

                        @Override public Integer getMaxLength() {
                            return width;
                        }

                        @Override public Integer getScale() {
                            return null;
                        }

                        @Override public SortOrder getSortOrder() {
                            return SortOrder.getDefault();
                        }
                    }, false, SortOrder.getDefault());
                } else {
                    builder.addField(new PDatum() {
                        @Override public boolean isNullable() {
                            return false;
                        }

                        @Override public PDataType getDataType() {
                            return PVarchar.INSTANCE;
                        }

                        @Override public Integer getMaxLength() {
                            return null;
                        }

                        @Override public Integer getScale() {
                            return null;
                        }

                        @Override public SortOrder getSortOrder() {
                            return SortOrder.getDefault();
                        }
                    }, false, SortOrder.getDefault());
                }
            }
            this.schema = builder.build();
            this.slots = slots;
            this.expectedKey = expectedKey;
            this.bound = bound;
        }

        @Test
        public void test() {
            byte[] key = new byte[1024];
            int[] position = new int[slots.size()];
            int
                    offset =
                    ScanUtil.setKey(schema, slots, ScanUtil.getDefaultSlotSpans(slots.size()), position,
                            bound, key, 0, 0, slots.size());
            byte[] actualKey = new byte[offset];
            System.arraycopy(key, 0, actualKey, 0, offset);
            assertArrayEquals(expectedKey, actualKey);
        }

        @Parameters(name = "{0} {1} {2} {3} {4}")
        public static synchronized Collection<Object> data() {
            List<Object> testCases = Lists.newArrayList();
            // 1, Lower bound, all single keys, all inclusive.
            testCases.addAll(foreach(new KeyRange[][] {
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true), },
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true), },
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("A"),
                                    true), } }, new int[] { 1, 1, 1 }, PChar.INSTANCE.toBytes("a1A"),
                    Bound.LOWER));
            // 2, Lower bound, all range keys, all inclusive.
            testCases.addAll(foreach(new KeyRange[][] {
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("b"), true), },
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), true), },
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"),
                                    true), } }, new int[] { 1, 1, 1 }, PChar.INSTANCE.toBytes("a1A"),
                    Bound.LOWER));
            // 3, Lower bound, mixed single and range keys, all inclusive.
            testCases.addAll(foreach(new KeyRange[][] {
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true), },
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), true), },
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("A"),
                                    true), } }, new int[] { 1, 1, 1 }, PChar.INSTANCE.toBytes("a1A"),
                    Bound.LOWER));
            // 4, Lower bound, all range key, all exclusive on lower bound.
            testCases.addAll(foreach(new KeyRange[][] {
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), false, Bytes.toBytes("b"), true), },
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), false, Bytes.toBytes("2"), true), },
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), false, Bytes.toBytes("B"),
                                    true), } }, new int[] { 1, 1, 1 }, PChar.INSTANCE.toBytes("b2B"),
                    Bound.LOWER));
            // 5, Lower bound, all range key, some exclusive.
            testCases.addAll(foreach(new KeyRange[][] {
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), false, Bytes.toBytes("b"), true), },
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), true), },
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), false, Bytes.toBytes("B"),
                                    true), } }, new int[] { 1, 1, 1 }, PChar.INSTANCE.toBytes("b1B"),
                    Bound.LOWER));
            // 6, Lower bound, mixed single and range key, mixed inclusive and exclusive.
            testCases.addAll(foreach(new KeyRange[][] {
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true), },
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), true), },
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), false, Bytes.toBytes("B"),
                                    true), } }, new int[] { 1, 1, 1 }, PChar.INSTANCE.toBytes("a1B"),
                    Bound.LOWER));
            // 7, Lower bound, unbound key in the middle, fixed length.
            testCases.addAll(foreach(new KeyRange[][] {
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true), },
                            { KeyRange.EVERYTHING_RANGE, },
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), false, Bytes.toBytes("B"),
                                    true), } }, new int[] { 1, 1, 1 }, PChar.INSTANCE.toBytes("a"),
                    Bound.LOWER));
            // 8, Lower bound, unbound key in the middle, variable length.
            testCases.addAll(foreach(new KeyRange[][] {
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true), },
                            { KeyRange.EVERYTHING_RANGE, } }, new int[] { 1, 1 }, PChar.INSTANCE.toBytes("a"),
                    Bound.LOWER));
            // 9, Lower bound, unbound key at end, variable length.
            testCases.addAll(foreach(new KeyRange[][] {
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true), },
                            { KeyRange.EVERYTHING_RANGE, },
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"),
                                    true), } }, new int[] { 1, 1, 1 }, PChar.INSTANCE.toBytes("a"),
                    Bound.LOWER));
            // 10, Upper bound, all single keys, all inclusive, increment at end.
            testCases.addAll(foreach(new KeyRange[][] {
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true), },
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true), },
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("A"),
                                    true), } }, new int[] { 1, 1, 1 }, PChar.INSTANCE.toBytes("a1B"),
                    Bound.UPPER));
            // 11, Upper bound, all range keys, all inclusive, increment at end.
            testCases.addAll(foreach(new KeyRange[][] {
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("b"), true), },
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), true), },
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"),
                                    true), } }, new int[] { 1, 1, 1 }, PChar.INSTANCE.toBytes("b2C"),
                    Bound.UPPER));
            // 12, Upper bound, all range keys, all exclusive, no increment at end.
            testCases.addAll(foreach(new KeyRange[][] {
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("b"), false), },
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), false), },
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"),
                                    false), } }, new int[] { 1, 1, 1 }, PChar.INSTANCE.toBytes("b"),
                    Bound.UPPER));
            // 13, Upper bound, single inclusive, range inclusive, increment at end.
            testCases.addAll(foreach(new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"),
                            true), } }, new int[] { 1, 1 }, PChar.INSTANCE.toBytes("a3"), Bound.UPPER));
            // 14, Upper bound, range exclusive, single inclusive, increment at end.
            testCases.addAll(foreach(new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("b"), false), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"),
                            true), } }, new int[] { 1, 1 }, PChar.INSTANCE.toBytes("b"), Bound.UPPER));
            // 15, Upper bound, range inclusive, single inclusive, increment at end.
            testCases.addAll(foreach(new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("b"), true), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"),
                            true), } }, new int[] { 1, 1 }, PChar.INSTANCE.toBytes("b2"), Bound.UPPER));
            // 16, Upper bound, single inclusive, range exclusive, no increment at end.
            testCases.addAll(foreach(new KeyRange[][] {
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true), },
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"),
                                    false), } }, new int[] { 1, 1 }, PChar.INSTANCE.toBytes("a2"),
                    Bound.UPPER));
            // 17, Upper bound, unbound key, fixed length;
            testCases.addAll(foreach(new KeyRange[][] {
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true), },
                            { KeyRange.EVERYTHING_RANGE, } }, new int[] { 1, 1 }, PChar.INSTANCE.toBytes("b"),
                    Bound.UPPER));
            // 18, Upper bound, unbound key, variable length;
            testCases.addAll(foreach(new KeyRange[][] {
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true), },
                            { KeyRange.EVERYTHING_RANGE, } }, new int[] { 1, 1 }, PChar.INSTANCE.toBytes("b"),
                    Bound.UPPER));
            // 19, Upper bound, keys wrapped around when incrementing.
            testCases.addAll(foreach(new KeyRange[][] {
                            { PChar.INSTANCE.getKeyRange(new byte[] { -1 }, true, new byte[] { -1 }, true) },
                            { PChar.INSTANCE.getKeyRange(new byte[] { -1 }, true, new byte[] { -1 }, true) } },
                    new int[] { 1, 1 }, ByteUtil.EMPTY_BYTE_ARRAY, Bound.UPPER));
            // 20, Variable length
            testCases.addAll(foreach(new KeyRange[][] {
                            { PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true), },
                            { PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"),
                                    true), } }, new int[] { 1, 0 },
                    ByteUtil.nextKey(ByteUtil.concat(PVarchar.INSTANCE.toBytes("aB"), QueryConstants.SEPARATOR_BYTE_ARRAY)),
                    Bound.UPPER));
            return testCases;
        }

        private static Collection<?> foreach(KeyRange[][] ranges, int[] widths, byte[] expectedKey,
                Bound bound) {
            List<List<KeyRange>> slots = Lists.transform(Lists.newArrayList(ranges), ARRAY_TO_LIST);
            List<Object> ret = Lists.newArrayList();
            ret.add(new Object[] { slots, widths, expectedKey, bound });
            return ret;
        }

        private static final Function<KeyRange[], List<KeyRange>> ARRAY_TO_LIST = new Function<KeyRange[], List<KeyRange>>() {
            @Override public List<KeyRange> apply(KeyRange[] input) {
                return Lists.newArrayList(input);
            }
        };
    }

    public static class NonParameterizedScanUtilTest {

        @Test
        public void testSlotsSaltedVarbinaryPk() {
            byte[] key = new byte[1024];

            RowKeySchemaBuilder builder = new RowKeySchemaBuilder(2);

            builder.addField(new PDatum() {
                @Override
                public boolean isNullable() {
                    return false;
                }

                @Override
                public PDataType getDataType() {
                    return PBinary.INSTANCE;
                }

                @Override
                public Integer getMaxLength() {
                    return 1;
                }

                @Override
                public Integer getScale() {
                    return null;
                }

                @Override
                public SortOrder getSortOrder() {
                    return SortOrder.getDefault();
                }
            }, false, SortOrder.getDefault());

            builder.addField(new PDatum() {
                @Override
                public boolean isNullable() {
                    return false;
                }

                @Override
                public PDataType getDataType() {
                    return PVarbinary.INSTANCE;
                }

                @Override
                public Integer getMaxLength() {
                    return 60;
                }

                @Override
                public Integer getScale() {
                    return null;
                }

                @Override
                public SortOrder getSortOrder() {
                    return SortOrder.getDefault();
                }
            }, false, SortOrder.getDefault());

            List<KeyRange> ranges = Lists.newArrayList(KeyRange.getKeyRange(new byte[] { 0, 5 }));
            List<List<KeyRange>> pkKeyRanges = Lists.newArrayList();
            pkKeyRanges.add(ranges);

            // For this case slots the salt bucket and key are one span
            int[] slotSpans = new int[] { 1 };

            int offset = ScanUtil.setKey(builder.build(), pkKeyRanges, slotSpans, new int[] { 0 }, Bound.UPPER, key, 0,
                    0, slotSpans.length);
            byte[] actualKey = new byte[offset];
            System.arraycopy(key, 0, actualKey, 0, offset);
            assertArrayEquals(new byte[] { 0, 5, 0 }, actualKey);
        }

        @Test
        public void testLastPkColumnIsVariableLengthAndDescBug5307() throws Exception{
            RowKeySchemaBuilder rowKeySchemaBuilder = new RowKeySchemaBuilder(2);

            rowKeySchemaBuilder.addField(new PDatum() {
                @Override
                public boolean isNullable() {
                    return false;
                }

                @Override
                public PDataType getDataType() {
                    return PVarchar.INSTANCE;
                }

                @Override
                public Integer getMaxLength() {
                    return null;
                }

                @Override
                public Integer getScale() {
                    return null;
                }

                @Override
                public SortOrder getSortOrder() {
                    return SortOrder.getDefault();
                }
            }, false, SortOrder.getDefault());

            rowKeySchemaBuilder.addField(new PDatum() {
                @Override
                public boolean isNullable() {
                    return false;
                }

                @Override
                public PDataType getDataType() {
                    return PVarchar.INSTANCE;
                }

                @Override
                public Integer getMaxLength() {
                    return null;
                }

                @Override
                public Integer getScale() {
                    return null;
                }

                @Override
                public SortOrder getSortOrder() {
                    return SortOrder.DESC;
                }
            }, false, SortOrder.DESC);

            rowKeySchemaBuilder.rowKeyOrderOptimizable(true);
            RowKeySchema rowKeySchema = rowKeySchemaBuilder.build();
            //it is [[obj1, obj2, obj3], [\xCD\xCD\xCD\xCD, \xCE\xCE\xCE\xCE, \xCE\xCE\xCE\xCE]]
            List<List<KeyRange>> rowKeySlotRangesList = Arrays.asList(
                    Arrays.asList(
                            KeyRange.getKeyRange(PVarchar.INSTANCE.toBytes("obj1", SortOrder.ASC)),
                            KeyRange.getKeyRange(PVarchar.INSTANCE.toBytes("obj2", SortOrder.ASC)),
                            KeyRange.getKeyRange(PVarchar.INSTANCE.toBytes("obj3", SortOrder.ASC))),
                    Arrays.asList(
                            KeyRange.getKeyRange(PVarchar.INSTANCE.toBytes("2222", SortOrder.DESC)),
                            KeyRange.getKeyRange(PVarchar.INSTANCE.toBytes("1111", SortOrder.DESC)),
                            KeyRange.getKeyRange(PVarchar.INSTANCE.toBytes("1111", SortOrder.DESC))));

            int[] rowKeySlotSpans = new int[]{0,0};
            byte[] rowKey = new byte[1024];
            int[] rowKeySlotRangesIndexes = new int[]{0,0};
            int rowKeyLength = ScanUtil.setKey(
                    rowKeySchema,
                    rowKeySlotRangesList,
                    rowKeySlotSpans,
                    rowKeySlotRangesIndexes,
                    Bound.LOWER,
                    rowKey,
                    0,
                    0,
                    2);
            byte[] startKey = Arrays.copyOf(rowKey, rowKeyLength);

            rowKeySlotRangesIndexes = new int[]{2,2};
            rowKey = new byte[1024];
            rowKeyLength = ScanUtil.setKey(
                    rowKeySchema,
                    rowKeySlotRangesList,
                    rowKeySlotSpans,
                    rowKeySlotRangesIndexes,
                    Bound.UPPER,
                    rowKey,
                    0,
                    0,
                    2);
            byte[] endKey = Arrays.copyOf(rowKey, rowKeyLength);

            byte[] expectedStartKey = ByteUtil.concat(
                    PVarchar.INSTANCE.toBytes("obj1", SortOrder.ASC),
                    QueryConstants.SEPARATOR_BYTE_ARRAY,
                    PVarchar.INSTANCE.toBytes("2222", SortOrder.DESC),
                    QueryConstants.DESC_SEPARATOR_BYTE_ARRAY);
            byte[] expectedEndKey =  ByteUtil.concat(
                    PVarchar.INSTANCE.toBytes("obj3", SortOrder.ASC),
                    QueryConstants.SEPARATOR_BYTE_ARRAY,
                    PVarchar.INSTANCE.toBytes("1111", SortOrder.DESC),
                    QueryConstants.DESC_SEPARATOR_BYTE_ARRAY);
            ByteUtil.nextKey(expectedEndKey, expectedEndKey.length);

            assertArrayEquals(expectedStartKey, startKey);
            assertArrayEquals(expectedEndKey, endKey);
        }
    }

    public static class PhoenixTTLScanUtilTest extends BaseConnectionlessQueryTest {

        @Test
        public void testPhoenixTTLUtilMethods() throws SQLException {
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            try (Connection conn = driver.connect(getUrl(), props)) {
                PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
                PTable table = phxConn.getTable(new PTableKey(null, ATABLE_NAME));

                byte[] emptyColumnFamilyName = SchemaUtil.getEmptyColumnFamily(table);
                byte[] emptyColumnName = table.getEncodingScheme()
                        == PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS ?
                        QueryConstants.EMPTY_COLUMN_BYTES :
                        table.getEncodingScheme().encode(QueryConstants.ENCODED_EMPTY_COLUMN_NAME);

                String row = "test.row";
                long timestamp42 = 42L;
                KeyValue.Type type42 = KeyValue.Type.Put;
                String value42 = "test.value.42";
                long seqId42 = 1042L;

                List<Cell> cellList = Lists.newArrayList();
                Cell cell42 = CellUtil.createCell(Bytes.toBytes(row),
                        emptyColumnFamilyName, emptyColumnName,
                        timestamp42, type42.getCode(), Bytes.toBytes(value42), seqId42);
                // Add cell to the cell list
                cellList.add(cell42);

                long timestamp43 = 43L;
                String columnName = "test_column";
                KeyValue.Type type43 = KeyValue.Type.Put;
                String value43 = "test.value.43";
                long seqId43 = 1043L;
                Cell cell43 = CellUtil.createCell(Bytes.toBytes(row),
                        emptyColumnFamilyName, Bytes.toBytes(columnName),
                        timestamp43, type43.getCode(), Bytes.toBytes(value43), seqId43);
                // Add cell to the cell list
                cellList.add(cell43);

                long timestamp44 = 44L;
                Scan testScan = new Scan();
                testScan.setAttribute(BaseScannerRegionObserver.PHOENIX_TTL, Bytes.toBytes(1L));
                // Test isTTLExpired
                Assert.assertTrue(ScanUtil.isTTLExpired(cell42, testScan, timestamp44));
                Assert.assertFalse(ScanUtil.isTTLExpired(cell43, testScan, timestamp44));
                // Test isEmptyColumn
                Assert.assertTrue(ScanUtil.isEmptyColumn(cell42, emptyColumnFamilyName, emptyColumnName));
                Assert.assertFalse(ScanUtil.isEmptyColumn(cell43, emptyColumnFamilyName, emptyColumnName));
                // Test getMaxTimestamp
                Assert.assertEquals(timestamp43, ScanUtil.getMaxTimestamp(cellList));
            }
        }

        @Test
        public void testIsServerSideMaskingPropertySet() throws Exception {
            // Test property is not set
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(QueryServices.PHOENIX_TTL_SERVER_SIDE_MASKING_ENABLED, "false");
            PhoenixTestDriver driver1 = new PhoenixTestDriver(ReadOnlyProps.EMPTY_PROPS.addAll(props));
            try (Connection conn = driver1.connect(getUrl(), props)) {
                PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
                Assert.assertFalse(ScanUtil.isServerSideMaskingEnabled(phxConn));
            }

            // Test property is set
            props.setProperty(QueryServices.PHOENIX_TTL_SERVER_SIDE_MASKING_ENABLED, "true");
            PhoenixTestDriver driver2 = new PhoenixTestDriver(ReadOnlyProps.EMPTY_PROPS.addAll(props));
            try (Connection conn = driver2.connect(getUrl(), props)) {
                PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
                Assert.assertTrue(ScanUtil.isServerSideMaskingEnabled(phxConn));
            }
        }
    }
}
