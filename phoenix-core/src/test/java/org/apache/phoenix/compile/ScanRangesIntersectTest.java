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
package org.apache.phoenix.compile;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PUnsignedTinyint;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ScanRangesIntersectTest {

    @Test
    public void testPointLookupIntersect() throws Exception {
        List<KeyRange> keys = points("a","j","m","z");
        ScanRanges ranges = ScanRanges.createPointLookup(keys);
        assertIntersect(ranges, "b", "l", "j");
        
    }
    
    private static void assertIntersect(ScanRanges ranges, String lowerRange, String upperRange, String... expectedPoints) {
        List<KeyRange> expectedKeys = points(expectedPoints);
        Collections.sort(expectedKeys,KeyRange.COMPARATOR);
        Scan scan = new Scan();
        scan.setFilter(ranges.getSkipScanFilter());
        byte[] startKey = lowerRange == null ? KeyRange.UNBOUND : PVarchar.INSTANCE.toBytes(lowerRange);
        byte[] stopKey = upperRange == null ? KeyRange.UNBOUND : PVarchar.INSTANCE.toBytes(upperRange);
        Scan newScan = ranges.intersectScan(scan, startKey, stopKey, 0, true);
        if (expectedPoints.length == 0) {
            assertNull(newScan);
        } else {
            assertNotNull(newScan);
            SkipScanFilter filter = (SkipScanFilter)newScan.getFilter();
            assertEquals(expectedKeys, filter.getSlots().get(0));
        }
    }

    private static byte[] stringToByteArray(String str){
        return PVarchar.INSTANCE.toBytes(str);
    }

    private static String ByteArrayToString(byte[] bytes){
        String literalString = PVarchar.INSTANCE.toStringLiteral(bytes,null);
        return literalString.substring(1,literalString.length() - 1);
    }

    private static List<KeyRange> points(String... points) {
        List<KeyRange> keys = Lists.newArrayListWithExpectedSize(points.length);
        for (String point : points) {
            keys.add(KeyRange.getKeyRange(stringToByteArray(point)));
        }
        return keys;
    }

    private static PDatum SIMPLE_CHAR = new PDatum() {
        @Override
        public boolean isNullable() {
            return false;
        }

        @Override
        public PDataType getDataType() {
            return PChar.INSTANCE;
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
    };

    private static PDatum SIMPLE_TINYINT = new PDatum() {
        @Override
        public boolean isNullable() {
            return false;
        }

        @Override
        public PDataType getDataType() {
            return PUnsignedTinyint.INSTANCE;
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
    };

    // Does not handle some edge conditions like empty string
    private String handleScanNextKey(String key) {
        char lastChar = key.charAt(key.length() - 1);
        return key.substring(0, key.length() - 1) + String.valueOf((char)(lastChar + 1));
    }

    @Test
    public void getRowKeyRangesTestEverythingRange() {
        int rowKeySchemaFields = 1;
        RowKeySchema schema = buildSimpleRowKeySchema(rowKeySchemaFields);

        List<List<KeyRange>> ranges = new ArrayList<>();
        ranges.add(Lists.newArrayList(KeyRange.EVERYTHING_RANGE));
        ScanRanges scanRanges = ScanRanges.createSingleSpan(schema, ranges);

        List<KeyRange> rowKeyRanges = scanRanges.getRowKeyRanges();
        assertEquals(1, rowKeyRanges.size());
        assertEquals(KeyRange.EVERYTHING_RANGE,rowKeyRanges.get(0));
    }

    @Test
    public void getRowKeyRangesTestEmptyRange() {
        int rowKeySchemaFields = 1;
        RowKeySchema schema = buildSimpleRowKeySchema(rowKeySchemaFields);

        List<List<KeyRange>> ranges = new ArrayList<>();
        ranges.add(Lists.newArrayList(KeyRange.EMPTY_RANGE));
        ScanRanges scanRanges = ScanRanges.createSingleSpan(schema, ranges);

        List<KeyRange> rowKeyRanges = scanRanges.getRowKeyRanges();
        assertEquals(1, rowKeyRanges.size());
        assertEquals(KeyRange.EMPTY_RANGE,rowKeyRanges.get(0));
    }

    @Test
    public void getRowKeyRangesTestPointLookUp() {
        String pointString = "A";
        KeyRange pointKeyRange = KeyRange.getKeyRange(stringToByteArray(pointString));
        ScanRanges singlePointScanRange = ScanRanges.createPointLookup(Lists.newArrayList(pointKeyRange));

        List<KeyRange> rowKeyRanges = singlePointScanRange.getRowKeyRanges();
        assertEquals(1, rowKeyRanges.size());
        assertEquals(false, rowKeyRanges.get(0).isSingleKey());
        assertEquals(singleKeyToScanRange(pointString), rowKeyRanges.get(0).toString());
    }

    @Test
    public void getRowKeyRangesTestPointsLookUp() {
        String pointString1 = "A";
        String pointString2 = "B";
        KeyRange pointKeyRange1 = KeyRange.getKeyRange(stringToByteArray(pointString1));
        KeyRange pointKeyRange2 = KeyRange.getKeyRange(stringToByteArray(pointString2));
        ScanRanges singlePointScanRange = ScanRanges
                .createPointLookup(Lists.newArrayList(pointKeyRange1, pointKeyRange2));

        List<KeyRange> rowKeyRanges = singlePointScanRange.getRowKeyRanges();
        assertEquals(2, rowKeyRanges.size());
        assertEquals(false, rowKeyRanges.get(0).isSingleKey());
        assertEquals(singleKeyToScanRange(pointString1), rowKeyRanges.get(0).toString());
        assertEquals(false, rowKeyRanges.get(1).isSingleKey());
        assertEquals(singleKeyToScanRange(pointString2), rowKeyRanges.get(1).toString());
    }

    @Test
    public void getRowKeyRangesTestOneRangeLookUp() {
        RowKeySchema schema = buildSimpleRowKeySchema(1);

        String lowerString = "A";
        String upperString = "B";

        KeyRange rangeKeyRange = KeyRange.getKeyRange(stringToByteArray(lowerString), stringToByteArray(upperString));
        List<List<KeyRange>> ranges = new ArrayList<>();
        ranges.add(Lists.newArrayList(rangeKeyRange));
        ScanRanges scanRanges = ScanRanges.createSingleSpan(schema, ranges);

        List<KeyRange> rowKeyRanges = scanRanges.getRowKeyRanges();
        assertEquals(1, rowKeyRanges.size());
        assertEquals(false, rowKeyRanges.get(0).isSingleKey());
        assertEquals(lowerString, ByteArrayToString(rowKeyRanges.get(0).getLowerRange()));
        assertEquals(upperString, ByteArrayToString(rowKeyRanges.get(0).getUpperRange()));
    }

    @Test
    public void getRowKeyRangesTestOneExclusiveRangeLookUp() {
        RowKeySchema schema = buildSimpleRowKeySchema(2);

        String lowerString = "A";
        String upperString = "C";

        KeyRange rangeKeyRange = KeyRange.getKeyRange(stringToByteArray(lowerString),false, stringToByteArray(upperString),false);
        List<List<KeyRange>> ranges = new ArrayList<>();
        ranges.add(Lists.newArrayList(rangeKeyRange));
        ScanRanges scanRanges = ScanRanges.createSingleSpan(schema, ranges);

        List<KeyRange> rowKeyRanges = scanRanges.getRowKeyRanges();
        assertEquals(1, rowKeyRanges.size());
        assertEquals(false, rowKeyRanges.get(0).isSingleKey());
        assertEquals("[B - C)", rowKeyRanges.get(0).toString());
    }

    @Test
    public void getRowKeyRangesTestOneExclusiveRangeNotFullyQualifiedLookUp() {
        RowKeySchema schema = buildSimpleRowKeySchema(2);

        String lowerString = "A";
        String upperString = "C";

        KeyRange rangeKeyRange = KeyRange.getKeyRange(stringToByteArray(lowerString),false, stringToByteArray(upperString),false);
        List<List<KeyRange>> ranges = new ArrayList<>();
        ranges.add(Lists.newArrayList(rangeKeyRange));
        ScanRanges scanRanges = ScanRanges.createSingleSpan(schema, ranges);

        List<KeyRange> rowKeyRanges = scanRanges.getRowKeyRanges();
        assertEquals(1, rowKeyRanges.size());
        assertEquals(false, rowKeyRanges.get(0).isSingleKey());
        assertEquals("[B - C)", rowKeyRanges.get(0).toString());
    }

    @Test
    public void getRowKeyRangesTestTwoRangesLookUp() {
        int rowKeySchemaFields = 2;
        RowKeySchema schema = buildSimpleRowKeySchema(rowKeySchemaFields);
        int[] slotSpan = new int[rowKeySchemaFields];

        String lowerString = "A";
        String upperString = "B";

        String lowerString2 = "C";
        String upperString2 = "D";

        KeyRange rangeKeyRange1 = KeyRange.getKeyRange(stringToByteArray(lowerString), true,
                stringToByteArray(upperString), true);
        KeyRange rangeKeyRange2 = KeyRange.getKeyRange(stringToByteArray(lowerString2), true,
                stringToByteArray(upperString2), true);

        List<List<KeyRange>> ranges = new ArrayList<>();
        ranges.add(Lists.newArrayList(rangeKeyRange1));
        ranges.add(Lists.newArrayList(rangeKeyRange2));

        ScanRanges scanRanges = ScanRanges.create(schema, ranges, slotSpan, null, true, -1);

        List<KeyRange> rowKeyRanges = scanRanges.getRowKeyRanges();
        assertEquals(1, rowKeyRanges.size());
        assertEquals(false, rowKeyRanges.get(0).isSingleKey());
        assertEquals(lowerString + lowerString2, ByteArrayToString(rowKeyRanges.get(0).getLowerRange()));
        assertEquals(handleScanNextKey(upperString + upperString2),
                ByteArrayToString(rowKeyRanges.get(0).getUpperRange()));
    }

    @Test
    public void getRowKeyRangesTestNotFullyQualifiedRowKeyLookUp() {
        int rowKeySchemaFields = 2;
        RowKeySchema schema = buildSimpleRowKeySchema(rowKeySchemaFields);

        String keyString1 = "A";
        String keyString2 = "B";

        KeyRange rangeKeyRange1 = KeyRange.getKeyRange(stringToByteArray(keyString1));
        KeyRange rangeKeyRange2 = KeyRange.getKeyRange(stringToByteArray(keyString2));

        List<List<KeyRange>> ranges = new ArrayList<>();
        ranges.add(Lists.newArrayList(rangeKeyRange1, rangeKeyRange2));

        int[] slotSpan = new int[ranges.size()];

        ScanRanges scanRanges = ScanRanges.create(schema, ranges, slotSpan, null, true, -1);

        List<KeyRange> rowKeyRanges = scanRanges.getRowKeyRanges();
        assertEquals(2, rowKeyRanges.size());
        assertEquals(false, rowKeyRanges.get(0).isSingleKey());
        assertEquals(keyString1, ByteArrayToString(rowKeyRanges.get(0).getLowerRange()));
        assertEquals(handleScanNextKey(keyString1), ByteArrayToString(rowKeyRanges.get(0).getUpperRange()));
        assertEquals(false, rowKeyRanges.get(1).isSingleKey());
        assertEquals(keyString2, ByteArrayToString(rowKeyRanges.get(1).getLowerRange()));
        assertEquals(handleScanNextKey(keyString2), ByteArrayToString(rowKeyRanges.get(1).getUpperRange()));
    }

    @Test
    public void getRowKeyRangesTestValuesAndRangesLookUp() {
        int rowKeySchemaFields = 2;
        RowKeySchema schema = buildSimpleRowKeySchema(rowKeySchemaFields);
        int[] slotSpan = new int[rowKeySchemaFields];

        String point1 = "A";
        String point2 = "B";

        String lowerString2 = "C";
        String upperString2 = "D";

        KeyRange pointKeyRange1 = KeyRange.getKeyRange(stringToByteArray(point1));
        KeyRange pointKeyRange2 = KeyRange.getKeyRange(stringToByteArray(point2));

        KeyRange rangeKeyRange = KeyRange.getKeyRange(stringToByteArray(lowerString2), true,
                stringToByteArray(upperString2), true);

        List<List<KeyRange>> ranges = new ArrayList<>();
        ranges.add(Lists.newArrayList(pointKeyRange1, pointKeyRange2));
        ranges.add(Lists.newArrayList(rangeKeyRange));

        ScanRanges scanRanges = ScanRanges.create(schema, ranges, slotSpan, null, true, -1);

        List<KeyRange> rowKeyRanges = scanRanges.getRowKeyRanges();
        assertEquals(2, rowKeyRanges.size());
        assertEquals(false, rowKeyRanges.get(0).isSingleKey());
        assertEquals(point1 + lowerString2, ByteArrayToString(rowKeyRanges.get(0).getLowerRange()));
        assertEquals(handleScanNextKey(point1 + upperString2), ByteArrayToString(rowKeyRanges.get(0).getUpperRange()));

        assertEquals(false, rowKeyRanges.get(1).isSingleKey());
        assertEquals(point2 + lowerString2, ByteArrayToString(rowKeyRanges.get(1).getLowerRange()));
        assertEquals(handleScanNextKey(point2 + upperString2), ByteArrayToString(rowKeyRanges.get(1).getUpperRange()));
    }


    @Test
    public void getRowKeyRangesSorted() {
        int rowKeySchemaFields = 2;
        RowKeySchema schema = buildSimpleRowKeySchema(rowKeySchemaFields);
        int[] slotSpan = new int[rowKeySchemaFields];

        String k1a = "A";
        String k1b = "B";
        String k1c = "C";

        String k2x = "X";
        String k2y = "Y";
        String k2z = "Z";

        KeyRange pointKey1Range1 = KeyRange.getKeyRange(stringToByteArray(k1a));
        KeyRange pointKey1Range2 = KeyRange.getKeyRange(stringToByteArray(k1b));
        KeyRange pointKey1Range3 = KeyRange.getKeyRange(stringToByteArray(k1c));
        KeyRange pointKey2Range1 = KeyRange.getKeyRange(stringToByteArray(k2x));
        KeyRange pointKey2Range2 = KeyRange.getKeyRange(stringToByteArray(k2y));
        KeyRange pointKey2Range3 = KeyRange.getKeyRange(stringToByteArray(k2z));

        List<List<KeyRange>> ranges = new ArrayList<>();
        ranges.add(Lists.newArrayList(pointKey1Range1, pointKey1Range2, pointKey1Range3));
        ranges.add(Lists.newArrayList(pointKey2Range1,pointKey2Range2,pointKey2Range3));

        ScanRanges scanRanges = ScanRanges.create(schema, ranges, slotSpan, null, true, -1);

        List<KeyRange> rowKeyRanges = scanRanges.getRowKeyRanges();
        assertEquals(9, rowKeyRanges.size());
        assertEquals(singleKeyToScanRange("AX"), rowKeyRanges.get(0).toString());
        assertEquals(singleKeyToScanRange("AY"), rowKeyRanges.get(1).toString());
        assertEquals(singleKeyToScanRange("AZ"), rowKeyRanges.get(2).toString());

        assertEquals(singleKeyToScanRange("BX"), rowKeyRanges.get(3).toString());
        assertEquals(singleKeyToScanRange("BY"), rowKeyRanges.get(4).toString());
        assertEquals(singleKeyToScanRange("BZ"), rowKeyRanges.get(5).toString());

        assertEquals(singleKeyToScanRange("CX"), rowKeyRanges.get(6).toString());
        assertEquals(singleKeyToScanRange("CY"), rowKeyRanges.get(7).toString());
        assertEquals(singleKeyToScanRange("CZ"), rowKeyRanges.get(8).toString());

    }

    @Test
    public void getRowKeyRangesAdjacentSubRanges() {
        int rowKeySchemaFields = 2;
        RowKeySchema schema = buildSimpleRowKeySchema(rowKeySchemaFields);

        List<KeyRange> keyRanges = new ArrayList<>();
        keyRanges.add(KeyRange.getKeyRange(stringToByteArray("A"), true,
                stringToByteArray("C"), false));
        keyRanges.add(KeyRange.getKeyRange(stringToByteArray("C"), true,
                stringToByteArray("E"), false));
        keyRanges.add(KeyRange.getKeyRange(stringToByteArray("E"), true,
                stringToByteArray("G"), false));
        keyRanges.add(KeyRange.getKeyRange(stringToByteArray("G"), true,
                stringToByteArray("I"), false));

        List<List<KeyRange>> ranges = new ArrayList<>();
        ranges.add(keyRanges);

        int[] slotSpan = new int[ranges.size()];

        ScanRanges scanRanges = ScanRanges.create(schema, ranges, slotSpan, null, true, -1);

        List<KeyRange> rowKeyRanges = scanRanges.getRowKeyRanges();
        assertEquals(4, rowKeyRanges.size());
        assertEquals("[A - C)", rowKeyRanges.get(0).toString());
        assertEquals("[C - E)", rowKeyRanges.get(1).toString());
        assertEquals("[E - G)", rowKeyRanges.get(2).toString());
        assertEquals("[G - I)", rowKeyRanges.get(3).toString());
    }

    @Test
    public void getRowKeyRangesAdjacentSubRangesUpperInclusive() {
        int rowKeySchemaFields = 1;
        RowKeySchema schema = buildSimpleRowKeySchema(rowKeySchemaFields);

        int[] slotSpan = new int[rowKeySchemaFields];

        List<KeyRange> keyRanges = new ArrayList<>();
        keyRanges.add(KeyRange.getKeyRange(stringToByteArray("A"), false,
                stringToByteArray("C"), true));
        keyRanges.add(KeyRange.getKeyRange(stringToByteArray("C"), false,
                stringToByteArray("E"), true));
        keyRanges.add(KeyRange.getKeyRange(stringToByteArray("E"), false,
                stringToByteArray("G"), true));
        keyRanges.add(KeyRange.getKeyRange(stringToByteArray("G"), false,
                stringToByteArray("I"), true));

        List<List<KeyRange>> ranges = new ArrayList<>();
        ranges.add(keyRanges);

        ScanRanges scanRanges = ScanRanges.create(schema, ranges, slotSpan, null, true, -1);

        List<KeyRange> rowKeyRanges = scanRanges.getRowKeyRanges();
        assertEquals(4, rowKeyRanges.size());
        assertEquals("[B - D)", rowKeyRanges.get(0).toString());
        assertEquals("[D - F)", rowKeyRanges.get(1).toString());
        assertEquals("[F - H)", rowKeyRanges.get(2).toString());
        assertEquals("[H - J)", rowKeyRanges.get(3).toString());
    }

    /*
     * range/single    boundary       bound      increment
     *  range          inclusive      lower         no
     *  range          inclusive      upper         yes, at the end if occurs at any slots.
     *  range          exclusive      lower         yes
     *  range          exclusive      upper         no
     *  single         inclusive      lower         no
     *  single         inclusive      upper         yes, at the end if it is the last slots.
     */

    @Test
    public void getRangeKeyExclusiveLowerIncrementedUpperNotIncremented() {
        int rowKeySchemaFields = 1;
        RowKeySchema schema = buildSimpleRowKeySchema(rowKeySchemaFields);

        int[] slotSpan = new int[rowKeySchemaFields];

        String lowerKeyString = "E";
        String upperKeyString = "O";
        KeyRange rangeKeyRange = KeyRange.getKeyRange(stringToByteArray(lowerKeyString),false, stringToByteArray(upperKeyString), false);
        List<List<KeyRange>> ranges = new ArrayList<>();
        ranges.add(Lists.newArrayList(rangeKeyRange));

        ScanRanges scanRanges = ScanRanges.create(schema, ranges, slotSpan, null, true, -1);

        List<KeyRange> rowKeyRanges = scanRanges.getRowKeyRanges();
        assertEquals(1, rowKeyRanges.size());
        assertTrue(rowKeyRanges.get(0).isLowerInclusive());
        assertFalse(rowKeyRanges.get(0).isUpperInclusive());
        assertArrayEquals(stringToByteArray(handleScanNextKey(lowerKeyString)),rowKeyRanges.get(0).getLowerRange());
        assertArrayEquals(stringToByteArray(upperKeyString),rowKeyRanges.get(0).getUpperRange());
    }

    @Test
    public void getAdjacentKeysLowerNotIncrementedUpperIncrementedLastSlots() {
        int rowKeySchemaFields = 2;
        RowKeySchema schema = buildSimpleRowKeySchema(rowKeySchemaFields);

        int[] slotSpan = new int[rowKeySchemaFields];

        KeyRange keyRange1_1 = KeyRange.getKeyRange(stringToByteArray("A"));
        KeyRange keyRange2_1 = KeyRange.getKeyRange(stringToByteArray("B"));

        KeyRange keyRange1_2 = KeyRange.getKeyRange(stringToByteArray("C"));
        KeyRange keyRange2_2 = KeyRange.getKeyRange(stringToByteArray("D"));
        List<List<KeyRange>> ranges = new ArrayList<>();
        ranges.add(Lists.newArrayList(keyRange1_1,keyRange2_1));
        ranges.add(Lists.newArrayList(keyRange1_2,keyRange2_2));

        ScanRanges scanRanges = ScanRanges.create(schema, ranges, slotSpan, null, true, -1);

        List<KeyRange> rowKeyRanges = scanRanges.getRowKeyRanges();
        assertEquals(4, rowKeyRanges.size());
        assertEquals(singleKeyToScanRange("AC"),rowKeyRanges.get(0).toString());
        assertEquals(singleKeyToScanRange("AD"),rowKeyRanges.get(1).toString());
        assertEquals(singleKeyToScanRange("BC"),rowKeyRanges.get(2).toString());
        assertEquals(singleKeyToScanRange("BD"),rowKeyRanges.get(3).toString());
    }

    @Test
    public void getRowKeyRangesMultipleFieldsSingleSlot() {
        int rowKeySchemaFields = 3;
        RowKeySchema schema = buildSimpleRowKeySchema(rowKeySchemaFields);

        int[] slotSpan = new int[1];
        slotSpan[0] = 2;

        KeyRange keyRange1 = KeyRange.getKeyRange(stringToByteArray("ABC"));
        KeyRange keyRange2 = KeyRange.getKeyRange(stringToByteArray("DEF"));
        List<List<KeyRange>> ranges = new ArrayList<>();
        ranges.add(Lists.newArrayList(keyRange1,keyRange2));

        ScanRanges scanRanges = ScanRanges.create(schema, ranges, slotSpan, null, true, -1);

        List<KeyRange> rowKeyRanges = scanRanges.getRowKeyRanges();
        assertEquals(2, rowKeyRanges.size());
        assertEquals(singleKeyToScanRange("ABC"),rowKeyRanges.get(0).toString());
        assertEquals(singleKeyToScanRange("DEF"),rowKeyRanges.get(1).toString());
    }

    @Test
    public void getRowKeyRangesMultipleFieldsFrontLoadedSlot() {
        int rowKeySchemaFields = 3;
        RowKeySchema schema = buildSimpleRowKeySchema(rowKeySchemaFields);

        int[] slotSpan = new int[2];
        slotSpan[0] = 1;

        KeyRange keyRange1 = KeyRange.getKeyRange(stringToByteArray("AB"));
        List<List<KeyRange>> ranges = new ArrayList<>();
        ranges.add(Lists.newArrayList(keyRange1));

        KeyRange keyRange3 = KeyRange.getKeyRange(stringToByteArray("C"));
        ranges.add(Lists.newArrayList(keyRange3));

        ScanRanges scanRanges = ScanRanges.create(schema, ranges, slotSpan, null, true, -1);

        List<KeyRange> rowKeyRanges = scanRanges.getRowKeyRanges();
        assertEquals(1, rowKeyRanges.size());
        assertEquals("[ABC - ABD)",rowKeyRanges.get(0).toString());
    }

    @Test
    public void getRowKeyRangesMultipleFieldsBackLoadedSlot() {
        int rowKeySchemaFields = 3;
        RowKeySchema schema = buildSimpleRowKeySchema(rowKeySchemaFields);

        int[] slotSpan = new int[2];
        slotSpan[1] = 1;

        KeyRange keyRange1 = KeyRange.getKeyRange(stringToByteArray("A"));
        List<List<KeyRange>> ranges = new ArrayList<>();
        ranges.add(Lists.newArrayList(keyRange1));

        KeyRange keyRange3 = KeyRange.getKeyRange(stringToByteArray("BC"));
        ranges.add(Lists.newArrayList(keyRange3));

        ScanRanges scanRanges = ScanRanges.create(schema, ranges, slotSpan, null, true, -1);

        List<KeyRange> rowKeyRanges = scanRanges.getRowKeyRanges();
        assertEquals(1, rowKeyRanges.size());
        assertEquals("[ABC - ABD)",rowKeyRanges.get(0).toString());
    }

    @Test
    public void getRowKeyRangesUpperBoundOverflows() {
        int rowKeySchemaFields = 2;
        RowKeySchema.RowKeySchemaBuilder builder = new RowKeySchema.RowKeySchemaBuilder(rowKeySchemaFields);
        for(int i = 0; i < rowKeySchemaFields; i++) {
            builder.addField(SIMPLE_TINYINT, SIMPLE_TINYINT.isNullable(), SIMPLE_TINYINT.getSortOrder());
        }
        RowKeySchema schema = builder.build();

        int[] slotSpan = new int[2];

        KeyRange keyRange1 = KeyRange.getKeyRange(new byte[]{-1});
        List<List<KeyRange>> ranges = new ArrayList<>();
        ranges.add(Lists.newArrayList(keyRange1));

        KeyRange keyRange3 = KeyRange.getKeyRange(new byte[]{-1});
        ranges.add(Lists.newArrayList(keyRange3));

        ScanRanges scanRanges = ScanRanges.create(schema, ranges, slotSpan, null, true, -1);

        List<KeyRange> rowKeyRanges = scanRanges.getRowKeyRanges();
        assertEquals(1, rowKeyRanges.size());
        assertEquals("[\\xFF\\xFF - *)",rowKeyRanges.get(0).toString());
    }

    private RowKeySchema buildSimpleRowKeySchema(int fields){
        RowKeySchema.RowKeySchemaBuilder builder = new RowKeySchema.RowKeySchemaBuilder(fields);
        for(int i = 0; i < fields; i++) {
            builder.addField(SIMPLE_CHAR, SIMPLE_CHAR.isNullable(), SIMPLE_CHAR.getSortOrder());
        }
        return builder.build();
    }

    private String singleKeyToScanRange(String key){
        //Appending 0 byte is next key for variable length asc fields
        //This includes point gets for more than 1 key as ScanRanges treats the combined pk
        // as a varbinary
        return String.format("[%s - %s\\x00)",key,key);
    }
}
